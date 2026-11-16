package ws

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is a WebSocket peer used by tests and by callers that dial this server.
// It speaks RFC 6455 with the standard library only.
type Client struct {
	w *wire
}

// ClientConfig is the dial policy for the standard-library client.
// Compression.Enabled offers permessage-deflate. Header is copied onto
// the handshake. If Header already contains Sec-WebSocket-Extensions,
// that value is the offer and Compression still decides what this
// client is willing to do with the response.
type ClientConfig struct {
	Header      http.Header
	Compression Compression
	// ReadLimit is the maximum inbound message size. Zero selects 8 MiB plus 4 KiB.
	ReadLimit int64
}

// Dial opens a WebSocket client connection.
// rawURL uses the ws or wss scheme. header is copied onto the handshake.
// On a non-101 response, Client is nil and the HTTP response is returned.
func Dial(ctx context.Context, rawURL string, header http.Header) (*Client, *http.Response, error) {
	return DialClient(ctx, rawURL, ClientConfig{Header: header})
}

// DialClient opens a WebSocket client connection with cfg.
func DialClient(ctx context.Context, rawURL string, cfg ClientConfig) (*Client, *http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	network := "tcp"
	switch u.Scheme {
	case "ws":
	case "wss":
		return nil, nil, fmt.Errorf("websocket: wss is not dialed by this client")
	default:
		return nil, nil, fmt.Errorf("websocket: unsupported scheme %q", u.Scheme)
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":80"
	}
	var d net.Dialer
	nc, err := d.DialContext(ctx, network, host)
	if err != nil {
		return nil, nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = nc.SetDeadline(deadline)
	}
	defer nc.SetDeadline(time.Time{})
	keyRaw := make([]byte, 16)
	if _, err := rand.Read(keyRaw); err != nil {
		_ = nc.Close()
		return nil, nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyRaw)
	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	header := cfg.Header.Clone()
	if header == nil {
		header = make(http.Header)
	}
	if cfg.Compression.Enabled && header.Get("Sec-WebSocket-Extensions") == "" {
		header.Set("Sec-WebSocket-Extensions", clientOffer(cfg.Compression))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\n", path)
	fmt.Fprintf(&b, "Host: %s\r\n", u.Host)
	io.WriteString(&b, "Upgrade: websocket\r\n")
	io.WriteString(&b, "Connection: Upgrade\r\n")
	fmt.Fprintf(&b, "Sec-WebSocket-Key: %s\r\n", key)
	io.WriteString(&b, "Sec-WebSocket-Version: 13\r\n")
	for name, values := range header {
		for _, v := range values {
			fmt.Fprintf(&b, "%s: %s\r\n", name, v)
		}
	}
	io.WriteString(&b, "\r\n")
	if _, err := io.WriteString(nc, b.String()); err != nil {
		_ = nc.Close()
		return nil, nil, err
	}
	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		_ = nc.Close()
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		resp.Body = io.NopCloser(strings.NewReader(string(body)))
		_ = nc.Close()
		return nil, resp, fmt.Errorf("websocket: handshake status %s", resp.Status)
	}
	if !strings.EqualFold(acceptValue(key), strings.TrimSpace(resp.Header.Get("Sec-WebSocket-Accept"))) {
		resp.Body.Close()
		_ = nc.Close()
		return nil, resp, fmt.Errorf("websocket: bad accept key")
	}
	limit := cfg.ReadLimit
	if limit == 0 {
		limit = 8<<20 + 4096
	}
	comp := cfg.Compression
	offered := comp.Enabled
	if !offered && strings.Contains(strings.ToLower(header.Get("Sec-WebSocket-Extensions")), "permessage-deflate") {
		// The caller wrote the offer. Decode the response with the
		// caller's compression policy, which defaults to no context takeover.
		offered = true
		comp.Enabled = true
	}
	var pm *pmce
	extHeader := resp.Header.Get("Sec-WebSocket-Extensions")
	if offered {
		if comp.MaxDecompressed == 0 {
			comp.MaxDecompressed = limit
		}
		if comp.MaxCompressed == 0 {
			comp.MaxCompressed = limit
		}
		var err error
		pm, err = clientPMCE(extHeader, comp)
		if err != nil {
			resp.Body.Close()
			_ = nc.Close()
			return nil, resp, err
		}
	} else if strings.TrimSpace(extHeader) != "" {
		resp.Body.Close()
		_ = nc.Close()
		return nil, resp, fmt.Errorf("%w: extension was not offered", errProtocol)
	}
	resp.Body.Close()
	return &Client{w: &wire{
		nc:        nc,
		br:        br,
		server:    false,
		readLimit: limit,
		pmce:      pm,
	}}, resp, nil
}

// Negotiated reports the permessage-deflate agreement for this client.
func (c *Client) Negotiated() Negotiated {
	if c == nil || c.w == nil || c.w.pmce == nil {
		return Negotiated{}
	}
	return c.w.pmce.neg
}

// Read returns the next data message.
func (c *Client) Read(ctx context.Context) (MessageType, []byte, error) {
	if c == nil || c.w == nil {
		return 0, nil, errClosed
	}
	binary, data, err := c.w.Read(ctx)
	if err != nil {
		return 0, nil, err
	}
	if binary {
		return MessageBinary, data, nil
	}
	return MessageText, data, nil
}

// Write sends one data message. Text frames are the usual choice.
func (c *Client) Write(ctx context.Context, typ MessageType, p []byte) error {
	if c == nil || c.w == nil {
		return errClosed
	}
	return c.w.writeMessage(ctx, typ == MessageBinary, p)
}

// Close writes a close frame. It does not wait for the peer to echo it.
func (c *Client) Close(code int, reason string) error {
	if c == nil || c.w == nil {
		return errClosed
	}
	return c.w.Close(code, reason)
}

// CloseNow aborts the TCP connection.
func (c *Client) CloseNow() error {
	if c == nil || c.w == nil {
		return nil
	}
	return c.w.CloseNow()
}

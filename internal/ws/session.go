// Package ws is the WebSocket transport.
// It implements the RFC 6455 handshake and frames with the standard library
// only. The root package does not re-export these types.
package ws

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"strings"
)

const acceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// DefaultReadLimit is the maximum inbound WebSocket message size in bytes.
const DefaultReadLimit int64 = 32 * 1024

// StatusNormalClosure is the RFC 6455 close code for a normal closure.
const StatusNormalClosure = 1000

// MessageType is the data-frame kind returned to a client.
type MessageType int

const (
	// MessageText is a text data message.
	MessageText MessageType = 1
	// MessageBinary is a binary data message.
	MessageBinary MessageType = 2
)

// CloseError is a peer close frame. Code is an RFC 6455 status code.
type CloseError struct {
	Code   int
	Reason string
}

func (e *CloseError) Error() string {
	if e == nil {
		return "websocket closed"
	}
	return fmt.Sprintf("websocket closed: status = %d reason = %q", e.Code, e.Reason)
}

// Config is the upgrade configuration owned by this package.
type Config struct {
	OriginPatterns     []string
	InsecureSkipOrigin bool
	// ReadLimit is the maximum inbound message size. Zero selects DefaultReadLimit.
	ReadLimit int64
}

// Session is one accepted WebSocket.
//
// Read must not run concurrently with itself. Write, Ping, and CloseNow may
// run alongside Read. A context that ends during Read, Write, or Ping fails
// that call. Canceling the context after the call has returned does not
// close the socket. CloseNow unblocks a Read that is waiting for a frame.
type Session struct {
	w *wire
}

// Upgrade hijacks r into a WebSocket session.
// On failure the HTTP response has already been written.
func Upgrade(w http.ResponseWriter, r *http.Request, cfg Config) (*Session, error) {
	limit := cfg.ReadLimit
	if limit == 0 {
		limit = DefaultReadLimit
	}
	if !originAllowed(r, cfg.OriginPatterns, cfg.InsecureSkipOrigin) {
		http.Error(w, "forbidden origin", http.StatusForbidden)
		return nil, errors.New("websocket: forbidden origin")
	}
	if err := checkHandshake(r); err != nil {
		http.Error(w, "bad websocket handshake", http.StatusBadRequest)
		return nil, err
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return nil, errors.New("websocket: response cannot be hijacked")
	}
	nc, rw, err := hj.Hijack()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return nil, err
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if err := writeAccept(rw.Writer, key); err != nil {
		_ = nc.Close()
		return nil, err
	}
	br := io.Reader(rw.Reader)
	if rw.Reader == nil {
		br = bufio.NewReader(nc)
	}
	return &Session{w: &wire{
		nc:        nc,
		br:        br,
		server:    true,
		readLimit: limit,
	}}, nil
}

// Read returns the next data message.
// binary is true for a binary message. The returned buffer is owned by the
// caller only until the next Read on this session; copy it to retain it.
func (s *Session) Read(ctx context.Context) (binary bool, data []byte, err error) {
	if s == nil || s.w == nil {
		return false, nil, errClosed
	}
	return s.w.Read(ctx)
}

// Write sends one text message. The payload is not retained after Write
// returns, and Write does not mutate p. Server frames are not masked.
func (s *Session) Write(ctx context.Context, p []byte) error {
	if s == nil || s.w == nil {
		return errClosed
	}
	return s.w.Write(ctx, p)
}

// Ping writes a ping and waits until the peer's pong is observed by Read.
// Ping must run concurrently with an active Read.
func (s *Session) Ping(ctx context.Context) error {
	if s == nil || s.w == nil {
		return errClosed
	}
	return s.w.Ping(ctx)
}

// CloseNow aborts the socket without waiting for a close handshake.
func (s *Session) CloseNow() error {
	if s == nil || s.w == nil {
		return nil
	}
	return s.w.CloseNow()
}

func originAllowed(r *http.Request, patterns []string, skip bool) bool {
	if skip {
		return true
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	host := originHost(origin)
	if host == "" {
		return false
	}
	req := strings.ToLower(r.Host)
	if host == req || sameHostname(host, req) {
		return true
	}
	for _, p := range patterns {
		ok, err := path.Match(strings.ToLower(p), host)
		if err == nil && ok {
			return true
		}
	}
	return false
}

func originHost(origin string) string {
	if !strings.Contains(origin, "://") {
		return strings.ToLower(origin)
	}
	// Avoid pulling the URL parser's path handling into the comparison.
	rest := origin[strings.Index(origin, "://")+3:]
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		rest = rest[:i]
	}
	return strings.ToLower(rest)
}

func sameHostname(originHost, reqHost string) bool {
	oh, _, err1 := net.SplitHostPort(originHost)
	if err1 != nil {
		oh = originHost
	}
	rh, _, err2 := net.SplitHostPort(reqHost)
	if err2 != nil {
		rh = reqHost
	}
	return oh != "" && oh == rh
}

func checkHandshake(r *http.Request) error {
	if !headerHasToken(r.Header.Get("Connection"), "upgrade") {
		return errors.New("websocket: missing connection upgrade")
	}
	if !headerHasToken(r.Header.Get("Upgrade"), "websocket") {
		return errors.New("websocket: missing upgrade")
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		return errors.New("websocket: unsupported version")
	}
	if _, err := decodeKey(r.Header.Get("Sec-WebSocket-Key")); err != nil {
		return err
	}
	return nil
}

func headerHasToken(v, token string) bool {
	for _, part := range strings.Split(v, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func decodeKey(key string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(key))
	if err != nil || len(b) != 16 {
		return nil, errors.New("websocket: bad sec-websocket-key")
	}
	return b, nil
}

func acceptValue(key string) string {
	sum := sha1.Sum([]byte(strings.TrimSpace(key) + acceptGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func writeAccept(w io.Writer, key string) error {
	if bw, ok := w.(*bufio.Writer); ok {
		if _, err := io.WriteString(bw, acceptResponse(key)); err != nil {
			return err
		}
		return bw.Flush()
	}
	_, err := io.WriteString(w, acceptResponse(key))
	return err
}

func acceptResponse(key string) string {
	return "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptValue(key) + "\r\n\r\n"
}

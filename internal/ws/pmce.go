package ws

import (
	"bytes"
	"compress/flate"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	extPermessageDeflate = "permessage-deflate"
	paramServerNoContext = "server_no_context_takeover"
	paramClientNoContext = "client_no_context_takeover"
	paramServerWindow    = "server_max_window_bits"
	paramClientWindow    = "client_max_window_bits"

	// windowSize is the only LZ77 window this process can actually use.
	// compress/flate has a fixed 32,768-byte history.
	windowSize = 1 << 15
	windowBits = 15

	// defaultMinCompress is the smallest payload that is worth compressing.
	// Shorter messages skip the compressor and are sent with RSV1 clear.
	defaultMinCompress = 128
)

// Negotiated is the permessage-deflate agreement for one connection.
// Extension is the Sec-WebSocket-Extensions value sent to the peer.
// A zero ServerContextTakeover means each message this process sends
// starts from an empty window. A zero ClientContextTakeover means each
// message the peer sends is inflated from an empty window.
type Negotiated struct {
	Enabled               bool
	Extension             string
	ServerContextTakeover bool
	ClientContextTakeover bool
}

// Compression is the permessage-deflate policy for one endpoint.
// The zero value does not negotiate the extension.
// Level 0 selects flate.DefaultCompression. MinSize 0 selects defaultMinCompress.
// MaxDecompressed and MaxCompressed are byte caps. Zero selects the caller's default.
type Compression struct {
	Enabled               bool
	Level                 int
	ServerContextTakeover bool
	ClientContextTakeover bool
	MinSize               int
	MaxDecompressed       int64
	MaxCompressed         int64
}

// pmce is the RFC 7692 state for one direction pair on one connection.
// The compressor is used only while the wire write lock is held.
// The decompressor is used only while the wire read lock is held.
type pmce struct {
	on  bool
	neg Negotiated

	level   int
	minSize int
	// outContext reuses the compressor window across messages we send.
	outContext bool
	zw         *flate.Writer
	buf        bytes.Buffer

	// inContext reuses the inflater window across messages we receive.
	inContext bool
	zr        io.ReadCloser
	reset     flate.Resetter
	window    []byte
	full      []byte
	maxOut    int64
	maxIn     int64
}

func negotiate(headerValues []string, cfg Compression) (string, *pmce, error) {
	if !cfg.Enabled {
		return "", nil, nil
	}
	joined := strings.Join(headerValues, ",")
	if strings.TrimSpace(joined) == "" {
		return "", nil, nil
	}
	offers, ok := parseExtensionList(joined)
	if !ok {
		// A malformed offer is declined. The handshake itself still completes.
		return "", nil, nil
	}
	for _, offer := range offers {
		agreed, ok := acceptOffer(offer, cfg)
		if !ok {
			continue
		}
		p, err := newPMCE(agreed, cfg, roleServer)
		if err != nil {
			return "", nil, err
		}
		return agreed.extension, p, nil
	}
	return "", nil, nil
}

type agreement struct {
	extension     string
	serverContext bool
	clientContext bool
}

func acceptOffer(offer extOffer, cfg Compression) (agreement, bool) {
	if !strings.EqualFold(offer.name, extPermessageDeflate) {
		return agreement{}, false
	}
	seen := make(map[string]struct{}, len(offer.params))
	clientAskedServerNo := false
	serverBits := -1
	for _, p := range offer.params {
		key := strings.ToLower(p.key)
		if _, dup := seen[key]; dup {
			return agreement{}, false
		}
		seen[key] = struct{}{}
		switch key {
		case paramServerNoContext:
			if p.hasVal {
				return agreement{}, false
			}
			clientAskedServerNo = true
		case paramClientNoContext:
			if p.hasVal {
				return agreement{}, false
			}
		case paramServerWindow:
			// The parameter is required to carry a value. A window below 15
			// cannot be honored: flate always has a 32,768-byte history.
			bits, ok := windowBitsValue(p.val)
			if !p.hasVal || !ok || bits < windowBits {
				return agreement{}, false
			}
			serverBits = bits
		case paramClientWindow:
			// Absent value means the client can accept a limit. A value is a hint.
			// We inflate with a 32,768-byte window, so any hint from 8 to 15 is safe,
			// and we do not echo a tighter limit we would not enforce.
			if p.hasVal {
				if _, ok := windowBitsValue(p.val); !ok {
					return agreement{}, false
				}
			}
		default:
			return agreement{}, false
		}
	}

	// server_no_context_takeover in an offer forbids the server from
	// reusing its window. client_no_context_takeover in an offer is only
	// a hint: if we omit it from the response the peer may still take
	// over, so our inflater keeps history whenever the application asked
	// for that. We emit the parameter whenever we are not keeping it.
	serverContext := cfg.ServerContextTakeover && !clientAskedServerNo
	clientContext := cfg.ClientContextTakeover
	var b strings.Builder
	b.WriteString(extPermessageDeflate)
	if !serverContext {
		b.WriteString("; ")
		b.WriteString(paramServerNoContext)
	}
	if !clientContext {
		b.WriteString("; ")
		b.WriteString(paramClientNoContext)
	}
	if serverBits == windowBits {
		b.WriteString("; ")
		b.WriteString(paramServerWindow)
		b.WriteString("=15")
	}
	ext := b.String()
	if strings.ContainsAny(ext, "\r\n") {
		return agreement{}, false
	}
	return agreement{
		extension:     ext,
		serverContext: serverContext,
		clientContext: clientContext,
	}, true
}

// clientPMCE reads the server's extension response.
// An empty header means the server declined compression.
func clientPMCE(header string, cfg Compression) (*pmce, error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return nil, nil
	}
	offers, ok := parseExtensionList(header)
	if !ok || len(offers) != 1 || !strings.EqualFold(offers[0].name, extPermessageDeflate) {
		return nil, fmt.Errorf("%w: unsupported extension response", errProtocol)
	}
	seen := make(map[string]struct{}, len(offers[0].params))
	serverNo := false
	clientNo := false
	for _, p := range offers[0].params {
		key := strings.ToLower(p.key)
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("%w: duplicate extension parameter", errProtocol)
		}
		seen[key] = struct{}{}
		switch key {
		case paramServerNoContext:
			if p.hasVal {
				return nil, fmt.Errorf("%w: bad %s", errProtocol, key)
			}
			serverNo = true
		case paramClientNoContext:
			if p.hasVal {
				return nil, fmt.Errorf("%w: bad %s", errProtocol, key)
			}
			clientNo = true
		case paramServerWindow:
			bits, ok := windowBitsValue(p.val)
			if !p.hasVal || !ok {
				return nil, fmt.Errorf("%w: bad %s", errProtocol, key)
			}
			_ = bits
		case paramClientWindow:
			// A response value limits our compressor. flate cannot use a
			// window smaller than 32,768 bytes, so only 15 is acceptable.
			bits, ok := windowBitsValue(p.val)
			if !p.hasVal || !ok || bits != windowBits {
				return nil, fmt.Errorf("%w: unsupported %s", errProtocol, key)
			}
		default:
			return nil, fmt.Errorf("%w: unknown extension parameter", errProtocol)
		}
	}
	if !serverNo && !cfg.ServerContextTakeover {
		return nil, fmt.Errorf("%w: server context takeover was not offered", errProtocol)
	}
	agreed := agreement{
		extension:     header,
		serverContext: !serverNo,
		clientContext: cfg.ClientContextTakeover && !clientNo,
	}
	return newPMCE(agreed, cfg, roleClient)
}

type endpointRole int

const (
	roleServer endpointRole = iota
	roleClient
)

func newPMCE(agreed agreement, cfg Compression, who endpointRole) (*pmce, error) {
	level := cfg.Level
	if level == 0 || level == flate.NoCompression {
		level = flate.DefaultCompression
	}
	minSize := cfg.MinSize
	if minSize == 0 {
		minSize = defaultMinCompress
	}
	outContext := agreed.serverContext
	inContext := agreed.clientContext
	if who == roleClient {
		// The client compressor follows client context takeover.
		// The client inflater follows server context takeover.
		outContext = agreed.clientContext
		inContext = agreed.serverContext
	}
	p := &pmce{
		on:         true,
		level:      level,
		minSize:    minSize,
		outContext: outContext,
		inContext:  inContext,
		maxOut:     cfg.MaxDecompressed,
		maxIn:      cfg.MaxCompressed,
		neg: Negotiated{
			Enabled:               true,
			Extension:             agreed.extension,
			ServerContextTakeover: agreed.serverContext,
			ClientContextTakeover: agreed.clientContext,
		},
	}
	zw, err := flate.NewWriter(&p.buf, level)
	if err != nil {
		return nil, err
	}
	p.zw = zw
	rc := flate.NewReader(bytes.NewReader(nil))
	reset, ok := rc.(flate.Resetter)
	if !ok {
		_ = rc.Close()
		return nil, errors.New("websocket: flate reader cannot be reset")
	}
	p.zr = rc
	p.reset = reset
	return p, nil
}

func (p *pmce) compress(src []byte) ([]byte, bool, error) {
	if p == nil || !p.on || len(src) < p.minSize {
		return src, false, nil
	}
	if p.zw == nil {
		return nil, false, errClosed
	}
	p.buf.Reset()
	if !p.outContext {
		p.zw.Reset(&p.buf)
	}
	if _, err := p.zw.Write(src); err != nil {
		return p.compressFailed(src, err)
	}
	if err := p.zw.Flush(); err != nil {
		return p.compressFailed(src, err)
	}
	raw := p.buf.Bytes()
	if len(raw) < 4 || raw[len(raw)-4] != 0x00 || raw[len(raw)-3] != 0x00 || raw[len(raw)-2] != 0xff || raw[len(raw)-1] != 0xff {
		if p.outContext {
			return nil, false, errProtocol
		}
		return src, false, nil
	}
	raw = raw[:len(raw)-4]
	// A no-context message that did not shrink is sent unchanged.
	// The compressor was reset, so dropping the result does not desync
	// the peer. Context takeover cannot be rolled back: the window
	// already contains src, and the compressed bytes have to be sent.
	if !p.outContext && len(raw) >= len(src) {
		return src, false, nil
	}
	return raw, true, nil
}

func (p *pmce) compressFailed(src []byte, err error) ([]byte, bool, error) {
	if p.outContext {
		return nil, false, err
	}
	return src, false, nil
}

func (p *pmce) decompress(ctx context.Context, src []byte) ([]byte, error) {
	if p == nil || !p.on || p.reset == nil {
		return nil, errClosed
	}
	if p.maxIn > 0 && int64(len(src)) > p.maxIn {
		return nil, errTooBig
	}
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	p.full = append(p.full[:0], src...)
	p.full = append(p.full, 0x00, 0x00, 0xff, 0xff)
	var dict []byte
	if p.inContext {
		dict = p.window
	}
	if err := p.reset.Reset(bytes.NewReader(p.full), dict); err != nil {
		return nil, fmt.Errorf("%w: %v", errProtocol, err)
	}
	limit := p.maxOut
	if limit <= 0 {
		limit = DefaultReadLimit
	}
	capHint := len(src) * 2
	if capHint < 64 {
		capHint = 64
	}
	if int64(capHint) > limit {
		capHint = int(limit)
	}
	dst := make([]byte, 0, capHint)
	var scratch [4096]byte
	for {
		if err := contextErr(ctx); err != nil {
			return nil, err
		}
		n, err := p.zr.Read(scratch[:])
		if n > 0 {
			if int64(len(dst))+int64(n) > limit {
				return nil, errTooBig
			}
			dst = append(dst, scratch[:n]...)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if p.inContext {
				p.note(dst)
			}
			return dst, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errProtocol, err)
		}
	}
}

func (p *pmce) note(msg []byte) {
	if len(msg) == 0 {
		return
	}
	if len(msg) >= windowSize {
		p.window = append(p.window[:0], msg[len(msg)-windowSize:]...)
		return
	}
	p.window = append(p.window, msg...)
	if len(p.window) > windowSize {
		p.window = append([]byte(nil), p.window[len(p.window)-windowSize:]...)
	}
}

func (p *pmce) releaseIn() {
	if p == nil {
		return
	}
	if p.zr != nil {
		_ = p.zr.Close()
		p.zr = nil
		p.reset = nil
	}
	p.window = nil
	p.full = nil
}

func (p *pmce) releaseOut() {
	if p == nil {
		return
	}
	p.zw = nil
	p.buf.Reset()
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func windowBitsValue(v string) (int, bool) {
	if v == "" || (len(v) > 1 && v[0] == '0') {
		return 0, false
	}
	n := 0
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	if n < 8 || n > 15 {
		return 0, false
	}
	return n, true
}

func clientOffer(cfg Compression) string {
	var b strings.Builder
	b.WriteString(extPermessageDeflate)
	if !cfg.ServerContextTakeover {
		b.WriteString("; ")
		b.WriteString(paramServerNoContext)
	}
	if !cfg.ClientContextTakeover {
		b.WriteString("; ")
		b.WriteString(paramClientNoContext)
	}
	return b.String()
}

type extOffer struct {
	name   string
	params []extParam
}

type extParam struct {
	key    string
	val    string
	hasVal bool
}

func parseExtensionList(v string) ([]extOffer, bool) {
	var offers []extOffer
	i := 0
	for {
		i = skipWS(v, i)
		if i >= len(v) {
			return offers, true
		}
		name, next, ok := readToken(v, i)
		if !ok {
			return nil, false
		}
		i = next
		var params []extParam
		for {
			i = skipWS(v, i)
			if i >= len(v) {
				break
			}
			if v[i] == ',' {
				i++
				break
			}
			if v[i] != ';' {
				return nil, false
			}
			i = skipWS(v, i+1)
			key, next, ok := readToken(v, i)
			if !ok {
				return nil, false
			}
			i = skipWS(v, next)
			param := extParam{key: key}
			if i < len(v) && v[i] == '=' {
				i = skipWS(v, i+1)
				if i >= len(v) {
					return nil, false
				}
				if v[i] == '"' {
					val, next, ok := readQuoted(v, i)
					if !ok {
						return nil, false
					}
					param.val = val
					param.hasVal = true
					i = next
				} else {
					val, next, ok := readToken(v, i)
					if !ok {
						return nil, false
					}
					param.val = val
					param.hasVal = true
					i = next
				}
			}
			params = append(params, param)
		}
		offers = append(offers, extOffer{name: name, params: params})
	}
}

func skipWS(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	return i
}

func readToken(s string, i int) (string, int, bool) {
	start := i
	for i < len(s) && isTChar(s[i]) {
		i++
	}
	if i == start {
		return "", i, false
	}
	return s[start:i], i, true
}

func readQuoted(s string, i int) (string, int, bool) {
	if i >= len(s) || s[i] != '"' {
		return "", i, false
	}
	i++
	var b strings.Builder
	for i < len(s) {
		c := s[i]
		switch {
		case c == '"':
			return b.String(), i + 1, true
		case c == '\\':
			if i+1 >= len(s) {
				return "", i, false
			}
			n := s[i+1]
			if n < 0x20 && n != '\t' {
				return "", i, false
			}
			b.WriteByte(n)
			i += 2
		case c < 0x20:
			return "", i, false
		default:
			b.WriteByte(c)
			i++
		}
	}
	return "", i, false
}

func isTChar(c byte) bool {
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// Package ws adapts github.com/coder/websocket to a small session type.
// The root module does not import the WebSocket library, so transport types
// stay out of the public API and a later transport can implement the same
// shape.
package ws

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/coder/websocket"
)

// DefaultReadLimit is the maximum inbound WebSocket frame size in bytes.
const DefaultReadLimit int64 = 32 * 1024

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
	// ReadLimit is the maximum inbound frame size. Zero selects DefaultReadLimit.
	ReadLimit int64
}

// Session is one accepted WebSocket.
//
// Read must not run concurrently with itself. Write, Ping, and CloseNow may
// run alongside Read. coder/websocket closes the underlying socket when a
// context passed to Read, Write, or Ping ends, including after a successful
// call if that context is later canceled. Callers pass a timeout context for
// a single write or ping and cancel it only after the call returns. The
// long-lived read uses a context that is not canceled to express a deadline.
type Session struct {
	c *websocket.Conn
}

// Upgrade hijacks r into a WebSocket session.
// On failure Accept has already written the HTTP response.
func Upgrade(w http.ResponseWriter, r *http.Request, cfg Config) (*Session, error) {
	limit := cfg.ReadLimit
	if limit == 0 {
		limit = DefaultReadLimit
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:     cfg.OriginPatterns,
		InsecureSkipVerify: cfg.InsecureSkipOrigin,
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		return nil, err
	}
	c.SetReadLimit(limit)
	return &Session{c: c}, nil
}

// Read returns the next data frame.
// binary is true for a binary frame. The returned buffer is owned by the
// caller only until the next Read on this session; copy it to retain it.
func (s *Session) Read(ctx context.Context) (binary bool, data []byte, err error) {
	typ, data, err := s.c.Read(ctx)
	if err != nil {
		return false, nil, translate(err)
	}
	return typ == websocket.MessageBinary, data, nil
}

// Write sends one binary-safe text frame. The payload is not retained after
// Write returns. Server frames are not masked, and Write does not mutate p.
func (s *Session) Write(ctx context.Context, p []byte) error {
	if err := s.c.Write(ctx, websocket.MessageText, p); err != nil {
		return translate(err)
	}
	return nil
}

// Ping writes a ping and waits until the peer's pong is observed by Read.
// Ping must run concurrently with an active Read.
func (s *Session) Ping(ctx context.Context) error {
	if err := s.c.Ping(ctx); err != nil {
		return translate(err)
	}
	return nil
}

// CloseNow aborts the socket without a close handshake.
// Use this while Read is blocked. Conn.Close needs the read lock, which Read
// holds, and would wait on that lock until its handshake timeout.
func (s *Session) CloseNow() error {
	if s == nil || s.c == nil {
		return nil
	}
	return s.c.CloseNow()
}

func translate(err error) error {
	if err == nil {
		return nil
	}
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		return &CloseError{Code: int(ce.Code), Reason: ce.Reason}
	}
	return err
}

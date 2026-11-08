package gorealtime

import "context"

// session is the transport used by one connection.
// *ws.Session implements it. Tests use a fake that can block in Write.
type session interface {
	Read(ctx context.Context) (binary bool, data []byte, err error)
	Write(ctx context.Context, p []byte) error
	Ping(ctx context.Context) error
	CloseNow() error
}

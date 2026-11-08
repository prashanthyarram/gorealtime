package gorealtime

import (
	"context"
	"fmt"
	"sync"
	"time"
	"unicode"
)

const (
	maxIdentityBytes = 256
	maxMetaKeyBytes  = 64
	maxMetaValBytes  = 256
	maxMetaKeys      = 32
)

// State is the lifecycle of one connection.
type State int

const (
	// StateConnecting means the handshake succeeded and OnConnect has not finished.
	StateConnecting State = iota
	// StateOpen means OnConnect returned nil and the socket is being read.
	StateOpen
	// StateClosed means cleanup has finished. The value stays closed.
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateConnecting:
		return "connecting"
	case StateOpen:
		return "open"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Disconnect reasons passed to OnDisconnect. The first cleanup wins.
const (
	ReasonClientClose     = "client_close"
	ReasonServerClose     = "server_close"
	ReasonShutdown        = "shutdown"
	ReasonReadError       = "read_error"
	ReasonWriteError      = "write_error"
	ReasonConnectRejected = "connect_rejected"
	ReasonSlowConsumer    = "slow_consumer"
)

// Info is a snapshot of a connection. Metadata is a copy.
// The library does not log Info. Metadata may contain values the application
// considers sensitive; treat the snapshot accordingly.
type Info struct {
	ID               string
	Identity         string
	RemoteAddr       string
	Metadata         map[string]string
	ConnectedAt      time.Time
	LastReadAt       time.Time
	LastWriteAt      time.Time
	State            State
	DisconnectReason string
	QueueLen         int
	Groups           []string
}

// ConnectHandler runs after the handshake and before the hello frame.
// A non-nil error rejects the connection. The hook runs on the connection
// goroutine. It must return promptly, honor ctx, and must not call
// Server.Shutdown: Shutdown waits for this goroutine.
//
// A panic is recovered, logged with its stack, and treated as an error.
// The connection is rejected. OnDisconnect is not called.
type ConnectHandler func(ctx context.Context, conn *Connection) error

// DisconnectHandler runs after the socket has been closed, and only if
// OnConnect returned nil while the connection was still open.
// reason is one of the Reason constants. ctx is already canceled.
// The hook runs on the connection goroutine and must not call Server.Shutdown.
// A panic is recovered and logged. Later disconnect hooks still run.
type DisconnectHandler func(ctx context.Context, conn *Connection, reason string)

// Connection is one accepted client in this process.
// The application sets identity and metadata. The client cannot.
// Methods are safe to call from several goroutines.
type Connection struct {
	id          string
	remoteAddr  string
	sess        session
	server      *Server
	connectedAt time.Time
	out         chan []byte
	slowTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	// mu guards the fields below. Take the registry lock first if both are needed.
	mu          sync.Mutex
	identity    string
	meta        map[string]string
	state       State
	reason      string
	established bool
	reading     bool
	lastReadAt  time.Time
	lastWriteAt time.Time
	saturatedAt time.Time
	finishOnce  sync.Once
}

func newConnection(id, remote string, sess session, server *Server) *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	return &Connection{
		id:          id,
		remoteAddr:  remote,
		sess:        sess,
		server:      server,
		connectedAt: time.Now(),
		ctx:         ctx,
		cancel:      cancel,
		meta:        make(map[string]string),
		state:       StateConnecting,
	}
}

// ID is the process-local connection id. It does not change.
func (c *Connection) ID() string { return c.id }

// Context is canceled when cleanup starts. Hooks and handlers should stop
// work when it ends. It is not the HTTP request context.
func (c *Connection) Context() context.Context { return c.ctx }

// Identity is the application-defined identity, or empty when unset.
// Several connections may share one identity.
func (c *Connection) Identity() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.identity
}

// SetIdentity indexes this connection under identity.
// An empty identity removes it from the index. The client cannot call this;
// there is no protocol message that sets identity.
func (c *Connection) SetIdentity(identity string) error {
	if err := validateIdentity(identity); err != nil {
		return err
	}
	return c.server.reg.setIdentity(c, identity)
}

// Metadata returns a copy of the connection metadata.
func (c *Connection) Metadata() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return copyMeta(c.meta)
}

// SetMetadata stores one pair. The key and value are not read from the client.
func (c *Connection) SetMetadata(key, value string) error {
	if err := validateMeta(key, value); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == StateClosed {
		return ErrConnectionClosed
	}
	if _, exists := c.meta[key]; !exists && len(c.meta) >= maxMetaKeys {
		return fmt.Errorf("%w: at most %d keys", ErrInvalidMetadata, maxMetaKeys)
	}
	c.meta[key] = value
	return nil
}

// DeleteMetadata removes key. A missing key is not an error.
// It returns ErrConnectionClosed when the connection is already closed.
func (c *Connection) DeleteMetadata(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == StateClosed {
		return ErrConnectionClosed
	}
	delete(c.meta, key)
	return nil
}

// State returns the current lifecycle state.
func (c *Connection) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// Info returns a snapshot. LastReadAt or LastWriteAt is zero when that
// direction has not yet seen a data frame. QueueLen and Groups are
// snapshots and may change immediately.
func (c *Connection) Info() Info {
	var groups []string
	if c.server != nil {
		groups = c.server.groups.list(c.id)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	qlen := 0
	if c.out != nil {
		qlen = len(c.out)
	}
	return Info{
		ID:               c.id,
		Identity:         c.identity,
		RemoteAddr:       c.remoteAddr,
		Metadata:         copyMeta(c.meta),
		QueueLen:         qlen,
		Groups:           groups,
		ConnectedAt:      c.connectedAt,
		LastReadAt:       c.lastReadAt,
		LastWriteAt:      c.lastWriteAt,
		State:            c.state,
		DisconnectReason: c.reason,
	}
}

// Close asks the connection to stop with ReasonServerClose.
// It is safe to call more than once. The first reason wins.
//
// When the read loop is running, Close cancels Context and aborts the socket,
// and the read loop runs OnDisconnect after the current handler returns.
// Close itself does not wait for that. A handler must return when ctx is done
// and must not wait for another frame on this connection.
func (c *Connection) Close() {
	c.signalClose(ReasonServerClose)
}

// signalClose records reason and either finishes now or wakes the read loop.
// The read loop finishes when it is already running, so OnDisconnect does not
// overlap the handler.
func (c *Connection) signalClose(reason string) {
	c.mu.Lock()
	if c.state == StateClosed {
		c.mu.Unlock()
		return
	}
	if c.reason == "" {
		c.reason = reason
	}
	reading := c.reading
	c.mu.Unlock()
	if !reading {
		c.finish(reason)
		return
	}
	c.cancel()
	if c.sess != nil {
		_ = c.sess.CloseNow()
	}
}

func (c *Connection) markRead() {
	c.mu.Lock()
	c.lastReadAt = time.Now()
	c.mu.Unlock()
}

func (c *Connection) markWrite() {
	c.mu.Lock()
	c.lastWriteAt = time.Now()
	c.mu.Unlock()
}

func (c *Connection) configureQueue(size int, slow time.Duration) {
	if size < 1 {
		size = 1
	}
	c.out = make(chan []byte, size)
	c.slowTimeout = slow
}

// enqueue queues one immutable frame. It does not wait for the socket.
// A full queue returns ErrQueueFull, or ErrSlowConsumer once the connection
// has stayed saturated for its timeout.
func (c *Connection) enqueue(ctx context.Context, frame []byte) error {
	if c.out == nil {
		return ErrConnectionClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-c.ctx.Done():
		return ErrConnectionClosed
	case <-ctx.Done():
		return ctx.Err()
	case c.out <- frame:
		c.clearSaturated()
		return nil
	default:
	}
	if c.markSaturated() {
		c.signalClose(ReasonSlowConsumer)
		return ErrSlowConsumer
	}
	return ErrQueueFull
}

func (c *Connection) clearSaturated() {
	c.mu.Lock()
	c.saturatedAt = time.Time{}
	c.mu.Unlock()
}

func (c *Connection) markSaturated() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == StateClosed {
		return false
	}
	now := time.Now()
	if c.saturatedAt.IsZero() {
		c.saturatedAt = now
	}
	if c.slowTimeout <= 0 {
		return false
	}
	return now.Sub(c.saturatedAt) >= c.slowTimeout
}

// writeLoop is the only goroutine that writes data frames for c.
func (c *Connection) writeLoop() {
	defer c.server.wg.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case frame := <-c.out:
			if err := c.writeRaw(frame); err != nil {
				c.signalClose(ReasonWriteError)
				return
			}
		}
	}
}

// finish closes the socket, drops indexes, and runs OnDisconnect at most once.
// The first reason is kept.
func (c *Connection) finish(reason string) {
	c.finishOnce.Do(func() {
		c.mu.Lock()
		if c.reason == "" {
			c.reason = reason
		}
		reason = c.reason
		established := c.established
		c.state = StateClosed
		c.mu.Unlock()

		c.cancel()
		if c.sess != nil {
			_ = c.sess.CloseNow()
		}
		if c.server != nil {
			_ = c.server.reg.remove(c)
			c.server.groups.drop(c)
			c.server.opts.log.Info("connection.close", "id", c.id, "reason", reason)
			if established {
				c.server.fireDisconnect(c, reason)
			}
		}
	})
}

func copyMeta(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func validateIdentity(identity string) error {
	if len(identity) > maxIdentityBytes {
		return fmt.Errorf("%w: longer than %d bytes", ErrInvalidIdentity, maxIdentityBytes)
	}
	if hasControl(identity) {
		return fmt.Errorf("%w: control character", ErrInvalidIdentity)
	}
	return nil
}

func validateMeta(key, value string) error {
	if key == "" || len(key) > maxMetaKeyBytes {
		return fmt.Errorf("%w: key must be 1 to %d bytes", ErrInvalidMetadata, maxMetaKeyBytes)
	}
	if len(value) > maxMetaValBytes {
		return fmt.Errorf("%w: value longer than %d bytes", ErrInvalidMetadata, maxMetaValBytes)
	}
	if hasControl(key) || hasControl(value) {
		return fmt.Errorf("%w: control character", ErrInvalidMetadata)
	}
	return nil
}

func hasControl(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f || unicode.IsControl(r) {
			return true
		}
	}
	return false
}

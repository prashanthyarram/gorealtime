package gorealtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prashanth-yarram/gorealtime/internal/protocol"
	"github.com/prashanth-yarram/gorealtime/internal/ws"
)

// writeTimeout bounds a single outbound frame in this milestone.
// Later milestones make the write timeout configurable.
const writeTimeout = 10 * time.Second

// RouteRegistrar is the part of a router required to mount the handshake.
// *http.ServeMux implements it. Third-party routers that provide Handle do too.
type RouteRegistrar interface {
	Handle(pattern string, handler http.Handler)
}

// Server accepts WebSocket connections and owns their process-local lifetime.
//
// The caller owns the HTTP server, TLS, and middleware. Register the handler
// on the caller's mux. Shutdown stops new handshakes and closes open sockets.
// Shutdown is safe to call more than once and from several goroutines.
// Do not call Shutdown from a connection hook. The hook runs on the connection
// goroutine, and Shutdown waits for that goroutine.
type Server struct {
	opts       options
	instanceID string

	reg    registry
	groups groupTable

	hookMu       sync.Mutex
	onConnect    []ConnectHandler
	onDisconnect []DisconnectHandler
	handlers     map[string]Handler
	onAck        AckHandler

	shutOnce sync.Once
	shutDone chan struct{}
	wg       sync.WaitGroup
}

// NewServer builds a server that accepts connections until Shutdown.
// Options that set a forbidden origin pattern panic; see WithOriginPatterns.
// NewServer panics if crypto/rand cannot mint an instance id. That is an
// environment failure, not a per-request error.
func NewServer(opts ...Option) *Server {
	o := defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	o.normalize()
	return &Server{
		opts:       o,
		instanceID: mustInstanceID(),
		reg:        newRegistry(),
		groups:     newGroupTable(),
		shutDone:   make(chan struct{}),
	}
}

// Path returns the path RegisterRoutes mounts.
func (s *Server) Path() string { return s.opts.path }

// InstanceID returns the id stamped on server-originated frames.
// It identifies this process. It is not a secret and it is not stable across restarts.
func (s *Server) InstanceID() string { return s.instanceID }

// ConnectionCount returns the number of connections currently registered.
// The count includes a connection from the moment it is registered until
// cleanup finishes, including one still inside OnConnect.
func (s *Server) ConnectionCount() int { return s.reg.count() }

// Connection returns the registered connection with id.
// A closed connection is absent. The bool is false when id is unknown.
func (s *Server) Connection(id string) (*Connection, bool) { return s.reg.get(id) }

// Connections returns a snapshot of registered connections.
// The slice order is not defined. A connection in the slice may close
// immediately after the call returns.
func (s *Server) Connections() []*Connection { return s.reg.list() }

// ConnectionsByIdentity returns a snapshot of connections indexed under identity.
// The order is not defined. An empty identity is not indexed and returns nil.
// Several connections may share one identity.
func (s *Server) ConnectionsByIdentity(identity string) []*Connection {
	if identity == "" {
		return nil
	}
	return s.reg.byIdentity(identity)
}

// OnConnect registers a hook. Hooks run in registration order.
// A nil hook panics. Registering from inside a hook is safe.
func (s *Server) OnConnect(h ConnectHandler) {
	if h == nil {
		panic("gorealtime: nil connect handler")
	}
	s.hookMu.Lock()
	s.onConnect = append(s.onConnect, h)
	s.hookMu.Unlock()
}

// OnDisconnect registers a hook. Hooks run in registration order.
// A nil hook panics.
func (s *Server) OnDisconnect(h DisconnectHandler) {
	if h == nil {
		panic("gorealtime: nil disconnect handler")
	}
	s.hookMu.Lock()
	s.onDisconnect = append(s.onDisconnect, h)
	s.hookMu.Unlock()
}

// Handler is the handshake endpoint. Mount it at the path clients should dial.
// The handler does not inspect the request path; the router decides that.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(s.serveHTTP)
}

// RegisterRoutes mounts Handler at Path.
func (s *Server) RegisterRoutes(mux RouteRegistrar) {
	mux.Handle(s.opts.path, s.Handler())
}

// Shutdown stops new handshakes and closes open connections.
// It waits until connection goroutines exit or ctx ends. A canceled ctx does
// not undo the shutdown: later calls still report the closed state, and they
// wait again with their own context. The first call performs the close.
// Connections closed by Shutdown disconnect with ReasonShutdown when their
// connect hook already succeeded.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.beginShutdown()
	select {
	case <-s.shutDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) beginShutdown() {
	s.shutOnce.Do(func() {
		conns := s.reg.drain()
		for _, c := range conns {
			c.signalClose(ReasonShutdown)
		}
		go func() {
			s.wg.Wait()
			close(s.shutDone)
		}()
	})
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !s.reg.acceptingNow() {
		http.Error(w, "server closed", http.StatusServiceUnavailable)
		return
	}

	id, err := newID()
	if err != nil {
		s.opts.log.Error("connection.id", "err", err.Error())
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	sess, err := ws.Upgrade(w, r, ws.Config{
		OriginPatterns:     s.opts.originPatterns,
		InsecureSkipOrigin: s.opts.insecureSkipOrigin,
		ReadLimit:          int64(s.opts.maxPayload) + protocol.MaxOverhead,
	})
	if err != nil {
		// Accept already wrote the status. Log a bounded error string and
		// not the request headers.
		s.opts.log.Warn("connection.upgrade", "remote", r.RemoteAddr, "err", boundErr(err))
		return
	}

	c := newConnection(id, r.RemoteAddr, sess, s)
	c.configureQueue(s.opts.queueSize, s.opts.slowTimeout)
	if !s.reg.insert(c, func() { s.wg.Add(2) }) {
		c.finish(ReasonShutdown)
		return
	}
	go c.writeLoop()
	go s.runConnection(c)
}

func (s *Server) runConnection(c *Connection) {
	defer s.wg.Done()
	reason := ReasonReadError
	defer func() { c.finish(reason) }()

	c.mu.Lock()
	c.reading = true
	if c.state == StateClosed {
		if c.reason != "" {
			reason = c.reason
		}
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	if err := s.runConnectHooks(c); err != nil {
		reason = ReasonConnectRejected
		s.opts.log.Info("connection.reject", "id", c.ID(), "err", boundErr(err))
		return
	}

	c.mu.Lock()
	if c.state == StateClosed {
		c.mu.Unlock()
		return
	}
	c.established = true
	c.state = StateOpen
	c.mu.Unlock()

	s.opts.log.Info("connection.open", "id", c.ID(), "remote", c.remoteAddr)

	hello, err := encodeHello(newEventID(), s.instanceID, c.id)
	if err != nil {
		reason = ReasonWriteError
		s.opts.log.Error("connection.hello", "id", c.ID(), "err", err.Error())
		return
	}
	if err := c.enqueue(context.Background(), hello); err != nil {
		reason = ReasonWriteError
		s.opts.log.Info("connection.hello", "id", c.ID(), "err", boundErr(err))
		return
	}

	// context.Background is intentional. coder/websocket treats cancellation
	// of the read context as a failed read and closes the socket, so a read
	// deadline cannot be expressed that way. CloseNow unblocks this call.
	// Handlers run inline on this goroutine, one event at a time.
	for {
		binary, data, err := c.sess.Read(context.Background())
		if err != nil {
			var ce *ws.CloseError
			if errors.As(err, &ce) {
				reason = ReasonClientClose
				s.opts.log.Info("connection.read", "id", c.ID(), "status", ce.Code)
			} else {
				s.opts.log.Info("connection.read", "id", c.ID(), "err", boundErr(err))
			}
			return
		}
		c.markRead()
		if err := s.handleFrame(c, binary, data); err != nil {
			reason = ReasonWriteError
			return
		}
	}
}

func (s *Server) runConnectHooks(c *Connection) error {
	for _, h := range s.connectHooks() {
		if err := s.callConnect(c, h); err != nil {
			return err
		}
		if c.State() == StateClosed {
			return ErrConnectionClosed
		}
	}
	return nil
}

func (s *Server) callConnect(c *Connection, h ConnectHandler) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			s.opts.log.Error("hook.panic",
				"hook", "connect",
				"id", c.ID(),
				"panic", fmt.Sprint(rec),
				"stack", string(debug.Stack()),
			)
			err = fmt.Errorf("connect hook panic: %v", rec)
		}
	}()
	return h(c.Context(), c)
}

func (s *Server) fireDisconnect(c *Connection, reason string) {
	for _, h := range s.disconnectHooks() {
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					s.opts.log.Error("hook.panic",
						"hook", "disconnect",
						"id", c.ID(),
						"panic", fmt.Sprint(rec),
						"stack", string(debug.Stack()),
					)
				}
			}()
			h(c.Context(), c, reason)
		}()
	}
}

func (s *Server) connectHooks() []ConnectHandler {
	s.hookMu.Lock()
	defer s.hookMu.Unlock()
	out := make([]ConnectHandler, len(s.onConnect))
	copy(out, s.onConnect)
	return out
}

func (s *Server) disconnectHooks() []DisconnectHandler {
	s.hookMu.Lock()
	defer s.hookMu.Unlock()
	out := make([]DisconnectHandler, len(s.onDisconnect))
	copy(out, s.onDisconnect)
	return out
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func newEventID() string {
	id, err := newID()
	if err != nil {
		// The connection id was already minted. A failed event id must not
		// drop the handshake. The value only has to be unique on this process.
		return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	return id
}

func mustInstanceID() string {
	id, err := newID()
	if err != nil {
		panic("gorealtime: crypto/rand failed while creating a server: " + err.Error())
	}
	return id
}

func boundErr(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	s = oneLine(s)
	const max = 200
	if len(s) > max {
		return s[:max]
	}
	return s
}

func oneLine(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\n', '\r':
			b = append(b, ' ')
		default:
			b = append(b, s[i])
		}
	}
	return string(b)
}

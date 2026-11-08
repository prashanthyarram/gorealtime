package gorealtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

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
type Server struct {
	opts       options
	instanceID string

	// mu guards accepting and conns. Do not hold it across upgrade, read, or write.
	mu        sync.Mutex
	accepting bool
	conns     map[string]*conn

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
		accepting:  true,
		conns:      make(map[string]*conn),
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
// cleanup finishes.
func (s *Server) ConnectionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
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
		s.mu.Lock()
		s.accepting = false
		conns := s.conns
		s.conns = make(map[string]*conn)
		s.mu.Unlock()
		for _, c := range conns {
			_ = c.sess.CloseNow()
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

	s.mu.Lock()
	accepting := s.accepting
	s.mu.Unlock()
	if !accepting {
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
		ReadLimit:          ws.DefaultReadLimit,
	})
	if err != nil {
		// Accept already wrote the status. Log a bounded error string and
		// not the request headers.
		s.opts.log.Warn("connection.upgrade", "remote", r.RemoteAddr, "err", boundErr(err))
		return
	}

	c := &conn{
		id:         id,
		remoteAddr: r.RemoteAddr,
		sess:       sess,
	}

	s.mu.Lock()
	if !s.accepting {
		s.mu.Unlock()
		_ = sess.CloseNow()
		return
	}
	s.conns[c.id] = c
	s.wg.Add(1)
	s.mu.Unlock()

	s.opts.log.Info("connection.open", "id", c.id, "remote", c.remoteAddr)
	go s.readLoop(c)
}

func (s *Server) readLoop(c *conn) {
	defer s.wg.Done()
	defer s.detach(c)

	hello, err := encodeHello(newEventID(), s.instanceID, c.id)
	if err != nil {
		s.opts.log.Error("connection.hello", "id", c.id, "err", err.Error())
		return
	}
	wctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	err = c.sess.Write(wctx, hello)
	cancel()
	if err != nil {
		s.opts.log.Info("connection.hello", "id", c.id, "err", boundErr(err))
		return
	}

	// context.Background is intentional. coder/websocket treats cancellation
	// of the read context as a failed read and closes the socket, so a read
	// deadline cannot be expressed that way. CloseNow unblocks this call.
	// Inbound data frames are discarded until the dispatcher exists. Reading
	// them is what lets the transport handle ping, pong, and close.
	for {
		_, _, err := c.sess.Read(context.Background())
		if err != nil {
			var ce *ws.CloseError
			if errors.As(err, &ce) {
				s.opts.log.Info("connection.close", "id", c.id, "status", ce.Code)
			} else {
				s.opts.log.Info("connection.close", "id", c.id, "err", boundErr(err))
			}
			return
		}
	}
}

func (s *Server) detach(c *conn) {
	c.once.Do(func() {
		s.mu.Lock()
		if cur, ok := s.conns[c.id]; ok && cur == c {
			delete(s.conns, c.id)
		}
		s.mu.Unlock()
		_ = c.sess.CloseNow()
		s.opts.log.Info("connection.detach", "id", c.id)
	})
}

type helloMessage struct {
	V       int          `json:"v"`
	Type    string       `json:"type"`
	ID      string       `json:"id"`
	Origin  string       `json:"origin"`
	Payload helloPayload `json:"payload"`
}

type helloPayload struct {
	ConnectionID string `json:"connectionId"`
}

func encodeHello(eventID, instanceID, connectionID string) ([]byte, error) {
	return json.Marshal(helloMessage{
		V:      1,
		Type:   "hello",
		ID:     eventID,
		Origin: instanceID,
		Payload: helloPayload{
			ConnectionID: connectionID,
		},
	})
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

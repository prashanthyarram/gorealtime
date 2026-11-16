package gorealtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prashanthyarram/gorealtime/internal/protocol"
)

// errClientGone means the HTTP peer went away or the session was closed.
var errClientGone = errors.New("gorealtime: client gone")

// pipeSession is the session for SSE and long polling.
// Read waits for a client POST. Write delivers one server frame.
type pipeSession struct {
	kind    string
	in      chan []byte
	out     chan []byte
	idleFor time.Duration

	mu      sync.Mutex
	w       http.ResponseWriter
	flusher http.Flusher
	closed  bool

	done      chan struct{}
	closeOnce sync.Once
	polling   int32
	lastPoll  atomic.Int64
}

func newPipeSession(kind string, queue int, idle time.Duration) *pipeSession {
	if queue < 1 {
		queue = 1
	}
	return &pipeSession{
		kind:    kind,
		in:      make(chan []byte, queue),
		out:     make(chan []byte, queue),
		idleFor: idle,
		done:    make(chan struct{}),
	}
}

func (p *pipeSession) attach(w http.ResponseWriter, f http.Flusher) {
	p.mu.Lock()
	p.w = w
	p.flusher = f
	p.mu.Unlock()
}

func (p *pipeSession) Read(ctx context.Context) (bool, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return false, nil, ctx.Err()
	case <-p.done:
		return false, nil, errClientGone
	case b := <-p.in:
		return false, b, nil
	}
}

func (p *pipeSession) Write(ctx context.Context, frame []byte) error {
	if p.kind == TransportSSE {
		return p.writeSSE(frame)
	}
	cp := append([]byte(nil), frame...)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return errClientGone
	case p.out <- cp:
		return nil
	}
}

func (p *pipeSession) Ping(ctx context.Context) error {
	if p.kind == TransportSSE {
		return p.writeComment("ping")
	}
	if p.idleFor <= 0 {
		return nil
	}
	last := p.lastPoll.Load()
	if last == 0 {
		return nil
	}
	if time.Since(time.Unix(0, last)) > p.idleFor {
		return errClientGone
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return errClientGone
	default:
		return nil
	}
}

func (p *pipeSession) CloseNow() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		close(p.done)
	})
	return nil
}

func (p *pipeSession) push(b []byte) error {
	select {
	case <-p.done:
		return errClientGone
	case p.in <- append([]byte(nil), b...):
		return nil
	default:
		return ErrQueueFull
	}
}

func (p *pipeSession) writeSSE(frame []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.w == nil {
		return errClientGone
	}
	if err := writeSSEData(p.w, frame); err != nil {
		return err
	}
	p.flusher.Flush()
	return nil
}

func (p *pipeSession) writeComment(text string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.w == nil {
		return errClientGone
	}
	if _, err := fmt.Fprintf(p.w, ": %s\n\n", text); err != nil {
		return err
	}
	p.flusher.Flush()
	return nil
}

func writeSSEData(w io.Writer, frame []byte) error {
	lines := bytes.Split(frame, []byte("\n"))
	for _, line := range lines {
		if _, err := w.Write([]byte("data: ")); err != nil {
			return err
		}
		if _, err := w.Write(line); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
	}
	_, err := w.Write([]byte("\n"))
	return err
}

func (p *pipeSession) beginPoll() bool {
	return atomic.CompareAndSwapInt32(&p.polling, 0, 1)
}

func (p *pipeSession) endPoll() {
	p.lastPoll.Store(time.Now().UnixNano())
	atomic.StoreInt32(&p.polling, 0)
}

func (p *pipeSession) take(ctx context.Context, wait time.Duration) ([][]byte, error) {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	var first []byte
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.done:
		return nil, errClientGone
	case <-timer.C:
		return nil, nil
	case first = <-p.out:
	}
	out := [][]byte{first}
	for len(out) < 16 {
		select {
		case more := <-p.out:
			out = append(out, more)
		default:
			return out, nil
		}
	}
	return out, nil
}

type pollGate struct {
	mu  sync.Mutex
	n   int
	max int
}

func (g *pollGate) acquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.max > 0 && g.n >= g.max {
		return false
	}
	g.n++
	return true
}

func (g *pollGate) release() {
	g.mu.Lock()
	if g.n > 0 {
		g.n--
	}
	g.mu.Unlock()
}

func (s *Server) serveSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.opts.transportEnabled(TransportSSE) {
		http.Error(w, "transport disabled", http.StatusNotFound)
		return
	}
	if !s.originPermitted(r) {
		http.Error(w, "origin rejected", http.StatusForbidden)
		return
	}
	s.writeCORS(w, r, false)
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	tk, code := s.beginHTTP(w, r, TransportSSE)
	if tk == nil {
		if code != 0 {
			http.Error(w, http.StatusText(code), code)
		}
		return
	}
	out := http.ResponseWriter(w)
	flush := flusher
	if gz, gzFlush, closeGzip, ok := wrapGzip(w, r); ok {
		out = gz
		flush = gzFlush
		defer closeGzip()
	}
	tk.sess.attach(out, flush)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flush.Flush()
	if !s.launch(tk.conn, tk.identity, tk.meta) {
		s.tickets.drop(tk.token)
		return
	}
	select {
	case <-r.Context().Done():
	case <-tk.conn.Context().Done():
	case <-tk.sess.done:
	}
	tk.sess.CloseNow()
}

func (s *Server) servePoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.opts.transportEnabled(TransportLongPolling) {
		http.Error(w, "transport disabled", http.StatusNotFound)
		return
	}
	if !s.originPermitted(r) {
		http.Error(w, "origin rejected", http.StatusForbidden)
		return
	}
	s.writeCORS(w, r, false)
	if !s.polls.acquire() {
		http.Error(w, "too many polls", http.StatusTooManyRequests)
		return
	}
	defer s.polls.release()

	view, ok := s.tickets.view(tokenFrom(r), time.Now())
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	sess := view.sess
	if !view.bound {
		var code int
		tk, code := s.beginHTTP(w, r, TransportLongPolling)
		if tk == nil {
			if code != 0 {
				http.Error(w, http.StatusText(code), code)
			}
			return
		}
		if !s.launch(tk.conn, tk.identity, tk.meta) {
			s.tickets.drop(tk.token)
			http.Error(w, "server closed", http.StatusServiceUnavailable)
			return
		}
		sess = tk.sess
	} else if sess == nil || view.conn == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	} else if _, _, ok := s.authenticate(w, r); !ok {
		return
	}
	if !sess.beginPoll() {
		http.Error(w, "poll already in progress", http.StatusConflict)
		return
	}
	defer sess.endPoll()

	frames, err := sess.take(r.Context(), s.opts.pollTimeout)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errClientGone) {
			return
		}
		return
	}
	if len(frames) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	msgs := make([]json.RawMessage, 0, len(frames))
	for _, f := range frames {
		msgs = append(msgs, json.RawMessage(f))
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	dst, closeBody, _ := gzipWriter(w, r)
	defer closeBody()
	_ = json.NewEncoder(dst).Encode(struct {
		Messages []json.RawMessage `json:"messages"`
	}{Messages: msgs})
}

func (s *Server) serveSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.originPermitted(r) {
		http.Error(w, "origin rejected", http.StatusForbidden)
		return
	}
	s.writeCORS(w, r, false)
	view, ok := s.tickets.view(tokenFrom(r), time.Now())
	if !ok || !view.bound || view.sess == nil || view.conn == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if _, _, ok := s.authenticate(w, r); !ok {
		return
	}
	limit := int64(s.opts.maxPayload) + int64(protocol.MaxOverhead)
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	if len(body) == 0 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := view.sess.push(body); err != nil {
		if errors.Is(err, ErrQueueFull) {
			s.noteQueueSaturated()
			http.Error(w, "queue full", http.StatusTooManyRequests)
			return
		}
		http.Error(w, "connection closed", http.StatusGone)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// beginHTTP authenticates, claims the token, and builds a connection that
// is not yet registered. On failure it writes nothing when code is 0
// because authenticate already wrote the status.
func (s *Server) beginHTTP(w http.ResponseWriter, r *http.Request, transport string) (*ticket, int) {
	if !s.reg.acceptingNow() {
		return nil, http.StatusServiceUnavailable
	}
	token := tokenFrom(r)
	tk, code := s.tickets.claim(token, time.Now())
	if tk == nil {
		if code == http.StatusUnauthorized {
			s.noteAuthFailed()
		}
		return nil, code
	}
	identity, meta, ok := s.authenticate(w, r)
	if !ok {
		s.tickets.release(token)
		return nil, 0
	}
	if s.overCapacity() {
		s.tickets.release(token)
		return nil, http.StatusServiceUnavailable
	}
	idle := s.opts.pollTimeout + s.opts.heartbeatTimeout
	sess := newPipeSession(transport, s.opts.queueSize, idle)
	c := newConnection(tk.id, r.RemoteAddr, sess, s)
	c.configureQueue(s.opts.queueSize, s.opts.slowTimeout)
	c.transport = transport
	c.accessToken = token
	s.tickets.attach(token, sess, c, identity, meta)
	tk.sess = sess
	tk.conn = c
	tk.identity = identity
	tk.meta = meta
	s.noteTransport(transport)
	return tk, http.StatusOK
}

// launch publishes c and starts its goroutines.
// It returns false when the server is closed, at capacity, or the
// identity cannot be recorded. The caller must not use c after false.
func (s *Server) launch(c *Connection, identity string, meta map[string]string) bool {
	ok, full := s.reg.insert(c, func() { s.wg.Add(2) })
	if !ok {
		reason := ReasonShutdown
		if full {
			reason = ReasonServerClose
		}
		c.finish(reason)
		if full {
			s.opts.log.Info("connection.rejected", "id", c.ID(), "reason", "capacity")
		}
		return false
	}
	if err := c.applyAuth(identity, meta); err != nil {
		c.finish(ReasonConnectRejected)
		s.wg.Done()
		s.wg.Done()
		return false
	}
	s.noteOpened()
	go c.writeLoop()
	go s.runConnection(c)
	return true
}

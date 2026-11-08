package gorealtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type scriptSession struct {
	mu      sync.Mutex
	block   bool
	wake    chan struct{}
	closed  chan struct{}
	got     [][]byte
	inWrite atomic.Int32
	discard bool
	sink    int
}

func newScriptSession(block bool) *scriptSession {
	return &scriptSession{block: block, wake: make(chan struct{}), closed: make(chan struct{})}
}

func (s *scriptSession) setBlock(block bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.block = block
	close(s.wake)
	s.wake = make(chan struct{})
}

func (s *scriptSession) Read(ctx context.Context) (bool, []byte, error) {
	select {
	case <-s.closed:
		return false, nil, errors.New("closed")
	case <-ctx.Done():
		return false, nil, ctx.Err()
	}
}

func (s *scriptSession) Write(ctx context.Context, p []byte) error {
	for {
		s.mu.Lock()
		block := s.block
		wake := s.wake
		s.mu.Unlock()
		if !block {
			if s.discard {
				s.sink += len(p)
				return nil
			}
			s.mu.Lock()
			s.got = append(s.got, append([]byte(nil), p...))
			s.mu.Unlock()
			return nil
		}
		s.inWrite.Add(1)
		var writeErr error
		select {
		case <-s.closed:
			writeErr = errors.New("closed")
		case <-wake:
		case <-ctx.Done():
			writeErr = ctx.Err()
		}
		s.inWrite.Add(-1)
		if writeErr != nil {
			return writeErr
		}
	}
}

func (s *scriptSession) Ping(ctx context.Context) error { return nil }

func (s *scriptSession) CloseNow() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return nil
}

func (s *scriptSession) frames() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.got))
	copy(out, s.got)
	return out
}

func attachLocal(t testing.TB, srv *Server, sess session, queue int, slow time.Duration) *Connection {
	t.Helper()
	id, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	c := newConnection(id, "local", sess, srv)
	c.configureQueue(queue, slow)
	if !srv.reg.tryAdd(c) {
		t.Fatal("not accepting")
	}
	c.mu.Lock()
	c.state = StateOpen
	c.established = true
	c.mu.Unlock()
	srv.wg.Add(1)
	go c.writeLoop()
	return c
}

func TestQueueFullAndSlowConsumer(t *testing.T) {
	srv := NewServer(WithQueueSize(1), WithSlowConsumerTimeout(40*time.Millisecond))
	defer shutdown(t, srv)
	blocked := newScriptSession(true)
	c := attachLocal(t, srv, blocked, 1, 40*time.Millisecond)

	ctx := context.Background()
	if err := c.enqueue(ctx, []byte("prime")); err != nil {
		t.Fatal(err)
	}
	waitInWrite(t, blocked)
	if !fillUntil(t, c, ErrQueueFull) {
		t.Fatal("expected a full queue")
	}
	if c.State() == StateClosed {
		t.Fatal("first overflow closed the connection")
	}
	time.Sleep(60 * time.Millisecond)
	err := c.enqueue(ctx, []byte("y"))
	if !errors.Is(err, ErrSlowConsumer) {
		t.Fatalf("err %v", err)
	}
	if c.State() != StateClosed || c.Info().DisconnectReason != ReasonSlowConsumer {
		t.Fatalf("state %s reason %s", c.State(), c.Info().DisconnectReason)
	}
}

func TestSaturationResets(t *testing.T) {
	srv := NewServer()
	defer shutdown(t, srv)
	sess := newScriptSession(true)
	c := attachLocal(t, srv, sess, 1, 30*time.Millisecond)
	ctx := context.Background()
	if !fillUntil(t, c, ErrQueueFull) {
		t.Fatal("queue never filled")
	}
	sess.setBlock(false)
	deadline := time.Now().Add(time.Second)
	for {
		if err := c.enqueue(ctx, []byte("ok")); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(40 * time.Millisecond)
	sess.setBlock(true)
	errKind := fillUntilErr(t, c)
	if !errors.Is(errKind, ErrQueueFull) {
		t.Fatalf("after reset, err %v", errKind)
	}
}

func waitInWrite(t *testing.T, s *scriptSession) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for s.inWrite.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("writer did not block")
		}
		time.Sleep(time.Millisecond)
	}
}

func fillUntil(t *testing.T, c *Connection, target error) bool {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		err := c.enqueue(ctx, []byte("x"))
		if errors.Is(err, target) {
			return true
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	return false
}

func fillUntilErr(t *testing.T, c *Connection) error {
	t.Helper()
	ctx := context.Background()
	var err error
	for i := 0; i < 8; i++ {
		err = c.enqueue(ctx, []byte("x"))
		if err != nil {
			return err
		}
	}
	return err
}

func TestBroadcastSkipsFullQueue(t *testing.T) {
	srv := NewServer()
	defer shutdown(t, srv)
	good := newScriptSession(false)
	bad := newScriptSession(true)
	okConn := attachLocal(t, srv, good, 4, 0)
	badConn := attachLocal(t, srv, bad, 1, 0)
	ctx := context.Background()
	if err := badConn.enqueue(ctx, []byte("prime")); err != nil && !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	waitInWrite(t, bad)
	if !fillUntil(t, badConn, ErrQueueFull) {
		t.Fatal("blocked queue never filled")
	}
	err := srv.Broadcast(ctx, "tick", map[string]int{"n": 1})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("broadcast %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, frame := range good.frames() {
			var env struct {
				Event   string          `json:"event"`
				Payload json.RawMessage `json:"payload"`
			}
			if json.Unmarshal(frame, &env) == nil && env.Event == "tick" {
				if okConn.ID() == badConn.ID() {
					t.Fatal("ids collided")
				}
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("open connection did not receive the broadcast")
}

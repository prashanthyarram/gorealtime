package gorealtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestConnectHookSetsIdentity(t *testing.T) {
	s := NewServer()
	s.OnConnect(func(ctx context.Context, c *Connection) error {
		if c.State() != StateConnecting {
			return fmt.Errorf("state %s", c.State())
		}
		if err := c.SetIdentity("alice"); err != nil {
			return err
		}
		if err := c.SetMetadata("role", "member"); err != nil {
			return err
		}
		if c.Context() == nil {
			return errors.New("nil context")
		}
		return nil
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	a := dial(t, ts.URL, s.Path(), nil)
	defer a.CloseNow()
	b := dial(t, ts.URL, s.Path(), nil)
	defer b.CloseNow()
	ha := readHello(t, a)
	hb := readHello(t, b)

	ca, ok := s.Connection(ha.Payload.ConnectionID)
	if !ok {
		t.Fatal("missing a")
	}
	cb, ok := s.Connection(hb.Payload.ConnectionID)
	if !ok {
		t.Fatal("missing b")
	}
	if ca.State() != StateOpen || cb.Identity() != "alice" {
		t.Fatalf("a %s %s b %s", ca.State(), ca.Identity(), cb.Identity())
	}
	if ca.Metadata()["role"] != "member" {
		t.Fatalf("meta %+v", ca.Metadata())
	}
	got := s.ConnectionsByIdentity("alice")
	if len(got) != 2 || len(s.Connections()) != 2 {
		t.Fatalf("indexed %d listed %d", len(got), len(s.Connections()))
	}
	if s.ConnectionsByIdentity("") != nil {
		t.Fatal("empty identity should not be indexed")
	}
	info := ca.Info()
	if info.LastWriteAt.IsZero() || info.RemoteAddr == "" || info.ConnectedAt.IsZero() {
		t.Fatalf("info %+v", info)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Write(ctx, websocket.MessageText, []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for ca.Info().LastReadAt.IsZero() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if ca.Info().LastReadAt.IsZero() {
		t.Fatal("last read was not recorded")
	}
}

func TestIdentityMoveAndCloseOneOfMany(t *testing.T) {
	s := NewServer()
	s.OnConnect(func(ctx context.Context, c *Connection) error {
		return c.SetIdentity("alice")
	})
	var reasons []string
	var mu sync.Mutex
	s.OnDisconnect(func(ctx context.Context, c *Connection, reason string) {
		mu.Lock()
		reasons = append(reasons, reason)
		mu.Unlock()
		if c.Identity() != "bob" && c.Identity() != "alice" {
			t.Errorf("identity %q", c.Identity())
		}
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	a := dial(t, ts.URL, s.Path(), nil)
	b := dial(t, ts.URL, s.Path(), nil)
	ha := readHello(t, a)
	_ = readHello(t, b)
	ca, _ := s.Connection(ha.Payload.ConnectionID)
	if err := ca.SetIdentity("bob"); err != nil {
		t.Fatal(err)
	}
	if len(s.ConnectionsByIdentity("alice")) != 1 || len(s.ConnectionsByIdentity("bob")) != 1 {
		t.Fatalf("alice %d bob %d", len(s.ConnectionsByIdentity("alice")), len(s.ConnectionsByIdentity("bob")))
	}
	if err := ca.SetIdentity(""); err != nil {
		t.Fatal(err)
	}
	if s.ConnectionsByIdentity("bob") != nil {
		t.Fatal("bob still indexed")
	}
	if err := ca.SetIdentity("bob"); err != nil {
		t.Fatal(err)
	}

	if err := a.Close(websocket.StatusNormalClosure, ""); err != nil {
		t.Fatal(err)
	}
	waitCount(t, s, 1)
	if len(s.ConnectionsByIdentity("bob")) != 0 || len(s.ConnectionsByIdentity("alice")) != 1 {
		t.Fatalf("after close alice %d bob %d", len(s.ConnectionsByIdentity("alice")), len(s.ConnectionsByIdentity("bob")))
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(reasons)
		mu.Unlock()
		if n >= 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	got := append([]string(nil), reasons...)
	mu.Unlock()
	if len(got) != 1 || got[0] != ReasonClientClose {
		t.Fatalf("reasons %v", got)
	}
	b.CloseNow()
}

func TestConnectRejectSkipsDisconnect(t *testing.T) {
	var disconnected atomic.Int32
	s := NewServer()
	var stage atomic.Int32
	s.OnConnect(func(ctx context.Context, c *Connection) error {
		if stage.Add(1) == 1 {
			return nil
		}
		return errors.New("denied")
	})
	s.OnConnect(func(ctx context.Context, c *Connection) error {
		stage.Add(10)
		return errors.New("second should not run after denial")
	})
	s.OnDisconnect(func(ctx context.Context, c *Connection, reason string) {
		disconnected.Add(1)
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := c.Read(ctx); err == nil {
		t.Fatal("expected hello to fail")
	}
	waitCount(t, s, 0)
	if disconnected.Load() != 0 {
		t.Fatalf("disconnect hook ran")
	}
}

func TestConnectHookOrderAndPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := NewServer(WithLogger(logger))
	var order []int
	s.OnConnect(func(ctx context.Context, c *Connection) error {
		order = append(order, 1)
		return nil
	})
	s.OnConnect(func(ctx context.Context, c *Connection) error {
		order = append(order, 2)
		panic("boom-panic")
	})
	s.OnConnect(func(ctx context.Context, c *Connection) error {
		order = append(order, 3)
		return nil
	})
	var disconnected atomic.Int32
	s.OnDisconnect(func(ctx context.Context, c *Connection, reason string) {
		disconnected.Add(1)
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, _ = c.Read(ctx)
	waitCount(t, s, 0)
	// Shutdown waits for the connection goroutine, including the close log
	// that follows removal. Reading the log buffer before that races.
	shutdown(t, s)
	if strings.Join(ints(order), ",") != "1,2" {
		t.Fatalf("order %v", order)
	}
	if disconnected.Load() != 0 {
		t.Fatal("disconnect after panic")
	}
	if !strings.Contains(buf.String(), "boom-panic") || !strings.Contains(buf.String(), "hook.panic") {
		t.Fatalf("panic was not logged: %s", buf.String())
	}
}

func TestCloseOnceAndShutdownReason(t *testing.T) {
	s := NewServer()
	var mu sync.Mutex
	var reasons []string
	s.OnDisconnect(func(ctx context.Context, c *Connection, reason string) {
		if ctx.Err() == nil {
			t.Errorf("context not canceled")
		}
		mu.Lock()
		reasons = append(reasons, reason)
		mu.Unlock()
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	c := dial(t, ts.URL, s.Path(), nil)
	h := readHello(t, c)
	conn, ok := s.Connection(h.Payload.ConnectionID)
	if !ok {
		t.Fatal("missing")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn.Close()
		}()
	}
	wg.Wait()
	waitCount(t, s, 0)
	mu.Lock()
	if len(reasons) != 1 || reasons[0] != ReasonServerClose {
		t.Fatalf("reasons %v", reasons)
	}
	mu.Unlock()
	if _, ok := s.Connection(conn.ID()); ok {
		t.Fatal("closed connection still indexed")
	}
	if err := conn.SetIdentity("x"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("set identity %v", err)
	}
	if err := conn.SetMetadata("k", "v"); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("set meta %v", err)
	}
	if conn.Info().State != StateClosed || conn.Info().DisconnectReason != ReasonServerClose {
		t.Fatalf("info %+v", conn.Info())
	}
	c.CloseNow()

	c2 := dial(t, ts.URL, s.Path(), nil)
	defer c2.CloseNow()
	_ = readHello(t, c2)
	shutdown(t, s)
	waitCount(t, s, 0)
	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 2 || reasons[1] != ReasonShutdown {
		t.Fatalf("reasons %v", reasons)
	}
}

func TestMetadataIsolationAndValidation(t *testing.T) {
	s := NewServer()
	defer shutdown(t, s)
	c := addLocal(t, s)
	if err := c.SetMetadata("k", "v"); err != nil {
		t.Fatal(err)
	}
	m := c.Metadata()
	m["k"] = "changed"
	m["other"] = "x"
	if c.Metadata()["k"] != "v" || len(c.Metadata()) != 1 {
		t.Fatalf("aliased %+v", c.Metadata())
	}
	if err := c.DeleteMetadata("missing"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteMetadata("k"); err != nil || len(c.Metadata()) != 0 {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		key, val string
	}{
		{"", "v"},
		{"has\nline", "v"},
		{"k", "bad\rvalue"},
		{strings.Repeat("a", maxMetaKeyBytes+1), "v"},
		{"k", strings.Repeat("b", maxMetaValBytes+1)},
	} {
		if err := c.SetMetadata(tc.key, tc.val); !errors.Is(err, ErrInvalidMetadata) {
			t.Fatalf("key %q val len %d err %v", tc.key, len(tc.val), err)
		}
	}
	for i := 0; i < maxMetaKeys; i++ {
		if err := c.SetMetadata(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.SetMetadata("overflow", "v"); !errors.Is(err, ErrInvalidMetadata) {
		t.Fatalf("overflow %v", err)
	}
	if err := c.SetMetadata("k0", "replaced"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetIdentity("ok identity"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetIdentity("bad\nidentity"); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("identity %v", err)
	}
	if err := c.SetIdentity(strings.Repeat("x", maxIdentityBytes+1)); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("long identity %v", err)
	}
	c.Close()
}

func TestSecretsStayOutOfLogs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	const ident = "user-secret-identity"
	const meta = "meta-secret-value"
	const payload = "payload-secret-token"
	s := NewServer(WithLogger(logger))
	s.OnConnect(func(ctx context.Context, c *Connection) error {
		if err := c.SetIdentity(ident); err != nil {
			return err
		}
		return c.SetMetadata("token", meta)
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, []byte(payload)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	var conn *Connection
	for time.Now().Before(deadline) {
		conns := s.Connections()
		if len(conns) == 1 && !conns[0].Info().LastReadAt.IsZero() {
			conn = conns[0]
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if conn == nil {
		t.Fatal("frame was not observed")
	}
	shutdown(t, s)
	logs := buf.String()
	for _, secret := range []string{ident, meta, payload} {
		if strings.Contains(logs, secret) {
			t.Fatalf("log contains %q: %s", secret, logs)
		}
	}
}

func TestRegistryConcurrent(t *testing.T) {
	s := NewServer()
	defer shutdown(t, s)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for j := 0; j < 40; j++ {
				c := addLocal(t, s)
				ident := fmt.Sprintf("u%d", j%4)
				if err := c.SetIdentity(ident); err != nil {
					t.Errorf("set: %v", err)
				}
				if err := c.SetMetadata("g", fmt.Sprintf("%d", g)); err != nil {
					t.Errorf("meta: %v", err)
				}
				_ = s.ConnectionsByIdentity(ident)
				_, _ = s.Connection(c.ID())
				if j%2 == 0 {
					c.SetIdentity("moved")
				}
				c.Close()
				if err := c.SetIdentity("after"); !errors.Is(err, ErrConnectionClosed) {
					t.Errorf("after close: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()
	if s.ConnectionCount() != 0 {
		t.Fatalf("count %d", s.ConnectionCount())
	}
	if s.ConnectionsByIdentity("moved") != nil || s.ConnectionsByIdentity("u0") != nil {
		t.Fatal("identity index leaked")
	}
}

func TestNilHooksPanic(t *testing.T) {
	s := NewServer()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	s.OnConnect(nil)
}

func addLocal(t *testing.T, s *Server) *Connection {
	t.Helper()
	id, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	c := newConnection(id, "local", nil, s)
	if !s.reg.tryAdd(c) {
		t.Fatal("not accepting")
	}
	return c
}

func ints(v []int) []string {
	out := make([]string, len(v))
	for i, n := range v {
		out[i] = fmt.Sprintf("%d", n)
	}
	return out
}

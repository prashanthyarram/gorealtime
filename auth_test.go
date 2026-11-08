package gorealtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestRequireAuthRejectsBeforeUpgrade(t *testing.T) {
	s := NewServer(WithRequireAuth())
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)
	assertDialStatus(t, ts, s.Path(), "http://"+ts.Listener.Addr().String(), http.StatusUnauthorized)
	if s.ConnectionCount() != 0 {
		t.Fatal(s.ConnectionCount())
	}
}

func TestMiddlewareAndAuthenticatorIdentity(t *testing.T) {
	var saw string
	s := NewServer(WithAuthenticator(func(ctx context.Context, r *http.Request) (AuthResult, error) {
		if got, _ := IdentityFromContext(ctx); got != "from-middleware" {
			t.Errorf("middleware identity %q", got)
		}
		if r.Header.Get("Authorization") != "Bearer demo-ok" {
			return AuthResult{}, ErrUnauthorized
		}
		return AuthResult{Identity: "bob", Metadata: map[string]string{"role": "member"}}, nil
	}))
	s.OnConnect(func(ctx context.Context, c *Connection) error {
		saw = c.Identity()
		if c.Metadata()["role"] != "member" {
			t.Errorf("meta %v", c.Metadata())
		}
		return nil
	})
	inner := s.Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := WithIdentity(r.Context(), "from-middleware")
		inner.ServeHTTP(w, r.WithContext(ctx))
	}))
	defer ts.Close()
	defer shutdown(t, s)

	c := dial(t, ts.URL, s.Path(), http.Header{"Authorization": []string{"Bearer demo-ok"}})
	defer c.CloseNow()
	_ = readHello(t, c)
	if saw != "bob" {
		t.Fatal(saw)
	}

	assertDialHeaderStatus(t, ts, s.Path(), "Bearer nope", http.StatusUnauthorized)
}

func TestAuthTokenIsNotLogged(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	const token = "super-secret-token"
	s := NewServer(WithLogger(logger), WithAuthenticator(func(ctx context.Context, r *http.Request) (AuthResult, error) {
		return AuthResult{}, ErrUnauthorized
	}))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)
	assertDialHeaderStatus(t, ts, s.Path(), "Bearer "+token, http.StatusUnauthorized)
	if bytes.Contains(buf.Bytes(), []byte(token)) {
		t.Fatalf("token leaked: %s", buf.String())
	}
}

func TestEventAndGroupAuthorizers(t *testing.T) {
	s := NewServer(WithHeartbeat(0, 0))
	var handled atomic.Bool
	s.On("ping", func(ctx context.Context, c *Connection, ev Event) error {
		handled.Store(true)
		return nil
	})
	s.AuthorizeEvents(func(ctx context.Context, c *Connection, event string) error {
		if event == "ping" {
			return WithMessage(ErrUnauthorized, "no ping")
		}
		return nil
	})
	s.AuthorizeGroups(func(ctx context.Context, c *Connection, group, action string) error {
		if action == GroupJoin && group == "secret" {
			return ErrUnauthorized
		}
		return nil
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)
	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	h := readHello(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	body := `{"v":1,"type":"event","event":"ping","id":"e1"}`
	if err := c.Write(ctx, websocket.MessageText, []byte(body)); err != nil {
		t.Fatal(err)
	}
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		Type  string `json:"type"`
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatal(err)
	}
	if env.Type != "error" || env.Error.Code != CodeUnauthorized || env.Error.Message != "no ping" {
		t.Fatalf("%s", data)
	}
	if handled.Load() {
		t.Fatal("handler ran")
	}
	conn, ok := s.Connection(h.Payload.ConnectionID)
	if !ok {
		t.Fatal("missing")
	}
	if err := conn.Join("secret"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("join %v", err)
	}
	if err := conn.Join("public"); err != nil {
		t.Fatal(err)
	}
	if s.GroupSize("public") != 1 {
		t.Fatal(s.GroupSize("public"))
	}
}

func TestHeartbeatTimeoutAndHealthyPeer(t *testing.T) {
	s := NewServer(WithHeartbeat(40*time.Millisecond, 80*time.Millisecond))
	reasons := make(chan string, 4)
	s.OnDisconnect(func(ctx context.Context, c *Connection, reason string) {
		select {
		case reasons <- reason:
		default:
		}
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	quiet := dial(t, ts.URL, s.Path(), nil)
	defer quiet.CloseNow()
	_ = readHello(t, quiet)
	select {
	case reason := <-reasons:
		if reason != ReasonHeartbeat {
			t.Fatalf("reason %s", reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("quiet peer was not closed")
	}

	live := dial(t, ts.URL, s.Path(), nil)
	defer live.CloseNow()
	go func() {
		for {
			if _, _, err := live.Read(context.Background()); err != nil {
				return
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	if s.ConnectionCount() != 1 {
		t.Fatalf("count %d", s.ConnectionCount())
	}
}

func TestShutdownSendsBye(t *testing.T) {
	s := NewServer(WithHeartbeat(0, 0))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)
	got := make(chan []byte, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, data, err := c.Read(ctx)
		if err != nil {
			got <- nil
			return
		}
		got <- append([]byte(nil), data...)
	}()
	time.Sleep(20 * time.Millisecond)
	shutdown(t, s)
	data := <-got
	if len(data) == 0 {
		t.Fatal("no bye frame")
	}
	var env struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("%s: %v", data, err)
	}
	if env.Type != "bye" || !bytes.Contains(env.Payload, []byte(`"shutdown"`)) {
		t.Fatalf("%s", data)
	}
}

func TestMetricsHooks(t *testing.T) {
	var opened, closed, received atomic.Int32
	s := NewServer(WithHeartbeat(0, 0), WithMetrics(&Metrics{
		ConnectionOpened: func() { opened.Add(1) },
		ConnectionClosed: func(reason string) {
			if reason == "" {
				t.Errorf("empty reason")
			}
			closed.Add(1)
		},
		EventsReceived: func() { received.Add(1) },
		ConnectionsActive: func(n int) {
			if n < 0 {
				t.Errorf("active %d", n)
			}
		},
	}))
	s.On("ping", func(ctx context.Context, c *Connection, ev Event) error { return nil })
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, []byte(`{"v":1,"type":"event","event":"ping"}`)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	shutdown(t, s)
	if opened.Load() != 1 || closed.Load() != 1 || received.Load() != 1 {
		t.Fatalf("opened %d closed %d received %d", opened.Load(), closed.Load(), received.Load())
	}
}

func assertDialHeaderStatus(t *testing.T, ts *httptest.Server, pth, auth string, status int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, wsURL(ts.URL, pth), &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{auth}},
	})
	if err == nil {
		t.Fatal("expected dial error")
	}
	if resp == nil || resp.StatusCode != status {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		t.Fatalf("status %d want %d err %v", code, status, err)
	}
}

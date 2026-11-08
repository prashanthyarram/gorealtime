package gorealtime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestHelloAndDistinctIDs(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	c1 := dial(t, ts.URL, s.Path(), nil)
	defer c1.CloseNow()
	c2 := dial(t, ts.URL, s.Path(), nil)
	defer c2.CloseNow()

	h1 := readHello(t, c1)
	h2 := readHello(t, c2)
	if h1.V != 1 || h1.Type != "hello" || h1.ID == "" || h1.Payload.ConnectionID == "" {
		t.Fatalf("hello: %+v", h1)
	}
	if h1.Origin != s.InstanceID() || h2.Origin != s.InstanceID() {
		t.Fatalf("origin %q %q instance %q", h1.Origin, h2.Origin, s.InstanceID())
	}
	if h1.Payload.ConnectionID == h2.Payload.ConnectionID || h1.ID == h2.ID {
		t.Fatalf("ids not distinct: %+v %+v", h1, h2)
	}
	if s.ConnectionCount() != 2 {
		t.Fatalf("count = %d", s.ConnectionCount())
	}
}

func TestRegisterRoutesAndSibling(t *testing.T) {
	s := NewServer(WithPath("realtime"))
	if s.Path() != "/realtime" {
		t.Fatalf("path %q", s.Path())
	}
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	defer shutdown(t, s)

	res, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("health %d", res.StatusCode)
	}

	c := dial(t, ts.URL, "/realtime", nil)
	defer c.CloseNow()
	if h := readHello(t, c); h.Type != "hello" {
		t.Fatalf("hello %+v", h)
	}

	res, err = http.Get(ts.URL + "/missing")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("missing %d", res.StatusCode)
	}
}

func TestMethodAndBareGet(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	res, err := http.Post(ts.URL+s.Path(), "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status %d", res.StatusCode)
	}

	res, err = http.Get(ts.URL + s.Path())
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode == http.StatusSwitchingProtocols || res.StatusCode >= 500 {
		t.Fatalf("bare GET status %d", res.StatusCode)
	}
}

func TestInboundFrameIsIgnoredAndNotLogged(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := NewServer(WithLogger(logger))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)

	const secret = "super-secret-payload-token"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, []byte(secret)); err != nil {
		t.Fatal(err)
	}
	// Give the server a moment to read the frame. The connection must stay up.
	time.Sleep(50 * time.Millisecond)
	if s.ConnectionCount() != 1 {
		t.Fatalf("count = %d", s.ConnectionCount())
	}
	if strings.Contains(buf.String(), secret) {
		t.Fatalf("payload leaked into logs: %s", buf.String())
	}
}

func TestClientCloseRemovesConnection(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	c := dial(t, ts.URL, s.Path(), nil)
	_ = readHello(t, c)
	if err := c.Close(websocket.StatusNormalClosure, ""); err != nil {
		t.Fatal(err)
	}
	waitCount(t, s, 0)
}

func TestOriginPolicy(t *testing.T) {
	t.Run("reject cross origin", func(t *testing.T) {
		s := NewServer()
		ts := httptest.NewServer(s.Handler())
		defer ts.Close()
		defer shutdown(t, s)
		assertDialStatus(t, ts, s.Path(), "https://evil.example", http.StatusForbidden)
	})

	t.Run("allow same host", func(t *testing.T) {
		s := NewServer()
		ts := httptest.NewServer(s.Handler())
		defer ts.Close()
		defer shutdown(t, s)
		c := dial(t, ts.URL, s.Path(), http.Header{"Origin": []string{ts.URL}})
		defer c.CloseNow()
		if readHello(t, c).Type != "hello" {
			t.Fatal("expected hello")
		}
	})

	t.Run("allow pattern", func(t *testing.T) {
		s := NewServer(WithOriginPatterns("evil.example", "*.trusted.example"))
		ts := httptest.NewServer(s.Handler())
		defer ts.Close()
		defer shutdown(t, s)
		c := dial(t, ts.URL, s.Path(), http.Header{"Origin": []string{"https://evil.example"}})
		defer c.CloseNow()
		_ = readHello(t, c)
		c2 := dial(t, ts.URL, s.Path(), http.Header{"Origin": []string{"https://a.trusted.example"}})
		defer c2.CloseNow()
		_ = readHello(t, c2)
		assertDialStatus(t, ts, s.Path(), "https://nope.example", http.StatusForbidden)
	})

	t.Run("insecure skip", func(t *testing.T) {
		s := NewServer(WithInsecureSkipOriginCheck())
		ts := httptest.NewServer(s.Handler())
		defer ts.Close()
		defer shutdown(t, s)
		c := dial(t, ts.URL, s.Path(), http.Header{"Origin": []string{"https://evil.example"}})
		defer c.CloseNow()
		_ = readHello(t, c)
	})
}

func TestOriginPatternPanics(t *testing.T) {
	for _, p := range []string{"*", "", "https://example.com", "example.com/path"} {
		p := p
		t.Run(p, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			NewServer(WithOriginPatterns(p))
		})
	}
}

func TestShutdownIdempotentAndRejects(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = s.Shutdown(ctx)

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- s.Shutdown(context.Background())
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	waitCount(t, s, 0)

	res, err := http.Get(ts.URL + s.Path())
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d body %s", res.StatusCode, body)
	}

	// The peer blocked in a read must have been released. A second read returns.
	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	defer rcancel()
	if _, _, err := c.Read(rctx); err == nil {
		t.Fatal("expected read to fail after shutdown")
	}
}

func TestConcurrentAcceptAndShutdown(t *testing.T) {
	s := NewServer(WithInsecureSkipOriginCheck())
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			c, _, err := websocket.Dial(ctx, wsURL(ts.URL, s.Path()), nil)
			if err != nil {
				return
			}
			defer c.CloseNow()
			_, _, _ = c.Read(ctx)
		}()
	}
	time.Sleep(20 * time.Millisecond)
	shutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Shutdown(shutCtx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if s.ConnectionCount() != 0 {
		t.Fatalf("count = %d", s.ConnectionCount())
	}
}

func TestNilLoggerAndNilOption(t *testing.T) {
	s := NewServer(nil, WithLogger(nil), WithPath(""))
	if s.Path() != defaultPath {
		t.Fatalf("path %q", s.Path())
	}
	if s.InstanceID() == "" {
		t.Fatal("empty instance id")
	}
}

type helloWire struct {
	V       int    `json:"v"`
	Type    string `json:"type"`
	ID      string `json:"id"`
	Origin  string `json:"origin"`
	Payload struct {
		ConnectionID string `json:"connectionId"`
	} `json:"payload"`
}

func dial(t *testing.T, base, pth string, header http.Header) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	c, _, err := websocket.Dial(ctx, wsURL(base, pth), &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return c
}

func assertDialStatus(t *testing.T, ts *httptest.Server, pth, origin string, status int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, wsURL(ts.URL, pth), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{origin}},
	})
	if err == nil {
		t.Fatal("expected dial error")
	}
	if resp == nil {
		t.Fatalf("dial err %v without response", err)
	}
	if resp.StatusCode != status {
		t.Fatalf("status %d want %d err %v", resp.StatusCode, status, err)
	}
}

func readHello(t *testing.T, c *websocket.Conn) helloWire {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	typ, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("type %v", typ)
	}
	var h helloWire
	if err := json.Unmarshal(data, &h); err != nil {
		t.Fatalf("unmarshal %s: %v", data, err)
	}
	return h
}

func waitCount(t *testing.T, s *Server, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.ConnectionCount() == n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("count = %d want %d", s.ConnectionCount(), n)
}

func shutdown(t *testing.T, s *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func wsURL(base, pth string) string {
	return "ws" + strings.TrimPrefix(base, "http") + pth
}

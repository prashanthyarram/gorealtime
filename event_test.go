package gorealtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestEventReachesHandler(t *testing.T) {
	s := NewServer()
	got := make(chan Event, 1)
	s.On("chat.send", func(ctx context.Context, c *Connection, ev Event) error {
		if c.Identity() != "" {
			t.Errorf("identity %q", c.Identity())
		}
		got <- ev
		return nil
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)
	sendText(t, c, `{"v":1,"type":"event","event":"chat.send","id":"e1","corr":"c1","origin":"spoof","trace":{"span":"s"},"payload":{"text":"hi"}}`)

	select {
	case ev := <-got:
		if ev.Name != "chat.send" || ev.ID != "e1" || ev.CorrelationID != "c1" {
			t.Fatalf("%+v", ev)
		}
		if string(ev.Payload) != `{"text":"hi"}` || ev.Trace["span"] != "s" {
			t.Fatalf("%+v %s", ev.Trace, ev.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called")
	}
}

func TestPayloadCopySurvivesNextFrame(t *testing.T) {
	s := NewServer()
	var mu sync.Mutex
	var kept []byte
	seen := make(chan struct{}, 2)
	s.On("e", func(ctx context.Context, c *Connection, ev Event) error {
		mu.Lock()
		if kept == nil {
			kept = ev.Payload
		}
		mu.Unlock()
		seen <- struct{}{}
		return nil
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)
	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)
	sendText(t, c, `{"v":1,"type":"event","event":"e","payload":{"n":1}}`)
	sendText(t, c, `{"v":1,"type":"event","event":"e","payload":{"n":2,"pad":"`+strings.Repeat("z", 64)+`"}}`)
	waitN(t, seen, 2)
	mu.Lock()
	defer mu.Unlock()
	if string(kept) != `{"n":1}` {
		t.Fatalf("payload overwritten: %s", kept)
	}
}

func TestDispatchErrorsStayOnTheSocket(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s := NewServer(WithLogger(logger))
	s.On("boom", func(ctx context.Context, c *Connection, ev Event) error {
		return errors.New("internal-detail-xyz")
	})
	s.On("public", func(ctx context.Context, c *Connection, ev Event) error {
		return WithMessage(ErrUnauthorized, "not a member")
	})
	s.On("panic", func(ctx context.Context, c *Connection, ev Event) error {
		panic("panic-secret-value")
	})
	next := make(chan struct{}, 1)
	s.On("next", func(ctx context.Context, c *Connection, ev Event) error {
		next <- struct{}{}
		return nil
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)
	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)

	sendText(t, c, `{"v":1,"type":"event","event":"missing","id":"m1"}`)
	assertError(t, c, "m1", CodeInvalidEvent, "")

	sendText(t, c, `{`)
	assertError(t, c, "", CodeInvalidMessage, "")

	sendText(t, c, `{"v":9,"type":"event","event":"a","id":"v9"}`)
	errFrame := assertError(t, c, "", CodeInvalidMessage, "")
	if !strings.Contains(errFrame.Error.Message, "version") {
		t.Fatalf("version message %s", errFrame.Error.Message)
	}

	sendText(t, c, `{"v":1,"type":"event","event":"boom","id":"b1","payload":{"password":"payload-secret"}}`)
	boom := assertError(t, c, "b1", CodeHandlerFailed, "handler failed")
	if strings.Contains(boom.Error.Message, "internal-detail-xyz") || strings.Contains(boom.Error.Message, "payload-secret") {
		t.Fatalf("client saw internals: %+v", boom.Error)
	}

	sendText(t, c, `{"v":1,"type":"event","event":"public","id":"p1"}`)
	assertError(t, c, "p1", CodeUnauthorized, "not a member")

	sendText(t, c, `{"v":1,"type":"event","event":"panic","id":"x1"}`)
	pan := assertError(t, c, "x1", CodeHandlerFailed, "handler failed")
	if strings.Contains(pan.Error.Message, "panic-secret-value") {
		t.Fatal(pan.Error.Message)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageBinary, []byte{0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	assertError(t, c, "", CodeInvalidMessage, "")

	sendText(t, c, `{"v":1,"type":"event","event":"next"}`)
	waitN(t, next, 1)
	if s.ConnectionCount() != 1 {
		t.Fatalf("count %d", s.ConnectionCount())
	}
	shutdown(t, s)
	logs := buf.String()
	if strings.Contains(logs, "payload-secret") {
		t.Fatalf("payload logged: %s", logs)
	}
	if !strings.Contains(logs, "internal-detail-xyz") || !strings.Contains(logs, "panic-secret-value") {
		t.Fatalf("failure was not logged: %s", logs)
	}
}

func TestPayloadLimits(t *testing.T) {
	s := NewServer(WithMaxPayloadBytes(64))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)
	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)

	over := `{"v":1,"type":"event","event":"a","id":"big","payload":"` + strings.Repeat("x", 80) + `"}`
	sendText(t, c, over)
	assertError(t, c, "", CodePayloadTooLarge, "")
	if s.ConnectionCount() != 1 {
		t.Fatal("payload limit closed the socket")
	}

	huge := strings.Repeat("y", 8000)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.Write(ctx, websocket.MessageText, []byte(huge))
	waitCount(t, s, 0)
}

func TestHandlerOrderAndIsolation(t *testing.T) {
	s := NewServer()
	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var order []int
	var n int
	s.On("tick", func(ctx context.Context, c *Connection, ev Event) error {
		mu.Lock()
		n++
		cur := n
		mu.Unlock()
		if cur == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		mu.Lock()
		order = append(order, cur)
		mu.Unlock()
		return nil
	})
	otherDone := make(chan struct{})
	s.On("other", func(ctx context.Context, c *Connection, ev Event) error {
		close(otherDone)
		return nil
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	a := dial(t, ts.URL, s.Path(), nil)
	defer a.CloseNow()
	b := dial(t, ts.URL, s.Path(), nil)
	defer b.CloseNow()
	_ = readHello(t, a)
	_ = readHello(t, b)
	sendText(t, a, `{"v":1,"type":"event","event":"tick"}`)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first handler did not start")
	}
	sendText(t, a, `{"v":1,"type":"event","event":"tick"}`)
	sendText(t, b, `{"v":1,"type":"event","event":"other"}`)
	select {
	case <-otherDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second connection was blocked by the first")
	}
	mu.Lock()
	if len(order) != 0 {
		t.Fatalf("second event ran early: %v", order)
	}
	mu.Unlock()
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := append([]int(nil), order...)
		mu.Unlock()
		if len(got) == 2 {
			if got[0] != 1 || got[1] != 2 {
				t.Fatalf("order %v", got)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("handlers did not finish in order")
}

func TestAckAndReplacement(t *testing.T) {
	s := NewServer()
	which := make(chan string, 1)
	s.On("echo", func(ctx context.Context, c *Connection, ev Event) error {
		which <- "first"
		return nil
	})
	s.On("echo", func(ctx context.Context, c *Connection, ev Event) error {
		which <- "second"
		return nil
	})
	acks := make(chan string, 1)
	s.OnAck(func(ctx context.Context, c *Connection, correlationID string) error {
		acks <- correlationID
		return nil
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)
	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)
	sendText(t, c, `{"v":1,"type":"ack","id":"ack1","corr":"evt-9"}`)
	select {
	case id := <-acks:
		if id != "evt-9" {
			t.Fatal(id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ack was not delivered")
	}
	sendText(t, c, `{"v":1,"type":"event","event":"echo"}`)
	select {
	case got := <-which:
		if got != "second" {
			t.Fatalf("handler %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement handler was not called")
	}
}

func TestAckWithoutHandlerIsQuiet(t *testing.T) {
	s := NewServer()
	got := make(chan struct{}, 1)
	s.On("ping", func(ctx context.Context, c *Connection, ev Event) error {
		got <- struct{}{}
		return nil
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)
	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)
	sendText(t, c, `{"v":1,"type":"ack","corr":"nobody"}`)
	sendText(t, c, `{"v":1,"type":"event","event":"ping"}`)
	waitN(t, got, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, _, err := c.Read(ctx); err == nil {
		t.Fatal("unexpected frame after a quiet ack")
	}
}

func TestShutdownCancelsHandler(t *testing.T) {
	s := NewServer()
	started := make(chan struct{})
	finished := make(chan struct{})
	s.On("wait", func(ctx context.Context, c *Connection, ev Event) error {
		close(started)
		<-ctx.Done()
		close(finished)
		return nil
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	_ = readHello(t, c)
	sendText(t, c, `{"v":1,"type":"event","event":"wait"}`)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	shutdown(t, s)
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not canceled")
	}
}

func TestOnPanicsOnBadHandler(t *testing.T) {
	s := NewServer()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	s.On("bad name", func(ctx context.Context, c *Connection, ev Event) error { return nil })
}

type errorWire struct {
	Type  string `json:"type"`
	ID    string `json:"id"`
	Corr  string `json:"corr"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func assertError(t *testing.T, c *websocket.Conn, corr, code, message string) errorWire {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read error frame: %v", err)
	}
	var w errorWire
	if err := json.Unmarshal(data, &w); err != nil {
		t.Fatalf("unmarshal %s: %v", data, err)
	}
	if w.Type != "error" || w.Error == nil || w.Error.Code != code {
		t.Fatalf("frame %+v body %s", w, data)
	}
	if corr != "" && w.Corr != corr {
		t.Fatalf("corr %q want %q", w.Corr, corr)
	}
	if message != "" && w.Error.Message != message {
		t.Fatalf("message %q want %q", w.Error.Message, message)
	}
	return w
}

func sendText(t *testing.T, c *websocket.Conn, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, []byte(body)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func waitN(t *testing.T, ch <-chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("received %d of %d", i, n)
		}
	}
}

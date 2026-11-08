package gorealtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestSendBroadcastIdentityAndGroup(t *testing.T) {
	s := NewServer()
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
	if err := ca.SetIdentity("alice"); err != nil || cb.SetIdentity("alice") != nil {
		t.Fatal("identity")
	}

	ctx := context.Background()
	if err := s.Send(ctx, ca.ID(), "note", map[string]string{"text": "hi"}, WithEventID("n1")); err != nil {
		t.Fatal(err)
	}
	ev := readEvent(t, a)
	if ev.Event != "note" || ev.ID != "n1" || string(ev.Payload) != `{"text":"hi"}` {
		t.Fatalf("%+v %s", ev, ev.Payload)
	}

	if err := s.SendToIdentity(ctx, "alice", "fan", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if readEvent(t, a).Event != "fan" || readEvent(t, b).Event != "fan" {
		t.Fatal("identity fan-out")
	}

	if err := ca.Join("room"); err != nil || cb.Join("room") != nil {
		t.Fatal("join")
	}
	if s.GroupSize("room") != 2 {
		t.Fatal(s.GroupSize("room"))
	}
	if err := s.SendToGroup(ctx, "room", "msg", map[string]int{"n": 2}, ExcludeConnections(ca.ID())); err != nil {
		t.Fatal(err)
	}
	if got := readEvent(t, b); got.Event != "msg" {
		t.Fatal(got.Event)
	}
	// A timed-out context passed to Read closes the socket, so the check
	// uses a background read and a timer instead of a read deadline.
	gotExtra := make(chan struct{}, 1)
	go func() {
		if _, _, err := a.Read(context.Background()); err == nil {
			gotExtra <- struct{}{}
		}
	}()
	select {
	case <-gotExtra:
		t.Fatal("excluded connection received the group event")
	case <-time.After(150 * time.Millisecond):
	}
	if err := ca.Leave("room"); err != nil {
		t.Fatal(err)
	}
	if s.GroupSize("room") != 1 || len(ca.Info().Groups) != 0 {
		t.Fatalf("size %d groups %v", s.GroupSize("room"), ca.Info().Groups)
	}
	cb.Close()
	waitCount(t, s, 1)
	deadline := time.Now().Add(time.Second)
	for s.GroupSize("room") != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if s.GroupSize("room") != 0 {
		t.Fatal("group leaked the closed connection")
	}

	if err := s.Send(ctx, "missing", "note", nil); !errors.Is(err, ErrConnectionNotFound) {
		t.Fatalf("missing %v", err)
	}
	if err := ca.Join("bad name"); !errors.Is(err, ErrInvalidGroup) {
		t.Fatalf("group %v", err)
	}
	if err := s.Send(ctx, ca.ID(), "note", []byte("not-json")); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("payload %v", err)
	}
}

func TestSendAfterShutdown(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	h := readHello(t, c)
	shutdown(t, s)
	err := s.Send(context.Background(), h.Payload.ConnectionID, "note", nil)
	if !errors.Is(err, ErrServerClosed) && !errors.Is(err, ErrConnectionNotFound) && !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("err %v", err)
	}
	if err := s.Broadcast(context.Background(), "note", nil); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("broadcast %v", err)
	}
}

func TestConcurrentBroadcast(t *testing.T) {
	s := NewServer(WithQueueSize(64))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)
	var clients []*websocket.Conn
	for i := 0; i < 4; i++ {
		c := dial(t, ts.URL, s.Path(), nil)
		defer c.CloseNow()
		_ = readHello(t, c)
		clients = append(clients, c)
		go func(c *websocket.Conn) {
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				_, _, err := c.Read(ctx)
				cancel()
				if err != nil {
					return
				}
			}
		}(c)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				_ = s.Broadcast(ctx, "tick", map[string]int{"n": i})
			}
		}()
	}
	wg.Wait()
}

type eventWire struct {
	V       int             `json:"v"`
	Type    string          `json:"type"`
	Event   string          `json:"event"`
	ID      string          `json:"id"`
	Origin  string          `json:"origin"`
	Payload json.RawMessage `json:"payload"`
}

func readEvent(t *testing.T, c *websocket.Conn) eventWire {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	typ, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read event: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("type %v", typ)
	}
	var ev eventWire
	if err := json.Unmarshal(data, &ev); err != nil {
		t.Fatalf("unmarshal %s: %v", data, err)
	}
	if ev.Type != "event" || ev.V != 1 || ev.Origin == "" {
		t.Fatalf("%+v", ev)
	}
	return ev
}

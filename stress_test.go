package gorealtime

import (
	"context"
	"net/http/httptest"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestStressFanout(t *testing.T) {
	n := 16
	rounds := 20
	if os.Getenv("GOREALTIME_STRESS") == "1" {
		n = 100
		rounds = 50
	}
	s := NewServer(WithHeartbeat(0, 0), WithQueueSize(256))
	ts := httptest.NewServer(s.Handler())
	base := runtime.NumGoroutine()

	clients := make([]*websocket.Conn, 0, n)
	for i := 0; i < n; i++ {
		c := dial(t, ts.URL, s.Path(), nil)
		_ = readHello(t, c)
		clients = append(clients, c)
		go func(c *websocket.Conn) {
			for {
				if _, _, err := c.Read(context.Background()); err != nil {
					return
				}
			}
		}(c)
	}
	if s.ConnectionCount() != n {
		t.Fatalf("count %d", s.ConnectionCount())
	}
	ctx := context.Background()
	for i := 0; i < rounds; i++ {
		if err := s.Broadcast(ctx, "tick", map[string]int{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	shutdown(t, s)
	for _, c := range clients {
		c.CloseNow()
	}
	ts.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.GC()
		got := runtime.NumGoroutine()
		if got <= base+8 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines %d, baseline %d", got, base)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

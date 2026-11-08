package gorealtime

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

func benchSession() *scriptSession {
	s := newScriptSession(false)
	s.discard = true
	return s
}

func benchServer(b *testing.B) *Server {
	b.Helper()
	s := NewServer(WithHeartbeat(0, 0), WithQueueSize(4096), WithSlowConsumerTimeout(0))
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			b.Error(err)
		}
	})
	return s
}

func BenchmarkRegistryAddRemove(b *testing.B) {
	s := benchServer(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, err := newID()
		if err != nil {
			b.Fatal(err)
		}
		c := newConnection(id, "local", benchSession(), s)
		c.configureQueue(1, 0)
		if !s.reg.tryAdd(c) {
			b.Fatal("add")
		}
		c.finish(ReasonServerClose)
	}
}

func BenchmarkDispatch(b *testing.B) {
	s := benchServer(b)
	s.On("ping", func(ctx context.Context, c *Connection, ev Event) error { return nil })
	c := attachLocal(b, s, benchSession(), 32, 0)
	frame := []byte(`{"v":1,"type":"event","event":"ping","id":"e1","payload":{"n":1}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.handleFrame(c, false, frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSendOne(b *testing.B) {
	s := benchServer(b)
	c := attachLocal(b, s, benchSession(), 4096, 0)
	ctx := context.Background()
	payload := map[string]int{"n": 1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sendRetry(ctx, func() error { return c.Send(ctx, "tick", payload) }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBroadcast32(b *testing.B) {
	s := benchServer(b)
	for i := 0; i < 32; i++ {
		attachLocal(b, s, benchSession(), 4096, 0)
	}
	ctx := context.Background()
	payload := map[string]int{"n": 1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sendRetry(ctx, func() error { return s.Broadcast(ctx, "tick", payload) }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGroupBroadcast32(b *testing.B) {
	s := benchServer(b)
	for i := 0; i < 32; i++ {
		c := attachLocal(b, s, benchSession(), 4096, 0)
		if err := c.Join("room"); err != nil {
			b.Fatal(err)
		}
	}
	ctx := context.Background()
	payload := map[string]int{"n": 1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sendRetry(ctx, func() error { return s.SendToGroup(ctx, "room", "tick", payload) }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParallelBroadcast16(b *testing.B) {
	s := benchServer(b)
	for i := 0; i < 16; i++ {
		attachLocal(b, s, benchSession(), 4096, 0)
	}
	ctx := context.Background()
	payload := map[string]int{"n": 1}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := sendRetry(ctx, func() error { return s.Broadcast(ctx, "tick", payload) }); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// sendRetry waits for the write goroutine when the queue is full.
// The benchmark then measures sustainable send rate, including that wait.
func sendRetry(ctx context.Context, fn func() error) error {
	for {
		err := fn()
		if err == nil || !errors.Is(err, ErrQueueFull) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		runtime.Gosched()
	}
}

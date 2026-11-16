package gorealtime

import (
	"context"
	"net/http"
	"testing"
	"time"
)

type discardResponse struct {
	h http.Header
}

func (d *discardResponse) Header() http.Header {
	if d.h == nil {
		d.h = make(http.Header)
	}
	return d.h
}

func (d *discardResponse) Write(p []byte) (int, error) { return len(p), nil }

func (d *discardResponse) WriteHeader(int) {}

func (d *discardResponse) Flush() {}

func BenchmarkSSEDeliver(b *testing.B) {
	rec := &discardResponse{}
	p := newPipeSession(TransportSSE, 32, time.Second)
	p.attach(rec, rec)
	frame := []byte(`{"v":1,"type":"event","event":"tick","payload":{"n":1}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.Write(context.Background(), frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLongPollDeliver(b *testing.B) {
	p := newPipeSession(TransportLongPolling, 64, time.Second)
	done := make(chan struct{})
	defer func() {
		close(done)
		_ = p.CloseNow()
	}()
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			_, err := p.take(context.Background(), time.Second)
			if err != nil {
				return
			}
		}
	}()
	frame := []byte(`{"v":1,"type":"event","event":"tick","payload":{"n":1}}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.Write(context.Background(), frame); err != nil {
			b.Fatal(err)
		}
	}
}

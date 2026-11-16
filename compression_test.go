package gorealtime

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prashanthyarram/gorealtime/internal/ws"
)

func TestCompressionEventRoundTrip(t *testing.T) {
	s := NewServer(WithCompression(CompressionConfig{Enabled: true, MinSize: 1}))
	got := make(chan []byte, 1)
	s.On("echo", func(ctx context.Context, c *Connection, ev Event) error {
		got <- append([]byte(nil), ev.Payload...)
		return c.Send(ctx, "echo", ev.Payload)
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, resp, err := ws.DialClient(ctx, wsURL(ts.URL, s.Path()), ws.ClientConfig{
		Compression: ws.Compression{Enabled: true, MinSize: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	hello := readHello(t, c)
	conn, ok := s.Connection(hello.Payload.ConnectionID)
	if !ok {
		t.Fatal("missing connection")
	}
	st := conn.Compression()
	if !st.Negotiated || st.ServerContextTakeover || st.ClientContextTakeover {
		t.Fatalf("%+v", st)
	}
	if st.Level != flate.DefaultCompression || st.MinSize != 1 {
		t.Fatalf("%+v", st)
	}
	wantExt := "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
	if st.Extension != wantExt {
		t.Fatalf("extension %q", st.Extension)
	}
	if st.MaxDecompressedBytes == 0 || st.MaxCompressedBytes == 0 {
		t.Fatalf("limits %+v", st)
	}

	payload := strings.Repeat("abc", 400)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"v":1,"type":"event","event":"echo","id":"e1","payload":` + string(raw) + `}`
	if err := c.Write(ctx, ws.MessageText, []byte(body)); err != nil {
		t.Fatal(err)
	}
	select {
	case gotPayload := <-got:
		if !bytes.Equal(gotPayload, raw) {
			t.Fatalf("handler payload %s", gotPayload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called")
	}
	typ, echo, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if typ != ws.MessageText || !bytes.Contains(echo, raw) {
		t.Fatalf("echo %s", echo)
	}
}

func TestDecompressionLimitDropsConnection(t *testing.T) {
	var called atomic.Bool
	s := NewServer(WithCompression(CompressionConfig{
		Enabled:              true,
		MinSize:              1,
		MaxDecompressedBytes: 512,
	}))
	s.On("echo", func(ctx context.Context, c *Connection, ev Event) error {
		called.Store(true)
		return nil
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := ws.DialClient(ctx, wsURL(ts.URL, s.Path()), ws.ClientConfig{
		Compression: ws.Compression{Enabled: true, MinSize: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	if _, _, err := c.Read(ctx); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(strings.Repeat("a", 4000))
	if err != nil {
		t.Fatal(err)
	}
	body := `{"v":1,"type":"event","event":"echo","id":"e1","payload":` + string(raw) + `}`
	_ = c.Write(ctx, ws.MessageText, []byte(body))
	waitCount(t, s, 0)
	if called.Load() {
		t.Fatal("handler saw an over-limit message")
	}
}

func TestCompressionDisabledIgnoresOffer(t *testing.T) {
	s := NewServer()
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)
	c := dial(t, ts.URL, s.Path(), nil)
	defer c.CloseNow()
	if c.Negotiated().Enabled {
		t.Fatal("default server negotiated compression")
	}
	hello := readHello(t, c)
	conn, ok := s.Connection(hello.Payload.ConnectionID)
	if !ok {
		t.Fatal("missing connection")
	}
	if conn.Compression().Negotiated || conn.Compression().Level != 0 {
		t.Fatalf("%+v", conn.Compression())
	}
}

func TestCompressionConfigValidation(t *testing.T) {
	NewServer(WithCompression(CompressionConfig{Enabled: true}))
	NewServer(WithCompression(CompressionConfig{Enabled: true, Level: -2}))
	NewServer(WithCompression(CompressionConfig{Enabled: true, Level: -1}))
	NewServer(WithCompression(CompressionConfig{Enabled: true, Level: 9}))
	NewServer(WithCompression(CompressionConfig{Enabled: false, Level: 99, MinSize: -5}))
	mustPanic(t, func() {
		NewServer(WithCompression(CompressionConfig{Enabled: true, Level: 10}))
	})
	mustPanic(t, func() {
		NewServer(WithCompression(CompressionConfig{Enabled: true, Level: -3}))
	})
	mustPanic(t, func() {
		NewServer(WithCompression(CompressionConfig{Enabled: true, MinSize: -1}))
	})
	mustPanic(t, func() {
		NewServer(WithCompression(CompressionConfig{Enabled: true, MaxDecompressedBytes: -1}))
	})
	mustPanic(t, func() {
		NewServer(WithCompression(CompressionConfig{Enabled: true, MaxCompressedBytes: 9 << 20}))
	})
}

func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}

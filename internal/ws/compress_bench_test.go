package ws

import (
	"bytes"
	"compress/flate"
	"context"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func BenchmarkCompress(b *testing.B) {
	text := bytes.Repeat([]byte("hello world "), 4096)
	random := make([]byte, 8<<10)
	rnd := rand.New(rand.NewSource(1))
	if _, err := rnd.Read(random); err != nil {
		b.Fatal(err)
	}
	cases := []struct {
		name    string
		payload []byte
		context bool
		min     int
	}{
		{name: "small-skip", payload: []byte("ping"), context: false, min: 128},
		{name: "small-force", payload: bytes.Repeat([]byte("a"), 16), context: false, min: 1},
		{name: "text", payload: text, context: false, min: 1},
		{name: "text-context", payload: text, context: true, min: 1},
		{name: "random", payload: random, context: false, min: 1},
		{name: "random-context", payload: random, context: true, min: 1},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			p := benchPMCE(b, tc.context, tc.min)
			if _, _, err := p.compress(tc.payload); err != nil {
				b.Fatal(err)
			}
			out, _, err := p.compress(tc.payload)
			if err != nil {
				b.Fatal(err)
			}
			wire := len(out)
			if wire == 0 {
				wire = 1
			}
			b.SetBytes(int64(len(tc.payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := p.compress(tc.payload); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(out)), "wire-B")
			b.ReportMetric(float64(len(tc.payload))/float64(wire), "ratio")
		})
	}
}

func BenchmarkDecompress(b *testing.B) {
	text := bytes.Repeat([]byte("hello world "), 4096)
	random := make([]byte, 8<<10)
	rnd := rand.New(rand.NewSource(1))
	if _, err := rnd.Read(random); err != nil {
		b.Fatal(err)
	}
	cases := []struct {
		name    string
		payload []byte
		context bool
	}{
		{name: "text", payload: text, context: false},
		{name: "text-context", payload: text, context: true},
		{name: "random", payload: random, context: false},
		{name: "small", payload: bytes.Repeat([]byte("a"), 16), context: false},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			src := benchPMCE(b, tc.context, 1)
			dst := benchPMCE(b, tc.context, 1)
			comp, ok, err := src.compress(tc.payload)
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				// Poorly compressible input is sent uncompressed on the wire.
				// Force a raw DEFLATE block here so inflation of that input
				// still has a cost to measure.
				comp = forceDeflate(tc.payload)
			} else {
				comp = append([]byte(nil), comp...)
			}
			if _, err := dst.decompress(context.Background(), comp); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(tc.payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := dst.decompress(context.Background(), comp); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(comp)), "wire-B")
			b.ReportMetric(float64(len(tc.payload))/float64(max(len(comp), 1)), "ratio")
		})
	}
}

func BenchmarkEcho(b *testing.B) {
	text := bytes.Repeat([]byte("hello world "), 256)
	random := make([]byte, 4<<10)
	if _, err := rand.New(rand.NewSource(1)).Read(random); err != nil {
		b.Fatal(err)
	}
	cases := []struct {
		name    string
		payload []byte
		on      bool
		context bool
	}{
		{name: "off-text", payload: text, on: false},
		{name: "on-text", payload: text, on: true},
		{name: "on-text-context", payload: text, on: true, context: true},
		{name: "off-random", payload: random, on: false},
		{name: "on-random", payload: random, on: true},
		{name: "on-small", payload: []byte("ping"), on: true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			cfg := Compression{Enabled: tc.on, MinSize: 1, ServerContextTakeover: tc.context, ClientContextTakeover: tc.context}
			s, c := benchEcho(b, cfg)
			defer s.Close()
			defer c.CloseNow()
			ctx := context.Background()
			b.SetBytes(int64(len(tc.payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := c.Write(ctx, MessageText, tc.payload); err != nil {
					b.Fatal(err)
				}
				if _, _, err := c.Read(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEchoParallel(b *testing.B) {
	text := bytes.Repeat([]byte("hello world "), 64)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := Upgrade(w, r, Config{ReadLimit: 1 << 20, Compression: Compression{
			Enabled: true, MinSize: 1, MaxDecompressed: 1 << 20, MaxCompressed: 1 << 20,
		}})
		if err != nil {
			return
		}
		defer sess.CloseNow()
		for {
			binary, data, err := sess.Read(context.Background())
			if err != nil {
				return
			}
			msg := append([]byte(nil), data...)
			if err := sess.w.writeMessage(context.Background(), binary, msg); err != nil {
				return
			}
		}
	}))
	defer s.Close()
	url := "ws" + strings.TrimPrefix(s.URL, "http") + "/"
	b.SetBytes(int64(len(text)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c, _, err := DialClient(context.Background(), url, ClientConfig{Compression: Compression{
			Enabled: true, MinSize: 1, MaxDecompressed: 1 << 20, MaxCompressed: 1 << 20,
		}, ReadLimit: 1 << 20})
		if err != nil {
			b.Errorf("dial: %v", err)
			return
		}
		defer c.CloseNow()
		for pb.Next() {
			if err := c.Write(context.Background(), MessageText, text); err != nil {
				b.Errorf("write: %v", err)
				return
			}
			if _, _, err := c.Read(context.Background()); err != nil {
				b.Errorf("read: %v", err)
				return
			}
		}
	})
}

func benchPMCE(b *testing.B, contextTakeover bool, min int) *pmce {
	b.Helper()
	cfg := Compression{
		Enabled:               true,
		Level:                 6,
		MinSize:               min,
		MaxDecompressed:       8 << 20,
		MaxCompressed:         8 << 20,
		ServerContextTakeover: contextTakeover,
		ClientContextTakeover: contextTakeover,
	}
	_, p, err := negotiate([]string{"permessage-deflate"}, cfg)
	if err != nil || p == nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		p.releaseIn()
		p.releaseOut()
	})
	return p
}

func benchEcho(b *testing.B, comp Compression) (*httptest.Server, *Client) {
	b.Helper()
	if comp.Enabled {
		if comp.MaxDecompressed == 0 {
			comp.MaxDecompressed = 1 << 20
		}
		if comp.MaxCompressed == 0 {
			comp.MaxCompressed = 1 << 20
		}
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := Upgrade(w, r, Config{ReadLimit: 1 << 20, Compression: comp})
		if err != nil {
			return
		}
		defer sess.CloseNow()
		for {
			binary, data, err := sess.Read(context.Background())
			if err != nil {
				return
			}
			msg := append([]byte(nil), data...)
			if err := sess.w.writeMessage(context.Background(), binary, msg); err != nil {
				return
			}
		}
	}))
	c, _, err := DialClient(context.Background(), "ws"+strings.TrimPrefix(s.URL, "http")+"/", ClientConfig{
		Compression: comp,
		ReadLimit:   1 << 20,
	})
	if err != nil {
		s.Close()
		b.Fatal(err)
	}
	return s, c
}

func forceDeflate(src []byte) []byte {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, 6)
	if err != nil {
		return append([]byte(nil), src...)
	}
	_, _ = w.Write(src)
	_ = w.Flush()
	raw := buf.Bytes()
	if len(raw) >= 4 {
		raw = raw[:len(raw)-4]
	}
	return append([]byte(nil), raw...)
}

package ws

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{0x81, 0x81, 1, 2, 3, 4, 'a'})
	f.Add([]byte{0x89, 0x80, 9, 9, 9, 9})
	f.Add([]byte{0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()
		w := &wire{nc: c1, br: bytes.NewReader(data), server: true, readLimit: 4096}
		_, _, _, _, _ = w.readFrame()
	})
}

func FuzzHandshake(f *testing.F) {
	f.Add("upgrade", "websocket", "13", "dGhlIHNhbXBsZSBub25jZQ==")
	f.Add("", "", "", "")
	f.Fuzz(func(t *testing.T, connection, upgrade, version, key string) {
		req, err := http.NewRequest(http.MethodGet, "http://example/realtime", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Connection", connection)
		req.Header.Set("Upgrade", upgrade)
		req.Header.Set("Sec-WebSocket-Version", version)
		req.Header.Set("Sec-WebSocket-Key", key)
		_ = checkHandshake(req)
	})
}

func FuzzDecompress(f *testing.F) {
	f.Add([]byte{0x00})
	f.Add([]byte{0x78, 0x9c})
	f.Fuzz(func(t *testing.T, src []byte) {
		if len(src) > 4096 {
			src = src[:4096]
		}
		p, err := newPMCE(agreement{}, Compression{Enabled: true, MaxDecompressed: 8192, MaxCompressed: 4096}, roleServer)
		if err != nil {
			t.Fatal(err)
		}
		defer p.releaseIn()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, _ = p.decompress(ctx, src)
	})
}

package ws

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNegotiateAcceptsSupportedOffers(t *testing.T) {
	cfg := Compression{Enabled: true}
	cases := []struct {
		offer string
		want  string
	}{
		{
			offer: "permessage-deflate",
			want:  "permessage-deflate; server_no_context_takeover; client_no_context_takeover",
		},
		{
			offer: "Permessage-Deflate; Client_Max_Window_Bits",
			want:  "permessage-deflate; server_no_context_takeover; client_no_context_takeover",
		},
		{
			offer: `permessage-deflate; server_max_window_bits="15"`,
			want:  "permessage-deflate; server_no_context_takeover; client_no_context_takeover; server_max_window_bits=15",
		},
		{
			offer: "permessage-deflate; server_max_window_bits=8, permessage-deflate; client_max_window_bits",
			want:  "permessage-deflate; server_no_context_takeover; client_no_context_takeover",
		},
		{
			offer: "mux, permessage-deflate; client_no_context_takeover",
			want:  "permessage-deflate; server_no_context_takeover; client_no_context_takeover",
		},
	}
	for _, tc := range cases {
		ext, p, err := negotiate([]string{tc.offer}, cfg)
		if err != nil {
			t.Fatalf("%s: %v", tc.offer, err)
		}
		if ext != tc.want || p == nil || !p.neg.Enabled {
			t.Fatalf("%s: got %q enabled %v", tc.offer, ext, p)
		}
		if p.neg.ServerContextTakeover || p.neg.ClientContextTakeover {
			t.Fatalf("%s kept context: %+v", tc.offer, p.neg)
		}
		p.releaseIn()
		p.releaseOut()
	}
}

func TestNegotiateDeclinesUnsupportedOffers(t *testing.T) {
	cfg := Compression{Enabled: true, ServerContextTakeover: true, ClientContextTakeover: true}
	cases := []string{
		"permessage-deflate; server_max_window_bits=10",
		"permessage-deflate; server_max_window_bits=08",
		"permessage-deflate; server_max_window_bits",
		"permessage-deflate; client_max_window_bits=7",
		"permessage-deflate; server_no_context_takeover=1",
		"permessage-deflate; unknown",
		"permessage-deflate; client_no_context_takeover; client_no_context_takeover",
		"not-an-extension",
		"permessage-deflate;",
	}
	for _, offer := range cases {
		ext, p, err := negotiate([]string{offer}, cfg)
		if err != nil {
			t.Fatalf("%s: %v", offer, err)
		}
		if ext != "" || p != nil {
			t.Fatalf("%s: negotiated %q", offer, ext)
		}
	}
}

func TestNegotiateContextTakeover(t *testing.T) {
	cfg := Compression{Enabled: true, ServerContextTakeover: true, ClientContextTakeover: true}
	ext, p, err := negotiate([]string{"permessage-deflate"}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.releaseIn()
	defer p.releaseOut()
	if ext != "permessage-deflate" || !p.neg.ServerContextTakeover || !p.neg.ClientContextTakeover {
		t.Fatalf("ext %q neg %+v", ext, p.neg)
	}

	// The client's server_no_context_takeover forbids the server window
	// even when the application asked to keep it. The client hint for its
	// own window does not turn the server's inflater off.
	ext, p, err = negotiate([]string{"permessage-deflate; server_no_context_takeover; client_no_context_takeover"}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.releaseIn()
	defer p.releaseOut()
	if p.neg.ServerContextTakeover || !p.neg.ClientContextTakeover {
		t.Fatalf("hint negotiation %+v ext %q", p.neg, ext)
	}
	if !strings.Contains(ext, "server_no_context_takeover") || strings.Contains(ext, "client_no_context_takeover") {
		t.Fatalf("response %q", ext)
	}
}

func TestDisabledCompressionDoesNotNegotiate(t *testing.T) {
	ext, p, err := negotiate([]string{"permessage-deflate"}, Compression{})
	if err != nil || ext != "" || p != nil {
		t.Fatalf("ext %q p %v err %v", ext, p, err)
	}
}

func TestRFC7692Vectors(t *testing.T) {
	p := mustPMCE(t, false, false)
	vectors := []struct {
		name    string
		payload []byte
		dict    []byte
		want    string
	}{
		{"hello", []byte{0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00}, nil, "Hello"},
		{"bfinal", []byte{0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x00}, nil, "Hello"},
		{"stored", []byte{0x00, 0x05, 0x00, 0xfa, 0xff, 'H', 'e', 'l', 'l', 'o', 0x00}, nil, "Hello"},
		{"two-blocks", []byte{0xf2, 0x48, 0x05, 0x00, 0x00, 0x00, 0xff, 0xff, 0xca, 0xc9, 0xc9, 0x07, 0x00}, nil, "Hello"},
	}
	for _, tc := range vectors {
		got, err := p.decompress(context.Background(), tc.payload)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if string(got) != tc.want {
			t.Fatalf("%s: got %q", tc.name, got)
		}
	}
	p.inContext = true
	p.window = []byte("Hello")
	got, err := p.decompress(context.Background(), []byte{0xf2, 0x00, 0x11, 0x00, 0x00})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "Hello" {
		t.Fatalf("context vector %q", got)
	}
}

func TestDecompressRejectsCorruptAndOversized(t *testing.T) {
	p := mustPMCE(t, false, false)
	if _, err := p.decompress(context.Background(), []byte{0xff, 0x01}); !errors.Is(err, errProtocol) {
		t.Fatalf("corrupt: %v", err)
	}
	if p.window != nil {
		t.Fatal("corrupt updated the window")
	}
	p.maxOut = 4
	if _, err := p.decompress(context.Background(), []byte{0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00}); !errors.Is(err, errTooBig) {
		t.Fatalf("limit: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.decompress(ctx, []byte{0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
}

func TestCompressContextAndBypass(t *testing.T) {
	p := mustPMCE(t, true, true)
	p.minSize = 32
	msg := bytes.Repeat([]byte("abc"), 40)
	first, compressed, err := p.compress(msg)
	if err != nil || !compressed {
		t.Fatalf("first compressed %v err %v", compressed, err)
	}
	if !bytes.Equal(mustInflate(t, first, nil), msg) {
		t.Fatal("independent inflate mismatch")
	}
	skipped, compressed, err := p.compress([]byte("hi"))
	if err != nil || compressed || string(skipped) != "hi" {
		t.Fatalf("skip compressed %v %q err %v", compressed, skipped, err)
	}
	second, compressed, err := p.compress(msg)
	if err != nil || !compressed {
		t.Fatalf("second compressed %v err %v", compressed, err)
	}
	if got, err := tryInflate(second, nil); err == nil && bytes.Equal(got, msg) {
		t.Fatal("second message did not depend on the compression window")
	}
	if !bytes.Equal(mustInflate(t, second, msg), msg) {
		t.Fatal("second message did not inflate with the first plaintext as history")
	}

	plain := mustPMCE(t, false, false)
	plain.minSize = 1
	a, _, err := plain.compress(msg)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := plain.compress(msg)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("no-context encodings differed")
	}
	rnd := make([]byte, 180)
	if _, err := rand.Read(rnd); err != nil {
		t.Fatal(err)
	}
	out, compressed, err := plain.compress(rnd)
	if err != nil {
		t.Fatal(err)
	}
	if compressed || !bytes.Equal(out, rnd) {
		t.Fatal("incompressible payload was not left unchanged")
	}
}

func TestFragmentedCompressedRead(t *testing.T) {
	sess, client := dialPair(t, Config{Compression: Compression{Enabled: true, MinSize: 1}})
	defer sess.CloseNow()
	defer client.CloseNow()
	payload := []byte{0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00}
	ctx := context.Background()
	if err := client.w.writeFrameFlags(ctx, false, opText, true, payload[:3]); err != nil {
		t.Fatal(err)
	}
	if err := client.w.writeFrameFlags(ctx, true, opPing, false, []byte("p")); err != nil {
		t.Fatal(err)
	}
	if err := client.w.writeFrameFlags(ctx, true, opCont, false, payload[3:]); err != nil {
		t.Fatal(err)
	}
	binary, data, err := sess.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if binary || string(data) != "Hello" {
		t.Fatalf("got binary %v %q", binary, data)
	}

	if err := client.w.writeFrameFlags(ctx, true, opBinary, true, payload); err != nil {
		t.Fatal(err)
	}
	binary, data, err = sess.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !binary || string(data) != "Hello" {
		t.Fatalf("binary got %v %q", binary, data)
	}
}

func TestRejectsRSVMisuse(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		sess, client := dialPair(t, Config{})
		defer sess.CloseNow()
		defer client.CloseNow()
		if err := client.w.writeFrameFlags(context.Background(), true, opText, true, []byte{0x00}); err != nil {
			t.Fatal(err)
		}
		_, _, err := sess.Read(context.Background())
		if !errors.Is(err, errProtocol) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("rsv2", func(t *testing.T) {
		sess, client := dialPair(t, Config{Compression: Compression{Enabled: true}})
		defer sess.CloseNow()
		defer client.CloseNow()
		// RSV2 is bit 0x20. writeFrameFlags only sets RSV1, so write the byte directly.
		if err := writeRaw(client, []byte{0x80 | 0x20 | opText, 0x81, 1, 2, 3, 4, 'x' ^ 1}); err != nil {
			t.Fatal(err)
		}
		_, _, err := sess.Read(context.Background())
		if !errors.Is(err, errProtocol) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("continuation-rsv1", func(t *testing.T) {
		sess, client := dialPair(t, Config{Compression: Compression{Enabled: true}})
		defer sess.CloseNow()
		defer client.CloseNow()
		if err := client.w.writeFrameFlags(context.Background(), false, opText, true, []byte{0xf2}); err != nil {
			t.Fatal(err)
		}
		if err := client.w.writeFrameFlags(context.Background(), true, opCont, true, []byte{0x00}); err != nil {
			t.Fatal(err)
		}
		_, _, err := sess.Read(context.Background())
		if !errors.Is(err, errProtocol) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestCompressedRoundTrip(t *testing.T) {
	cfg := Config{
		ReadLimit: 1 << 20,
		Compression: Compression{
			Enabled:         true,
			MinSize:         1,
			MaxDecompressed: 1 << 20,
			MaxCompressed:   1 << 20,
		},
	}
	sess, client := dialPair(t, cfg)
	defer sess.CloseNow()
	defer client.CloseNow()
	if !client.Negotiated().Enabled || !sess.Negotiated().Enabled {
		t.Fatal("expected negotiation")
	}
	if client.Negotiated().Extension != sess.Negotiated().Extension {
		t.Fatalf("extensions %q %q", client.Negotiated().Extension, sess.Negotiated().Extension)
	}
	ctx := context.Background()
	text := bytes.Repeat([]byte("hello "), 500)
	if err := client.Write(ctx, MessageText, text); err != nil {
		t.Fatal(err)
	}
	binary, data, err := sess.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if binary || !bytes.Equal(data, text) {
		t.Fatalf("text mismatch %d", len(data))
	}
	if err := sess.Write(ctx, text); err != nil {
		t.Fatal(err)
	}
	typ, got, err := client.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if typ != MessageText || !bytes.Equal(got, text) {
		t.Fatal("echo mismatch")
	}
	if err := client.Write(ctx, MessageText, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	// MinSize on the client is 128, so this stays uncompressed.
	_, data, err = sess.Read(ctx)
	if err != nil || string(data) != "hi" {
		t.Fatalf("small %q %v", data, err)
	}
	pctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	go func() { _ = sess.Ping(pctx) }()
	if err := client.Write(ctx, MessageBinary, text); err != nil {
		t.Fatal(err)
	}
	binary, data, err = sess.Read(ctx)
	if err != nil || !binary || !bytes.Equal(data, text) {
		t.Fatalf("binary ping overlap %v %v", binary, err)
	}
}

func TestContextTakeoverRoundTrip(t *testing.T) {
	cfg := Config{
		ReadLimit: 1 << 20,
		Compression: Compression{
			Enabled:               true,
			ServerContextTakeover: true,
			ClientContextTakeover: true,
			MinSize:               1,
			MaxDecompressed:       1 << 20,
			MaxCompressed:         1 << 20,
		},
	}
	clientCfg := Compression{
		Enabled:               true,
		ServerContextTakeover: true,
		ClientContextTakeover: true,
		MinSize:               1,
		MaxDecompressed:       1 << 20,
		MaxCompressed:         1 << 20,
	}
	sess, client := dialPairConfig(t, cfg, clientCfg)
	defer sess.CloseNow()
	defer client.CloseNow()
	if !sess.Negotiated().ServerContextTakeover || !sess.Negotiated().ClientContextTakeover {
		t.Fatalf("%+v", sess.Negotiated())
	}
	msg := bytes.Repeat([]byte("xyz "), 300)
	ctx := context.Background()
	if err := client.Write(ctx, MessageText, msg); err != nil {
		t.Fatal(err)
	}
	if err := client.Write(ctx, MessageText, msg); err != nil {
		t.Fatal(err)
	}
	if err := client.w.writeFrameFlags(ctx, true, opText, false, []byte("plain")); err != nil {
		t.Fatal(err)
	}
	if err := client.Write(ctx, MessageText, msg); err != nil {
		t.Fatal(err)
	}
	want := [][]byte{msg, msg, []byte("plain"), msg}
	for i, exp := range want {
		_, data, err := sess.Read(ctx)
		if err != nil || !bytes.Equal(data, exp) {
			t.Fatalf("msg %d %v got %q", i, err, data)
		}
	}
}

func TestDecompressionLimitClosesRead(t *testing.T) {
	cfg := Config{
		ReadLimit: 1 << 20,
		Compression: Compression{
			Enabled:         true,
			MinSize:         1,
			MaxDecompressed: 64,
			MaxCompressed:   1 << 20,
		},
	}
	errCh := make(chan error, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := Upgrade(w, r, cfg)
		if err != nil {
			errCh <- err
			return
		}
		defer sess.CloseNow()
		_, _, err = sess.Read(context.Background())
		errCh <- err
	}))
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, err := DialClient(ctx, wsPath(s), ClientConfig{Compression: Compression{Enabled: true, MinSize: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	if err := c.Write(ctx, MessageText, bytes.Repeat([]byte("a"), 8000)); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; !errors.Is(err, errTooBig) {
		t.Fatalf("got %v", err)
	}
}

func TestCompressedSizeLimit(t *testing.T) {
	cfg := Config{
		ReadLimit: 1 << 20,
		Compression: Compression{
			Enabled:       true,
			MaxCompressed: 8,
		},
	}
	errCh := make(chan error, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := Upgrade(w, r, cfg)
		if err != nil {
			errCh <- err
			return
		}
		defer sess.CloseNow()
		_, _, err = sess.Read(context.Background())
		errCh <- err
	}))
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, err := DialClient(ctx, wsPath(s), ClientConfig{Compression: Compression{Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	if err := c.w.writeFrameFlags(ctx, true, opText, true, bytes.Repeat([]byte{0x01}, 32)); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; !errors.Is(err, errTooBig) {
		t.Fatalf("got %v", err)
	}
}

func TestConcurrentWritesAndConnections(t *testing.T) {
	const n = 6
	cfg := Config{
		ReadLimit:   1 << 20,
		Compression: Compression{Enabled: true, MinSize: 1, MaxDecompressed: 1 << 20, MaxCompressed: 1 << 20},
	}
	type delivery struct {
		id  int
		msg string
	}
	got := make(chan delivery, n*4)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := Upgrade(w, r, cfg)
		if err != nil {
			return
		}
		defer sess.CloseNow()
		for {
			_, data, err := sess.Read(context.Background())
			if err != nil {
				return
			}
			parts := strings.SplitN(string(data), ":", 2)
			if len(parts) != 2 {
				return
			}
			id := 0
			for _, c := range parts[0] {
				id = id*10 + int(c-'0')
			}
			got <- delivery{id: id, msg: parts[1]}
		}
	}))
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var clients []*Client
	for i := 0; i < n; i++ {
		c, _, err := DialClient(ctx, wsPath(s), ClientConfig{Compression: Compression{Enabled: true, MinSize: 1}})
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, c)
		defer c.CloseNow()
	}
	var wg sync.WaitGroup
	msg := strings.Repeat("q", 400)
	for i, c := range clients {
		wg.Add(1)
		go func(id int, c *Client) {
			defer wg.Done()
			body := []byte(itoa(id) + ":" + msg)
			var inner sync.WaitGroup
			for k := 0; k < 4; k++ {
				inner.Add(1)
				go func() {
					defer inner.Done()
					if err := c.Write(ctx, MessageText, body); err != nil {
						t.Errorf("write: %v", err)
					}
				}()
			}
			inner.Wait()
		}(i, c)
	}
	wg.Wait()
	seen := make([]int, n)
	deadline := time.After(3 * time.Second)
	for sum(seen) < n*4 {
		select {
		case d := <-got:
			if d.msg != msg {
				t.Fatalf("payload %q", d.msg)
			}
			seen[d.id]++
		case <-deadline:
			t.Fatalf("seen %v", seen)
		}
	}
}

func TestCloseReleasesCompression(t *testing.T) {
	hold := make(chan struct{})
	got := make(chan *Session, 1)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := Upgrade(w, r, Config{Compression: Compression{Enabled: true}})
		if err != nil {
			return
		}
		got <- sess
		<-hold
		_ = sess.CloseNow()
	}))
	defer s.Close()
	defer close(hold)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, err := DialClient(ctx, wsPath(s), ClientConfig{Compression: Compression{Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	sess := <-got
	_ = sess.CloseNow()
	_ = c.CloseNow()
	if sess.w.pmce == nil || sess.w.pmce.zw != nil || sess.w.pmce.zr != nil {
		t.Fatal("server compression was not released")
	}
	if c.w.pmce == nil || c.w.pmce.zw != nil || c.w.pmce.zr != nil {
		t.Fatal("client compression was not released")
	}
}

func TestCompressionIsOptional(t *testing.T) {
	t.Run("server off", func(t *testing.T) {
		sess, client := dialPairConfig(t, Config{}, Compression{Enabled: true, MinSize: 1})
		defer sess.CloseNow()
		defer client.CloseNow()
		if sess.Negotiated().Enabled || client.Negotiated().Enabled {
			t.Fatal("server disabled compression was negotiated")
		}
		if err := client.Write(context.Background(), MessageText, bytes.Repeat([]byte("a"), 300)); err != nil {
			t.Fatal(err)
		}
		_, data, err := sess.Read(context.Background())
		if err != nil || !bytes.Equal(data, bytes.Repeat([]byte("a"), 300)) {
			t.Fatalf("%q %v", data, err)
		}
	})
	t.Run("client off", func(t *testing.T) {
		sess, client := dialPairConfig(t, Config{Compression: Compression{Enabled: true}}, Compression{})
		defer sess.CloseNow()
		defer client.CloseNow()
		if sess.Negotiated().Enabled || client.Negotiated().Enabled {
			t.Fatal("unoffered compression was negotiated")
		}
	})
}

func TestClientRejectsSmallWindowResponse(t *testing.T) {
	_, err := clientPMCE("permessage-deflate; client_max_window_bits=8", Compression{Enabled: true})
	if err == nil {
		t.Fatal("expected rejection")
	}
	p, err := clientPMCE("permessage-deflate; server_no_context_takeover; client_no_context_takeover; client_max_window_bits=15", Compression{Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	p.releaseIn()
	p.releaseOut()
}

func mustPMCE(t *testing.T, serverCtx, clientCtx bool) *pmce {
	t.Helper()
	cfg := Compression{
		Enabled:               true,
		Level:                 flate.DefaultCompression,
		MinSize:               1,
		MaxDecompressed:       1 << 20,
		MaxCompressed:         1 << 20,
		ServerContextTakeover: serverCtx,
		ClientContextTakeover: clientCtx,
	}
	_, p, err := negotiate([]string{"permessage-deflate"}, cfg)
	if err != nil || p == nil {
		t.Fatalf("negotiate: %v", err)
	}
	t.Cleanup(func() {
		p.releaseIn()
		p.releaseOut()
	})
	return p
}

func mustInflate(t *testing.T, payload, dict []byte) []byte {
	t.Helper()
	got, err := tryInflate(payload, dict)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func tryInflate(payload, dict []byte) ([]byte, error) {
	full := append(append([]byte{}, payload...), 0x00, 0x00, 0xff, 0xff)
	r := flate.NewReaderDict(bytes.NewReader(full), dict)
	defer r.Close()
	var out bytes.Buffer
	buf := make([]byte, 128)
	for {
		n, err := r.Read(buf)
		out.Write(buf[:n])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return out.Bytes(), nil
		}
		if err != nil {
			return out.Bytes(), err
		}
	}
}

func dialPair(t *testing.T, cfg Config) (*Session, *Client) {
	t.Helper()
	return dialPairConfig(t, cfg, cfg.Compression)
}

func dialPairConfig(t *testing.T, cfg Config, client Compression) (*Session, *Client) {
	t.Helper()
	got := make(chan *Session, 1)
	hold := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := Upgrade(w, r, cfg)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		got <- sess
		<-hold
		_ = sess.CloseNow()
	}))
	t.Cleanup(func() {
		close(hold)
		s.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	c, _, err := DialClient(ctx, wsPath(s), ClientConfig{Compression: client, ReadLimit: cfg.ReadLimit})
	if err != nil {
		t.Fatal(err)
	}
	return <-got, c
}

func wsPath(s *httptest.Server) string {
	return "ws" + strings.TrimPrefix(s.URL, "http") + "/"
}

func writeRaw(c *Client, payload []byte) error {
	_, err := c.w.nc.Write(payload)
	return err
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func sum(v []int) int {
	n := 0
	for _, x := range v {
		n += x
	}
	return n
}

package gorealtime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNegotiateAndShouldFallback(t *testing.T) {
	s := NewServer(WithPath("/realtime"), WithCompression(CompressionConfig{Enabled: true}))
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	defer shutdown(t, s)

	res, err := http.Post(ts.URL+"/realtime/negotiate", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}
	var neg Negotiation
	if err := json.NewDecoder(res.Body).Decode(&neg); err != nil {
		t.Fatal(err)
	}
	if neg.ProtocolVersion != 1 || neg.ConnectionID == "" || len(neg.ConnectionToken) < 32 {
		t.Fatalf("negotiation %+v", neg)
	}
	if time.Until(neg.ExpiresAt) <= 0 {
		t.Fatalf("expiry %s", neg.ExpiresAt)
	}
	if len(neg.AvailableTransports) != 3 {
		t.Fatalf("transports %+v", neg.AvailableTransports)
	}
	if neg.AvailableTransports[0].Name != TransportWebSocket || neg.AvailableTransports[1].Name != TransportSSE || neg.AvailableTransports[2].Name != TransportLongPolling {
		t.Fatalf("order %+v", neg.AvailableTransports)
	}
	if len(neg.Capabilities) != 1 || neg.Capabilities[0] != "permessage-deflate" {
		t.Fatalf("capabilities %+v", neg.Capabilities)
	}
	if ShouldFallback(http.StatusUnauthorized) || ShouldFallback(http.StatusForbidden) || ShouldFallback(http.StatusConflict) || ShouldFallback(http.StatusServiceUnavailable) {
		t.Fatal("policy statuses must not fall back")
	}
	if !ShouldFallback(0) || !ShouldFallback(http.StatusNotFound) {
		t.Fatal("transport failures should fall back")
	}

	bad, err := http.Post(ts.URL+"/realtime/negotiate", "application/json", strings.NewReader(`{`))
	if err != nil {
		t.Fatal(err)
	}
	bad.Body.Close()
	if bad.StatusCode != http.StatusBadRequest || ShouldFallback(bad.StatusCode) {
		t.Fatalf("malformed %d", bad.StatusCode)
	}
}

func TestAuthFailureDoesNotOpenTransport(t *testing.T) {
	s := NewServer(WithRequireAuth())
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	res, err := http.Post(ts.URL+"/negotiate", "application/json", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d", res.StatusCode)
	}
	if ShouldFallback(res.StatusCode) {
		t.Fatal("auth failure must not fall back")
	}
	if s.ConnectionCount() != 0 {
		t.Fatalf("count %d", s.ConnectionCount())
	}
}

func TestSSERoundTripAndReuse(t *testing.T) {
	s := NewServer()
	got := make(chan []byte, 1)
	s.On("echo", func(ctx context.Context, conn *Connection, ev Event) error {
		if conn.Transport() != TransportSSE {
			t.Errorf("transport %s", conn.Transport())
		}
		return conn.Send(ctx, "echo", ev.Payload)
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	neg := negotiate(t, ts.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp := openSSE(t, ctx, ts.URL+"/sse", neg.ConnectionToken)
	defer resp.Body.Close()
	stream := bufio.NewReader(resp.Body)
	hello := readSSE(t, stream)
	if !strings.Contains(hello, neg.ConnectionID) {
		t.Fatalf("hello %s", hello)
	}
	waitCount(t, s, 1)

	againReq, err := http.NewRequest(http.MethodGet, ts.URL+"/sse", nil)
	if err != nil {
		t.Fatal(err)
	}
	againReq.Header.Set(connectionTokenHeader, neg.ConnectionToken)
	again, err := http.DefaultClient.Do(againReq)
	if err != nil {
		t.Fatal(err)
	}
	again.Body.Close()
	if again.StatusCode != http.StatusConflict || ShouldFallback(again.StatusCode) {
		t.Fatalf("reuse %d", again.StatusCode)
	}

	postEvent(t, ts.URL+"/send", neg.ConnectionToken, `{"v":1,"type":"event","event":"echo","payload":{"n":1}}`)
	echo := readSSE(t, stream)
	if !strings.Contains(echo, `"echo"`) {
		t.Fatalf("echo %s", echo)
	}
	select {
	case got <- []byte(echo):
	default:
	}
	_ = got

	cancel()
	waitCount(t, s, 0)
}

func TestLongPollRoundTrip(t *testing.T) {
	s := NewServer(WithPollTimeout(time.Second))
	s.On("echo", func(ctx context.Context, conn *Connection, ev Event) error {
		return conn.Send(ctx, "echo", ev.Payload)
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	neg := negotiate(t, ts.URL)
	body := poll(t, ts.URL+"/poll", neg.ConnectionToken)
	if !strings.Contains(body, neg.ConnectionID) {
		t.Fatalf("poll %s", body)
	}
	if _, ok := s.Connection(neg.ConnectionID); !ok {
		t.Fatal("connection was not registered")
	}
	postEvent(t, ts.URL+"/send", neg.ConnectionToken, `{"v":1,"type":"event","event":"echo","payload":{"ok":true}}`)
	echo := poll(t, ts.URL+"/poll", neg.ConnectionToken)
	if !strings.Contains(echo, `"echo"`) || !strings.Contains(echo, "true") {
		t.Fatalf("echo %s", echo)
	}

	emptyReq, err := http.NewRequest(http.MethodGet, ts.URL+"/poll", nil)
	if err != nil {
		t.Fatal(err)
	}
	emptyReq.Header.Set(connectionTokenHeader, neg.ConnectionToken)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	emptyReq = emptyReq.WithContext(ctx)
	res, err := http.DefaultClient.Do(emptyReq)
	if err != nil && ctx.Err() == nil {
		t.Fatal(err)
	}
	if res != nil {
		res.Body.Close()
	}

	second, err := http.NewRequest(http.MethodGet, ts.URL+"/poll", nil)
	if err != nil {
		t.Fatal(err)
	}
	second.Header.Set(connectionTokenHeader, neg.ConnectionToken)
	started := make(chan struct{})
	errc := make(chan int, 1)
	go func() {
		close(started)
		res, err := http.DefaultClient.Do(second)
		if err != nil {
			errc <- 0
			return
		}
		res.Body.Close()
		errc <- res.StatusCode
	}()
	<-started
	time.Sleep(30 * time.Millisecond)
	held, err := http.NewRequest(http.MethodGet, ts.URL+"/poll", nil)
	if err != nil {
		t.Fatal(err)
	}
	held.Header.Set(connectionTokenHeader, neg.ConnectionToken)
	conflict, err := http.DefaultClient.Do(held)
	if err != nil {
		t.Fatal(err)
	}
	conflict.Body.Close()
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("concurrent poll %d", conflict.StatusCode)
	}
	_ = <-errc
}

func TestDisabledTransportIsNotAPolicyError(t *testing.T) {
	s := NewServer(WithTransports(TransportSSE, TransportLongPolling))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	neg := negotiate(t, ts.URL)
	if len(neg.AvailableTransports) != 2 || neg.AvailableTransports[0].Name != TransportSSE {
		t.Fatalf("offered %+v", neg.AvailableTransports)
	}
	res, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound || !ShouldFallback(res.StatusCode) {
		t.Fatalf("websocket status %d", res.StatusCode)
	}
}

func TestOriginRejectsNegotiate(t *testing.T) {
	s := NewServer(WithOriginPatterns("example.com"))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/negotiate", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", "https://evil.example")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusForbidden || ShouldFallback(res.StatusCode) {
		t.Fatalf("status %d", res.StatusCode)
	}
}

func TestMaxConnections(t *testing.T) {
	s := NewServer(WithMaxConnections(1))
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	defer shutdown(t, s)

	c := dial(t, ts.URL, "/", nil)
	defer c.CloseNow()
	_ = readHello(t, c)
	waitCount(t, s, 1)
	assertDialStatus(t, ts, "/", "", http.StatusServiceUnavailable)
}

func negotiate(t *testing.T, base string) Negotiation {
	t.Helper()
	res, err := http.Post(base+"/negotiate", "application/json", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("negotiate %d %s", res.StatusCode, b)
	}
	var neg Negotiation
	if err := json.NewDecoder(res.Body).Decode(&neg); err != nil {
		t.Fatal(err)
	}
	return neg
}

func openSSE(t *testing.T, ctx context.Context, url, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(connectionTokenHeader, token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		t.Fatalf("sse %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		res.Body.Close()
		t.Fatalf("content-type %s", ct)
	}
	return res
}

func readSSE(t *testing.T, br *bufio.Reader) string {
	t.Helper()
	var buf bytes.Buffer
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("sse read: %v", err)
		}
		if line == "\n" {
			return buf.String()
		}
		if strings.HasPrefix(line, "data: ") {
			buf.WriteString(strings.TrimPrefix(line, "data: "))
		}
	}
	t.Fatal("sse timeout")
	return ""
}

func postEvent(t *testing.T, url, token, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(connectionTokenHeader, token)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("send %d %s", res.StatusCode, b)
	}
}

func poll(t *testing.T, url, token string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(connectionTokenHeader, token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("poll %d %s", res.StatusCode, b)
	}
	return string(b)
}

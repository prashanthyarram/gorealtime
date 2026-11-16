package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTextRoundTripAndPing(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := Upgrade(w, r, Config{})
		if err != nil {
			return
		}
		defer sess.CloseNow()
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = sess.Ping(ctx)
		}()
		binary, data, err := sess.Read(context.Background())
		if err != nil || binary || string(data) != "hi" {
			return
		}
		_ = sess.Write(context.Background(), []byte("yo"))
	}))
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	u := "ws" + strings.TrimPrefix(s.URL, "http") + "/"
	c, _, err := Dial(ctx, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	if err := c.Write(ctx, MessageText, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	typ, data, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if typ != MessageText || string(data) != "yo" {
		t.Fatalf("got %d %q", typ, data)
	}
}

func TestOriginForbidden(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := Upgrade(w, r, Config{})
		if err == nil {
			t.Error("expected origin rejection")
		}
	}))
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	u := "ws" + strings.TrimPrefix(s.URL, "http") + "/"
	_, resp, err := Dial(ctx, u, http.Header{"Origin": []string{"https://evil.example"}})
	if err == nil {
		t.Fatal("expected error")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("resp %v err %v", resp, err)
	}
}

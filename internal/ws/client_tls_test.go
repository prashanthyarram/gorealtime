package ws

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWSSValidatesCertificate(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sess, err := Upgrade(w, r, Config{})
		if err != nil {
			return
		}
		defer sess.CloseNow()
		_, data, err := sess.Read(r.Context())
		if err != nil {
			return
		}
		_ = sess.Write(r.Context(), data)
	}))
	defer srv.Close()

	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	raw := "wss" + strings.TrimPrefix(srv.URL, "https") + "/"
	c, _, err := DialClient(ctx, raw, ClientConfig{TLS: &tls.Config{RootCAs: pool}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	if err := c.Write(ctx, MessageText, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hi" {
		t.Fatalf("data %q", data)
	}
}

func TestWSSRejectsUntrustedCertificate(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = Upgrade(w, r, Config{})
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	raw := "wss" + strings.TrimPrefix(srv.URL, "https") + "/"
	c, _, err := DialClient(ctx, raw, ClientConfig{})
	if err == nil {
		c.CloseNow()
		t.Fatal("expected certificate verification to fail")
	}
	if c != nil {
		t.Fatal("client returned with an error")
	}
}

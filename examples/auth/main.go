// Command auth shows an authenticator in front of the handshake.
//
//	go run ./examples/auth
//
// A client must send Authorization: Bearer demo. The library does not
// parse tokens itself; this example does, and only for the demo.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/prashanthyarram/gorealtime"
)

func main() {
	srv := gorealtime.NewServer(
		gorealtime.WithPath("/realtime"),
		gorealtime.WithRequireAuth(),
		gorealtime.WithAuthenticator(func(ctx context.Context, r *http.Request) (gorealtime.AuthResult, error) {
			const prefix = "Bearer "
			h := r.Header.Get("Authorization")
			if !strings.HasPrefix(h, prefix) || strings.TrimPrefix(h, prefix) != "demo" {
				return gorealtime.AuthResult{}, gorealtime.ErrUnauthorized
			}
			return gorealtime.AuthResult{Identity: "demo-user"}, nil
		}),
	)
	srv.On("ping", func(ctx context.Context, conn *gorealtime.Connection, ev gorealtime.Event) error {
		return conn.Send(ctx, "pong", ev.Payload)
	})
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	addr := ":8080"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}
	log.Fatal(http.ListenAndServe(addr, mux))
}

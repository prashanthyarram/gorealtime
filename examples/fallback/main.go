// Command fallback serves WebSocket, server-sent events, and long polling.
//
//	go run ./examples/fallback
//
// POST /realtime/negotiate returns the transport list and a connection
// token. A client that cannot open a WebSocket may open GET /realtime/sse
// or GET /realtime/poll with X-Gorealtime-Connection-Token. Client events
// for those transports are POST /realtime/send. Do not put the token in a URL.
// HTTP 401, 403, and 409 are not reasons to try another transport.
package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/prashanthyarram/gorealtime"
)

func main() {
	srv := gorealtime.NewServer(gorealtime.WithPath("/realtime"))
	srv.On("echo", func(ctx context.Context, conn *gorealtime.Connection, ev gorealtime.Event) error {
		return conn.Send(ctx, "echo", ev.Payload)
	})
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	addr := ":8080"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}
	log.Fatal(http.ListenAndServe(addr, mux))
}

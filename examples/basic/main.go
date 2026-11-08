// Command basic is a minimal gorealtime server.
//
//	go run ./examples/basic
//
// Dial ws://localhost:8080/realtime. The first frame is a hello envelope.
// A text frame {"v":1,"type":"event","event":"ping"} is handled and logged
// by name only. GET /health returns 204.
package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/prashanth-yarram/gorealtime"
)

func main() {
	srv := gorealtime.NewServer(gorealtime.WithPath("/realtime"))
	srv.OnConnect(func(ctx context.Context, conn *gorealtime.Connection) error {
		log.Printf("open %s", conn.ID())
		return nil
	})
	srv.OnDisconnect(func(ctx context.Context, conn *gorealtime.Connection, reason string) {
		log.Printf("close %s %s", conn.ID(), reason)
	})
	srv.On("ping", func(ctx context.Context, conn *gorealtime.Connection, ev gorealtime.Event) error {
		log.Printf("event %s from %s (%d bytes)", ev.Name, conn.ID(), len(ev.Payload))
		return nil
	})
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	addr := ":8080"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}
	log.Printf("listening on %s path %s", addr, srv.Path())
	log.Fatal(http.ListenAndServe(addr, mux))
}

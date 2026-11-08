// Command basic is a minimal gorealtime server.
//
//	go run ./examples/basic
//
// Dial ws://localhost:8080/realtime. The first frame is a hello envelope
// whose payload.connectionId is the socket id. Further client frames are
// discarded until event handlers exist. GET /health returns 204.
package main

import (
	"log"
	"net/http"
	"os"

	"github.com/prashanth-yarram/gorealtime"
)

func main() {
	srv := gorealtime.NewServer(gorealtime.WithPath("/realtime"))
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

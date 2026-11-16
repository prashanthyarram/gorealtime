// Command compression enables optional permessage-deflate.
//
//	go run ./examples/compression
//
// Compression stays off for clients that do not offer it. Context takeover
// stays off. Do not enable this for frames that mix a secret with bytes a
// peer can choose.
package main

import (
	"log"
	"net/http"
	"os"

	"github.com/prashanthyarram/gorealtime"
)

func main() {
	srv := gorealtime.NewServer(
		gorealtime.WithPath("/realtime"),
		gorealtime.WithCompression(gorealtime.CompressionConfig{Enabled: true}),
	)
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	addr := ":8080"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}
	log.Fatal(http.ListenAndServe(addr, mux))
}

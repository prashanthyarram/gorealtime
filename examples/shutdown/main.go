// Command shutdown stops the library and the HTTP server on SIGINT.
//
//	go run ./examples/shutdown
//
// Shutdown refuses new handshakes and closes open connections. It does not
// flush queued frames and it does not stop the process by itself. The
// HTTP server is still the caller's.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/prashanthyarram/gorealtime"
)

func main() {
	srv := gorealtime.NewServer(gorealtime.WithPath("/realtime"))
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	addr := ":8080"
	if v := os.Getenv("ADDR"); v != "" {
		addr = v
	}
	httpSrv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("realtime shutdown: %v", err)
	}
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
}

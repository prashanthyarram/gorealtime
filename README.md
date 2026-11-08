# gorealtime

`gorealtime` is a Go library for process-local realtime delivery over WebSocket. You mount it on your own `net/http` server, and it accepts sockets, assigns a connection id, and keeps the connection until the peer leaves or you shut the server down.

It does not implement a chat product. It does not store messages, users, or memberships, and it does not speak the SignalR protocol. Authentication and authorization belong to the application. Those pieces are not in this module yet either: handlers, identity, groups, and backpressure land in later milestones. The sections below describe what the code does today.

## Install

```bash
go get github.com/prashanth-yarram/gorealtime
```

The module requires Go 1.22. The WebSocket implementation is `github.com/coder/websocket` v1.8.13. The reason for that pin is in `docs/architecture.md`.

## Server

```go
package main

import (
	"log"
	"net/http"

	"github.com/prashanth-yarram/gorealtime"
)

func main() {
	srv := gorealtime.NewServer(gorealtime.WithPath("/realtime"))
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	log.Fatal(http.ListenAndServe(":8080", mux))
}
```

`examples/basic` is the same server. A client that completes the handshake receives one text frame:

```json
{"v":1,"type":"hello","id":"<event-id>","origin":"<instance-id>","payload":{"connectionId":"<connection-id>"}}
```

`origin` identifies this process. It is not a credential. Inbound text and binary frames are read and discarded. The read is what keeps ping, pong, and close working.

## Connections

A connection is registered as soon as the handshake succeeds. `OnConnect` runs before the hello frame, on the connection goroutine. Return an error to reject the socket. A panic in the hook is logged with its stack and also rejects the socket. `OnDisconnect` runs only after `OnConnect` has returned nil and the connection is still open. It does not run for a rejected connect.

```go
srv.OnConnect(func(ctx context.Context, conn *gorealtime.Connection) error {
    return conn.SetIdentity("alice")
})
srv.OnDisconnect(func(ctx context.Context, conn *gorealtime.Connection, reason string) {
    log.Printf("closed %s (%s)", conn.ID(), reason)
})
```

Identity is an application string, not a user table. Several connections may use the same identity. An empty identity is anonymous and is not indexed. There is no client message that sets identity or metadata; only server-side methods do.

```go
conn, ok := srv.Connection(id)
same := srv.ConnectionsByIdentity("alice")
_ = conn.SetMetadata("locale", "en")
info := conn.Info()
conn.Close()
```

`Metadata` and `Info` return copies. Keys are limited to 64 bytes, values to 256, and 32 keys per connection. Identity is limited to 256 bytes. Control characters are rejected.

`Connection.Close` is idempotent. The first close wins the reason: `client_close`, `server_close`, `shutdown`, `read_error`, `write_error`, or `connect_rejected`. `Context` is canceled when cleanup starts.

Do not call `Shutdown` from a connect or disconnect hook. The hook runs on the connection goroutine, and `Shutdown` waits for it.

Hooks must not assume they can send on the socket. Sending is a later milestone. Hello is the only server frame today.

`Handler` is an `http.Handler` for a router other than `ServeMux`. It does not look at the request path. `RegisterRoutes` is what mounts it at `Path`.

## Configuration

| Option | Behavior |
| --- | --- |
| `WithPath` | Path used by `RegisterRoutes`. Empty means `/realtime`. The value is cleaned. |
| `WithLogger` | `*slog.Logger`. nil discards logs. Frame bodies are never logged. |
| `WithOriginPatterns` | Extra Origin hosts allowed to complete the handshake. `*` and URL-shaped patterns panic. |
| `WithInsecureSkipOriginCheck` | Disables the Origin check. Development only. |

A missing `Origin` is allowed. A present `Origin` must match the request host or a pattern. That stops a browser page on another site from opening the socket. It does not authenticate the caller. Non-browser clients can send any `Origin` they want.

`WithInsecureSkipOriginCheck` lets any site open the socket. Do not enable it on a browser-facing deployment.

## Shutdown

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := srv.Shutdown(ctx); err != nil {
	log.Fatal(err)
}
```

`Shutdown` stops new handshakes and aborts open sockets. It is safe to call more than once, including concurrently. A timed-out context does not reverse the shutdown; a later call with a live context waits for the same close to finish.

Call `Shutdown` even if you also call `http.Server.Shutdown`. Hijacked WebSocket connections stay up until this library closes them.

After shutdown, the handshake endpoint returns `503`.

## Errors and limits

There is no exported error set yet. Failed handshakes use HTTP status codes: `405` for a non-GET, `403` for a rejected origin, `503` after shutdown, and the WebSocket library's status for a malformed upgrade.

Each inbound frame is limited to 32 KiB. Each hello write is limited to 10 seconds. There is no heartbeat yet, so a half-open peer is not detected until the TCP session fails or the server shuts down.

## Scaling

Everything in this process is local. Connection ids are random 16-byte values from `crypto/rand`. They are not coordinated across instances. There is no shared bus. See `docs/architecture.md` for the delivery and deployment boundaries.

## License

MIT. Copyright (c) 2026 Prashanth Yarram.

# gorealtime

`gorealtime` is a Go library for process-local realtime delivery over WebSocket. You mount it on your own `net/http` server, and it accepts sockets, assigns a connection id, and keeps the connection until the peer leaves or you shut the server down.

It does not implement a chat product. It does not store messages, users, or memberships, and it does not speak the SignalR protocol. Authentication and authorization belong to the application. The library can require an identity, call an authenticator, and run authorizers the application supplies. It does not issue tokens or decide who may speak in a room.

## Install

```bash
go get github.com/prashanthyarram/gorealtime
```

The module requires Go 1.22. The WebSocket implementation is `github.com/coder/websocket` v1.8.13. The reason for that pin is in `docs/architecture.md`.

## Server

```go
package main

import (
	"log"
	"net/http"

	"github.com/prashanthyarram/gorealtime"
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

A handler can call `Send`, `Broadcast`, or `SendToGroup`. Those calls queue a frame; they do not write the socket on the read goroutine. A handler can also fail an event, and the server answers with an error frame.

## Events

Register a handler before or after the server is accepting. A second registration for the same name replaces the first. Handlers for one connection run one at a time on that connection's read goroutine, in the order the frames arrived. Do not wait for another frame on the same connection. Do not call `Shutdown` from a handler. Return when `ctx` ends.

```go
srv.On("chat.send", func(ctx context.Context, conn *gorealtime.Connection, ev gorealtime.Event) error {
    if len(ev.Payload) == 0 {
        return gorealtime.ErrInvalidPayload
    }
    return nil
})
```

The client sends a version 1 JSON text frame. `docs/protocol.md` is the envelope. `ev.Payload` is the raw JSON value and belongs to the handler. Binary frames, bad JSON, unknown events, and handler errors produce an `error` frame and leave the socket open. A frame larger than the read limit closes the socket.

```go
return gorealtime.WithMessage(gorealtime.ErrUnauthorized, "not a member")
```

`WithMessage` is the sentence the client sees. Any other handler error is logged on the server and reported as `handler_failed` with a fixed sentence. A panic is recovered, logged with its stack, and reported the same way. Do not put secrets in errors you return; those strings are logged. Payload bytes are not logged.

`OnAck` observes an application ack. An ack means the peer claims to have seen a frame id. It does not mean the server will retry anything.

`WithMaxPayloadBytes` sets the payload cap. The default is 32 KiB. The legal range is 1 byte to 8 MiB. The read limit is that value plus 4 KiB.

## Sending

`Send` queues one event for one connection. `SendToIdentity` queues it for every connection with that identity. `Broadcast` queues it for every connection. `SendToGroup` queues it for the members of a process-local group. The JSON is encoded once and the same frame is queued to each target. `ExcludeConnections` skips ids.

```go
err := srv.Send(ctx, conn.ID(), "chat.message", map[string]string{"text": "hi"})
err = conn.Send(ctx, "echo", ev.Payload)
err = srv.SendToGroup(ctx, "room-123", "chat.message", ev.Payload, gorealtime.ExcludeConnections(conn.ID()))
```

`nil` payload is JSON `null`. A `[]byte` or `json.RawMessage` must already be JSON; it is not wrapped again. Any other value is encoded with `encoding/json`.

The call returns when the frame is queued, not when the client has read it. A canceled context skips the insert. A full queue returns `ErrQueueFull` and the frame is dropped. If that connection's queue stays full for the slow-consumer timeout (default 15s), a later rejected send returns `ErrSlowConsumer` and the connection is closed. `WithSlowConsumerTimeout(0)` disables that close. `WithQueueSize` sets the depth (default 32, maximum 4096).

One slow connection does not stop a broadcast. Every target is attempted. The first queue error is returned.

`ErrConnectionNotFound` means the id or identity is not registered. `ErrServerClosed` means `Shutdown` has started.

Outbound memory is about `connections × queue size × frame size`, plus the frame currently being written. There is no separate global byte cap.

## Groups

Groups exist only in this process. `Join` and `AddToGroup` are trusted. The library does not decide who may join; the handler that calls `Join` does. A disconnect removes the connection from every group. Names use the same token grammar as event names.

```go
if err := conn.Join("room-123"); err != nil {
    return err
}
log.Println(srv.GroupSize("room-123"))
```

`examples/groups` broadcasts a line to a room. `examples/chat` keeps an in-memory slice of lines, maps a server-side demo token to an identity, and then asks the library to deliver. The slice is not inside the library.

`Handler` is an `http.Handler` for a router other than `ServeMux`. It does not look at the request path. `RegisterRoutes` is what mounts it at `Path`.

## Authentication

Middleware can attach an identity before the handler runs:

```go
h := srv.Handler()
http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    ctx := gorealtime.WithIdentity(r.Context(), "alice")
    h.ServeHTTP(w, r.WithContext(ctx))
})
```

`WithAuthenticator` runs on the HTTP request before the upgrade and replaces that identity. Return an error to refuse the handshake. `WithRequireAuth` answers HTTP 401 when the identity is still empty, and it does not open a socket. The 401 body is the word `unauthorized`. The server log records a bounded error string, not the request URI, headers, identity, or metadata. Do not put a token in the error you return.

```go
srv := gorealtime.NewServer(
    gorealtime.WithRequireAuth(),
    gorealtime.WithAuthenticator(func(ctx context.Context, r *http.Request) (gorealtime.AuthResult, error) {
        if r.Header.Get("Authorization") != "Bearer demo-alice" {
            return gorealtime.AuthResult{}, gorealtime.ErrUnauthorized
        }
        return gorealtime.AuthResult{Identity: "alice"}, nil
    }),
)
```

`AuthorizeEvents` runs before the handler. `ErrUnauthorized` becomes an error frame and leaves the socket open. `AuthorizeGroups` runs for `Join`, `Leave`, `AddToGroup`, and `RemoveFromGroup` with action `GroupJoin` or `GroupLeave`. It must not call those methods itself. `OnConnect` can still reject a socket that already upgraded.

`examples/chat` does not use HTTP auth. It maps a server-side demo token inside an event handler, then calls `SetIdentity`. That token is not a user id supplied by the client as the identity.

## Heartbeats and limits

A ping is sent every 30s. The pong must arrive within 10s. `WithHeartbeat(interval, timeout)` changes both. `WithHeartbeat(0, 0)` disables them. Pings start once the read loop is blocked in `Read`, because that is where the transport observes the pong. A missed pong closes the connection with `ReasonHeartbeat`.

`WithWriteTimeout` bounds one outbound frame. The default is 10s and the maximum is 2 minutes. `WithMaxPayloadBytes` bounds the JSON payload. The read limit is that value plus 4 KiB. There is no separate read-deadline context: canceling the context passed to `Read` closes the socket, so it cannot mean "wait for the next frame."

## Metrics

`WithMetrics` takes a `Metrics` value. Set the funcs you want. Nil funcs are skipped. Counts do not include payloads, identities, or tokens. `ConnectionClosed` receives a reason constant. A panic in a func is recovered and logged as `metrics.panic`. This module does not import Prometheus or OpenTelemetry.

## Configuration

| Option | Behavior |
| --- | --- |
| `WithPath` | Path used by `RegisterRoutes`. Empty means `/realtime`. The value is cleaned. |
| `WithLogger` | `*slog.Logger`. nil discards logs. Frame bodies are never logged. |
| `WithOriginPatterns` | Extra Origin hosts allowed to complete the handshake. `*` and URL-shaped patterns panic. |
| `WithInsecureSkipOriginCheck` | Disables the Origin check. Development only. |
| `WithQueueSize` | Outbound frames buffered per connection. 1 to 4096, default 32. |
| `WithSlowConsumerTimeout` | Close a connection that stays queue-full. Default 15s. Zero disables the close. |
| `WithMaxPayloadBytes` | Maximum JSON payload. 1 byte to 8 MiB, default 32 KiB. |
| `WithRequireAuth` | HTTP 401 when the handshake has no identity. |
| `WithAuthenticator` | Callback before upgrade. Replaces the middleware identity. |
| `WithHeartbeat` | Ping interval and pong timeout. Zero interval disables. Default 30s and 10s. |
| `WithWriteTimeout` | One outbound frame. Default 10s, maximum 2 minutes. |
| `WithMetrics` | Optional counters. nil funcs are skipped. |

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

`Shutdown` stops new handshakes. Each write loop then attempts one `bye` frame (`{"reason":"shutdown"}`) and aborts the TCP connection. Frames still sitting in the queue are dropped. `bye` is best-effort: a blocked write is bounded by `WithWriteTimeout`, and an in-flight ping is canceled. `Close` on one connection does not send `bye`. Shutdown is safe to call more than once, including concurrently. A timed-out context does not reverse it; a later call with a live context waits for the same close to finish.

Call `Shutdown` even if you also call `http.Server.Shutdown`. Hijacked WebSocket connections stay up until this library closes them.

After shutdown, the handshake endpoint returns `503`.

## Errors and limits

Sentinel errors: `ErrConnectionClosed`, `ErrInvalidIdentity`, `ErrInvalidMetadata`, `ErrInvalidEvent`, `ErrInvalidPayload`, `ErrPayloadTooLarge`, `ErrHandlerFailed`, `ErrSerialization`, `ErrUnauthorized`, `ErrConnectionNotFound`, `ErrServerClosed`, `ErrQueueFull`, `ErrSlowConsumer`, `ErrInvalidGroup`. `WithMessage` attaches the sentence a client may see. Other error text stays on the server.

Client error codes: `CodeInvalidMessage`, `CodeInvalidEvent`, `CodeInvalidPayload`, `CodePayloadTooLarge`, `CodeHandlerFailed`, `CodeUnauthorized`.

Handshake statuses: `401` when auth fails, `405` for a non-GET, `403` for a rejected origin, `503` after shutdown.

Disconnect reasons: `ReasonClientClose`, `ReasonServerClose`, `ReasonShutdown`, `ReasonReadError`, `ReasonWriteError`, `ReasonConnectRejected`, `ReasonSlowConsumer`, `ReasonHeartbeat`. The first reason wins.

`docs/security.md` is the threat boundary. `docs/protocol.md` is the envelope.

## Scaling

Everything in this process is local. Connection ids are random 16-byte values from `crypto/rand`. They are not coordinated across instances. There is no shared bus. See `docs/architecture.md` for the delivery and deployment boundaries. `docs/performance.md` is one measured run of the in-memory benchmarks, not a capacity guarantee.

## License

MIT. Copyright (c) 2026 Prashanth Yarram.

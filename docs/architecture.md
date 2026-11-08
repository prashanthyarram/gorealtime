# Architecture

`gorealtime` is a process-local realtime server library. A later chat application authenticates, checks membership, and stores messages, then asks this library to deliver an event to a connection, an identity, or a group. The library does not persist messages and does not decide who may see a conversation.

This document records decisions that are expensive to reverse. Milestone notes under `docs/milestones/` record the alternatives considered at the time.

## Transport

WebSocket is the only transport in this release. The handshake is `internal/ws`, built on `github.com/coder/websocket` v1.8.13. The root package does not import that module. Public APIs take `net/http` values and return this module's types.

`coder/websocket` is the maintained continuation of `nhooyr.io/websocket` (Coder took over maintenance in 2024). v1.8.13, released 2025-03-14, was evaluated against the needs of this library:

- Maintenance. The module is actively released. v1.8.14 (2025-09-06) and v1.8.15 (2026-06-15) continue that line.
- Concurrency. Methods may run concurrently except `Read`. One reader is required so ping, pong, and close frames are processed. Writes are synchronized inside the library.
- Context. `Read`, `Write`, and `Ping` take `context.Context`. Canceling or timing out that context closes the socket, including if the context is canceled after the call returns unless the library has reset its timeout to `context.Background`. Callers therefore use a fresh timeout context per write or ping and cancel it only after the call returns. The long-lived read uses `context.Background` and is unblocked with `CloseNow`.
- Performance. The library passes the Autobahn suite, supports low-allocation reads and writes, and does not require CGo. v1.8.13 starts one internal timeout goroutine per connection. v1.8.14 removes that goroutine by using `context.AfterFunc`, which needs Go 1.23.
- API. `Accept` works with `http.ResponseWriter` and does not force a router. Origin policy, read limits, and compression are explicit options. `Ping` waits for a pong that the read loop observes.
- Security. Cross-origin handshakes are rejected unless the origin host matches the request host or a `path.Match` host pattern. Compression is off unless requested. The default payload limit is 32 KiB. The WebSocket read limit is that value plus 4 KiB, and this library sets it explicitly. The library does not implement the WebSocket framing itself.

v1.8.14 and later set `go 1.23` in their `go.mod`. This module is built with Go 1.22, so those releases do not compile here. v1.8.13 is the newest release that does. The features left behind are real and small: scheme-aware origin patterns, `ErrMessageTooBig`, and the timeout-goroutine removal. Host patterns in v1.8.13 match the origin host only, which is the behavior this library documents. `golang.org/x/net/websocket` is deprecated. `gorilla/websocket` is widely used but its context support and close handshake are weaker, and the project has had long maintenance gaps. `gobwas/ws` is a lower-level framing toolkit; using it would mean implementing handshake and concurrency policy that `coder/websocket` already tests. None of those are a better fit than pinning v1.8.13.

There is no second transport. SSE, HTTP streaming, and long polling would implement the same session operations later: read a frame, write a frame, probe liveness, and abort. They are not implemented.

## Dependencies

| Module | Why |
| --- | --- |
| `github.com/coder/websocket` v1.8.13 | WebSocket handshake, frames, ping/pong, origin check, and read limit. See above. |

The standard library covers HTTP, JSON, logging (`log/slog`), and concurrency. Prometheus and OpenTelemetry are not dependencies. Metrics, when added, will be an interface the application implements.

## Process model

One `Server` is an in-memory registry plus an `http.Handler`. The caller runs `http.Server`, including TLS. `Shutdown` is the library's half of graceful stop: it refuses new handshakes and aborts open sockets. The caller still shuts down its own `http.Server`. Hijacked WebSockets are not closed by `http.Server.Shutdown`.

Connection ids are 16 random bytes, hex-encoded, from `crypto/rand`. They are unique within a process with overwhelming probability. They are not leased across instances. The instance id is the same kind of value, minted once per `Server`, and placed on server-originated frames as `origin`.

## Connections

Each socket is a `Connection`. The registry is two maps under one mutex: connection id, and identity to the set of connections that currently use it. Identity is optional and application-defined. Empty identity is not indexed. `sync.Map` is not used.

Lock order is the registry mutex, then the connection mutex. `SetIdentity` holds the registry lock across the index update, so a lookup that returns after `SetIdentity` returns does not still find the old identity.

`OnConnect` runs on the connection goroutine before hello. Failure and panics reject the socket and do not call `OnDisconnect`. Panics are recovered and logged with a stack. `OnDisconnect` runs after the socket is closed, only if connect succeeded. Hooks must not call `Shutdown`.

Cleanup is `sync.Once`. The first reason is kept.

## Concurrency

The registry mutex guards the accepting flag and both indexes. It is not held during network IO. The post-upgrade insert rechecks the accepting flag under that lock so shutdown and accept cannot lose a socket or add one after close.

Each connection has one read goroutine. `coder/websocket` adds its timeout goroutine. `Shutdown` waits until the library's goroutines finish. `sync.Once` makes cleanup run once if the read loop, `Close`, and shutdown overlap.

## Protocol

`docs/protocol.md` is the envelope. Version 1 JSON text frames. `internal/protocol` encodes and validates them and does not import the server. Client `origin` is ignored. The server stamps its instance id on frames it creates.

Handlers run sequentially on the connection read goroutine. That keeps order and caps handler goroutines at one per connection.

Outbound data frames share one bounded queue per connection and one write goroutine. Hello, error replies, and application events are ordered by enqueue. A send returns when the frame is queued. A full queue returns `ErrQueueFull` without waiting. The connection records the time it first became saturated. A later rejection after the slow-consumer timeout (default 15 seconds) closes it with `slow_consumer`. A successful enqueue clears that timer. Zero timeout leaves the connection up and still returns `ErrQueueFull`.

Fan-out encodes once. The frame slice is immutable after encoding and is queued to each target as-is. Server WebSocket frames are not masked, and the write copies into its own buffer, so sharing the slice does not race. Closed connections are skipped. One full queue does not stop the rest of a broadcast.

Groups are a pair of maps in this process: group to connections, and connection to groups. Disconnect removes both. There is no cross-instance membership.

## Security

Origin handling is described on `WithOriginPatterns`. A missing `Origin` is allowed because non-browser clients omit it; browsers send it. That check is not authentication. Frame bodies are not logged. The read limit is 32 KiB.

The application terminates TLS. Behind a reverse proxy, the proxy must forward `Connection: Upgrade` and `Upgrade: websocket` without buffering the byte stream. `RemoteAddr` is the peer the HTTP server sees. This library does not read `X-Forwarded-For`; a proxy can put a trusted address on the request before the handler runs.

## What this release is not

Delivery is not durable. A frame that has not been written is dropped when the socket closes. There is no Redis, NATS, or other shared bus. Groups are local to the process. A future distributed broadcast has to land on the instance that owns the connection and be written there. `origin` and frame ids are the hooks that adapter will use to avoid loops. Redis Pub/Sub is not a durable queue, and this library will not pretend otherwise.

## Current limitations

- Heartbeats are on by default (30s ping, 10s pong). A peer that never reads will miss the pong and be closed. `WithHeartbeat(0, 0)` leaves that detection to TCP.
- Server-initiated close aborts the TCP connection. It does not wait for a WebSocket close handshake, because that handshake needs the read lock the reader is holding. Shutdown writes one `bye` frame when it can, then aborts. `Close` does not send `bye`.
- `OnDisconnect` does not run when `OnConnect` fails or panics. It runs after the current handler returns when the read loop is what performs cleanup.
- Group membership and broadcast do not leave this process. `AuthorizeGroups` is optional. With no authorizer, `Join` is trusted server code.
- A queued frame is not a delivered frame. Nothing is replayed after a reconnect. Shutdown does not flush the queue.

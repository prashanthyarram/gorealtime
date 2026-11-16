# Architecture

`gorealtime` is a process-local realtime server library. A later chat application authenticates, checks membership, and stores messages, then asks this library to deliver an event to a connection, an identity, or a group. The library does not persist messages and does not decide who may see a conversation.

This document records decisions that are expensive to reverse. Milestone notes under `docs/milestones/` record the alternatives considered at the time.

## Transport

WebSocket is the only transport in this release. The handshake and frames live in `internal/ws` and use only the standard library (`net`, `crypto/sha1`, `bufio`, `net/http`). The root package does not re-export those types. Public APIs take `net/http` values and return this module's types.

`github.com/coder/websocket` v1.8.13 was the earlier choice. It is a maintained fork of `nhooyr.io/websocket`, and v1.8.13 was the newest release that compiled on Go 1.22. It was removed so this module has no external dependencies. `golang.org/x/net/websocket` is deprecated and is still a module outside the standard library. `gorilla/websocket` and `gobwas/ws` are external too. The library therefore implements the RFC 6455 subset it needs: the opening handshake, masked client frames, unmasked server frames, text and binary messages, ping/pong, close, a read-size limit, and the origin check.

Concurrency follows the same shape as before. `Read` must not run concurrently with itself, because that is the loop that answers ping and observes pong and close. `Write`, `Ping`, and `CloseNow` may run alongside `Read`. Writes share one mutex. A context that ends during `Read`, `Write`, or `Ping` fails that call by setting a socket deadline. Canceling the context after the call returns does not close the socket. `CloseNow` closes the TCP connection and unblocks a waiting read. Host patterns match the origin host with `path.Match`, case insensitively. A missing `Origin` is allowed. The default payload limit is 32 KiB. The WebSocket read limit is that value plus 4 KiB.

Permessage-deflate (RFC 7692) is optional and off unless `WithCompression` enables it. Negotiation lives next to the handshake. Each connection owns its own `compress/flate` writer and reader; the writer is used only under the write lock and the reader only under the read lock. RSV1 marks a compressed data message. Control frames are never compressed. The only window this process can use is 32,768 bytes, which is what `compress/flate` implements. An offer that requires `server_max_window_bits` below 15 is declined. Context takeover is off unless the application sets it, and a client's `server_no_context_takeover` is honored. `docs/protocol.md` and `docs/security.md` record the parameters and the CRIME/BREACH boundary.

There is no second transport. SSE, HTTP streaming, and long polling would implement the same session operations later: read a frame, write a frame, probe liveness, and abort. They are not implemented.

## Dependencies

There are none. `go.mod` has no `require` block.

The standard library covers HTTP, WebSocket framing, JSON, logging (`log/slog`), and concurrency. Prometheus and OpenTelemetry are not dependencies. Metrics are optional funcs the application supplies.

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

Each connection has one read goroutine and one write goroutine. `Shutdown` waits until those goroutines finish. `sync.Once` makes cleanup run once if the read loop, `Close`, and shutdown overlap.

## Protocol

`docs/protocol.md` is the envelope. Version 1 JSON text frames. `internal/protocol` encodes and validates them and does not import the server. Client `origin` is ignored. The server stamps its instance id on frames it creates.

Handlers run sequentially on the connection read goroutine. That keeps order and caps handler goroutines at one per connection.

Outbound data frames share one bounded queue per connection and one write goroutine. Hello, error replies, and application events are ordered by enqueue. A send returns when the frame is queued. A full queue returns `ErrQueueFull` without waiting. The connection records the time it first became saturated. A later rejection after the slow-consumer timeout (default 15 seconds) closes it with `slow_consumer`. A successful enqueue clears that timer. Zero timeout leaves the connection up and still returns `ErrQueueFull`.

Fan-out encodes once. The frame slice is immutable after encoding and is queued to each target as-is. Server WebSocket frames are not masked, and the write copies into its own buffer, so sharing the slice does not race. Closed connections are skipped. One full queue does not stop the rest of a broadcast.

Groups are a pair of maps in this process: group to connections, and connection to groups. Disconnect removes both. There is no cross-instance membership.

## Security

Origin handling is described on `WithOriginPatterns`. A missing `Origin` is allowed because non-browser clients omit it; browsers send it. That check is not authentication. Frame bodies are not logged. The default payload limit is 32 KiB.

The application terminates TLS. Behind a reverse proxy, the proxy must forward `Connection: Upgrade` and `Upgrade: websocket` without buffering the byte stream. `RemoteAddr` is the peer the HTTP server sees. This library does not read `X-Forwarded-For`; a proxy can put a trusted address on the request before the handler runs.

## What this release is not

Delivery is not durable. A frame that has not been written is dropped when the socket closes. There is no Redis, NATS, or other shared bus. Groups are local to the process. A future distributed broadcast has to land on the instance that owns the connection and be written there. `origin` and frame ids are the hooks that adapter will use to avoid loops. Redis Pub/Sub is not a durable queue, and this library will not pretend otherwise.

## Current limitations

- Heartbeats are on by default (30s ping, 10s pong). A peer that never reads will miss the pong and be closed. `WithHeartbeat(0, 0)` leaves that detection to TCP.
- Server-initiated close aborts the TCP connection. It does not wait for a WebSocket close handshake, because that handshake needs the read lock the reader is holding. Shutdown writes one `bye` frame when it can, then aborts. `Close` does not send `bye`.
- `OnDisconnect` does not run when `OnConnect` fails or panics. It runs after the current handler returns when the read loop is what performs cleanup.
- Group membership and broadcast do not leave this process. `AuthorizeGroups` is optional. With no authorizer, `Join` is trusted server code.
- A queued frame is not a delivered frame. Nothing is replayed after a reconnect. Shutdown does not flush the queue.
- Outbound messages are one WebSocket frame. Inbound compressed messages may be fragmented; RSV1 is honored only on the first frame, and the concatenated payload is inflated once.
- `server_max_window_bits` below 15 is not offered and is not accepted. `client_max_window_bits` is not written on the response. The inflater always keeps a 32,768-byte window, including when context takeover is off, because that is the size `compress/flate` allocates.
- The standard-library client still does not dial `wss`. TLS remains the caller's `http.Server` or the proxy in front of it.
- Compression does not look at event names. A secret and attacker-controlled bytes in the same frame are compressed together when compression is on. Turn it off for those sockets.

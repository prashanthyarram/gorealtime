# Milestone 1 — foundation

## Proposed design

Milestone 1 ships a module that can accept WebSocket connections on a configurable path, mint an id, send one hello frame, keep the socket open until the peer or the server closes it, and shut down idempotently. Event dispatch, identity, groups, queues, auth hooks, and heartbeats are later milestones. This milestone still reads every frame so `coder/websocket` can process ping, pong, and close. Inbound data frames are discarded on purpose; there is no dispatcher yet.

The application owns `net/http`. The library exposes `Handler` and `RegisterRoutes` and does not start a listener and does not pick a router.

Hello already uses the envelope shape later milestones will parse:

```json
{"v":1,"type":"hello","id":"<event-id>","origin":"<instance-id>","payload":{"connectionId":"<connection-id>"}}
```

`origin` is a per-process instance id. It is not a security boundary. It exists so a later distributed adapter can tell which process produced a frame.

## Package responsibilities

- Root package: options, HTTP entry, connection table, hello, shutdown.
- `internal/ws`: upgrade via `github.com/coder/websocket`, read/write/ping/close, translation of close errors. The root package does not import the WebSocket library, so its types never appear in the public API.
- `examples/basic`: a runnable server mounted on the standard library mux.

No connection manager package yet. A single mutex and a map are enough until identity indexing exists.

## Trade-offs

- Constructor does not return an error, matching a small `NewServer(opts...)` call. Invalid origin patterns panic at setup. The alternative was `NewServer(...) (*Server, error)`. That is easier to mishandle (`_ = err`) on a security setting, so a setup panic is the stricter choice. Runtime failures (upgrade, id generation for a request) return HTTP errors and do not panic.
- `coder/websocket` v1.8.13 is pinned because v1.8.14 and later require Go 1.23 and this environment is Go 1.22. See `docs/architecture.md`.
- Server-initiated close uses `CloseNow` while a read is in progress. `Conn.Close` tries to take the read lock for the close handshake and will wait several seconds if the read goroutine holds it. The peer still observes TCP close. A proper close handshake happens when the peer starts it, because the library answers that inside `Read`. Application-level goodbye frames arrive with messaging.
- Inbound messages are dropped, not queued. Queueing them now would build a buffer the dispatcher does not drain.

## Concurrency

- `Server.mu` guards `accepting` and `conns` only. It is not held across upgrade, read, or write.
- Registration rechecks `accepting` under the same lock that inserts the connection, after upgrade. Shutdown cannot miss a connection that passed the first check and cannot accept one that arrives after shutdown starts.
- One goroutine per connection reads. `coder/websocket` also starts its timeout goroutine. Both must exit before `Shutdown` returns.
- `Shutdown` is safe to call concurrently. The first call flips `accepting`, closes current sockets, and waits. Later calls wait for the same completion.
- `crypto/rand` ids are 16 bytes, hex-encoded. They are unique per process with overwhelming probability, not a cluster-wide lease.

## Security

- Origin policy is the `coder/websocket` default unless changed: a missing `Origin` is allowed (non-browser clients), and a present `Origin` must match the request host or an authorized host pattern. Host patterns use `path.Match` against the origin host only. `*` is rejected because it matches every host. Patterns that contain a scheme or slash are rejected so they cannot look like they work.
- `WithInsecureSkipOriginCheck` turns the check off. It is for local development. Browsers on any site could otherwise open the socket.
- Origin checks stop cross-site browser handshakes. They do not authenticate a client. A non-browser can send any `Origin` it wants.
- Read limit is 32 KiB per frame, set explicitly. The library default is the same; setting it keeps the cap obvious.
- Logs record connection id and remote address. They do not record frame bodies.
- The request context is not used as the connection lifetime. After hijack, that context is not a reliable cancel signal.

## Public API

- `NewServer(opts ...Option) *Server`
- `WithPath`, `WithLogger`, `WithOriginPatterns`, `WithInsecureSkipOriginCheck`
- `(*Server) Handler`, `RegisterRoutes`, `Shutdown`, `Path`, `InstanceID`, `ConnectionCount`
- `RouteRegistrar` so callers are not forced onto `http.ServeMux`

`Connection` is not exported yet. Callers cannot address a socket until the connection milestone.

## Tests

- Upgrade, hello shape, distinct connection ids, instance id echoed as `origin`.
- `RegisterRoutes` path and a sibling route on the same mux.
- Non-GET is 405. A bare GET is rejected by the WebSocket handshake, not by a panic.
- Client close drops the count to zero. An inbound data frame does not drop the connection and is not logged.
- Cross-origin rejection, same-host origin allowed, host pattern allowed, insecure skip allowed.
- `*` and scheme patterns panic.
- `Shutdown` is idempotent under concurrent callers, unblocks a peer, rejects later handshakes with 503, and leaves a zero count.
- Concurrent dials overlapping shutdown stay race-free and finish with a zero count.

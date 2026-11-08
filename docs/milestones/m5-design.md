# Milestone 5 — hardening

## Proposed design

Authentication stays in the application. The library only collects an identity the application already decided, and it can refuse the handshake when that identity is missing.

Three inputs, applied before the WebSocket upgrade:

1. `WithIdentity` on the request context. HTTP middleware calls it. The handshake reads it.
2. `WithAuthenticator`. The callback sees the `*http.Request` and returns an identity and metadata, or an error. It replaces the middleware identity. A nil callback leaves the middleware identity in place.
3. `WithRequireAuth`. An empty identity after the two steps above is HTTP 401 and the socket is not opened.

`OnConnect` is still the place that can reject a socket after it exists. An application that wants a first application frame to carry a demo token keeps using an event handler and `SetIdentity`. `AuthorizeEvents` can refuse every event except that one until identity is set. There is no second protocol for login.

`AuthorizeEvents` runs on the read goroutine before the handler. A refusal becomes an error frame and the socket stays up. `AuthorizeGroups` runs inside `Join`, `Leave`, `AddToGroup`, and `RemoveFromGroup`. Actions are `join` and `leave`. A nil authorizer allows the operation. The authorizer must not call back into join or leave.

Heartbeats use WebSocket ping/pong. The default is a ping every 30s and a 10s wait for the pong. `WithHeartbeat(0, 0)` turns them off. Pings run on the write goroutine, and only after the read loop is blocked in `Read`, because the transport observes the pong on the reader. The ping context is canceled after `Ping` returns. Canceling it on timeout closes the socket; the disconnect reason is `heartbeat_timeout`. Shutdown cancels an in-flight ping so the wait is not an extra timeout on top of the caller's context.

`WithWriteTimeout` bounds one data-frame write. The default is 10s. A read deadline is not a context on `Read`: this transport closes the socket when that context ends, so it cannot express "try again next frame." Dead peers are the heartbeat's job. The payload cap remains the read-size limit.

Shutdown still stops accepts, then asks each write loop to write one `bye` frame and abort the TCP connection. Queued application frames are not flushed. `bye` is best-effort. `Close` of one connection does not send `bye`.

`Metrics` is a struct of optional funcs. Nil funcs are skipped. A panic in one is recovered and logged as `metrics.panic`. There is no Prometheus or OpenTelemetry dependency.

## Package responsibilities

- `auth.go`: request identity, authenticator, authorizers.
- `metrics.go`: the struct and the emit helpers.
- `options.go`: require-auth, heartbeat, write timeout, metrics.
- `connection.go`: ping, bye, shutdown signal, heartbeat reason.
- `internal/protocol`: fuzz the decoder. No protocol change other than actually sending `bye`.

## Trade-offs

- Rejecting unauthenticated clients before upgrade returns 401 without a WebSocket error frame. The client has no connection id. That is the point: no socket, no read goroutine.
- The authenticator replaces middleware identity, including with an empty string. Merging the two would hide which layer won.
- Heartbeats default on. A quiet client that never reads will be closed. A client that reads will answer pings through the transport. Disabling them is one option.
- One bye, then `CloseNow`. A close handshake waits on the read lock the reader holds. The abort is the same trade-off as milestone 2.
- Metrics are funcs, not an interface. Callers set the counts they have and leave the rest nil.

## Concurrency

- The authenticator runs on the HTTP goroutine before the connection exists. It must not touch the registry.
- Authorizers run without the group lock and without the registry lock. They see a connection that may close underneath them.
- The write goroutine is the only pinger. It does not call `Ping` until the read goroutine has entered `Read`.
- Shutdown closes a per-connection channel. The write goroutine prefers that channel over another queued frame, writes bye, then aborts. The read goroutine still owns `OnDisconnect`.
- An in-flight ping watches that channel and cancels its context so shutdown does not wait out the pong timer. Canceling the ping context closes the socket. The first disconnect reason stays `shutdown` when shutdown already recorded it.

## Security

- 401 bodies are the fixed word `unauthorized`. Internal error text is logged, bounded, and not written to the client.
- The log line does not include the request URI, headers, identity, or metadata. An authenticator that puts a token in the error string will leak it through the log; the docs tell the application not to.
- Authorizer failures use the existing public error sentence. `WithMessage` is the only custom text.
- Origin checks, the payload cap, and the read limit are unchanged.
- Client frames still cannot set identity.

## Public API

- `AuthResult`, `Authenticator`, `WithAuthenticator`, `WithRequireAuth`
- `WithIdentity`, `IdentityFromContext`
- `EventAuthorizer`, `GroupAuthorizer`, `GroupJoin`, `GroupLeave`
- `AuthorizeEvents`, `AuthorizeGroups`
- `WithHeartbeat`, `WithWriteTimeout`
- `Metrics`, `WithMetrics`
- `ReasonHeartbeat`

## Tests

- `WithRequireAuth` answers 401 and does not register a connection.
- Middleware identity is visible after hello. An authenticator overrides it. A rejected token is 401 and the token bytes are absent from the log.
- An event authorizer prevents the handler from running and returns `unauthorized`.
- A group authorizer rejects `Join` and allows a different group.
- A client that does not read is closed with `heartbeat_timeout`. A client that reads survives several intervals.
- Shutdown produces a `bye` frame when the peer is reading.
- Fuzz `DecodeClient` with random bytes. It must not panic, and a successful decode must encode to valid JSON.

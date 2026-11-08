# Milestone 2 — connection management

## Proposed design

A `Connection` is the public handle for one socket. It carries an id, an optional application identity, a metadata map, a context, state, timestamps, and a disconnect reason. The server keeps two indexes under one mutex: id to connection, and identity to the set of connections that currently claim it. Identity is not a user record. The application sets it from a connect hook. Empty identity means anonymous and is not indexed. Many connections may share one identity.

`OnConnect` runs after the handshake and before hello. A non-nil error, or a panic, rejects the socket: no hello, no `OnDisconnect`. `OnDisconnect` runs only after `OnConnect` has returned nil and the connection was still open. Both run on the connection goroutine. They must return promptly and must not call `Server.Shutdown`, which waits for that goroutine.

Cleanup is `sync.Once`. The read loop, `Connection.Close`, and `Shutdown` all call it. The first reason wins.

The outbound queue is not part of this milestone. Hello is still one write on the connection goroutine before `Read`. A bounded queue and a write goroutine show up with messaging, where backpressure has a caller to apply to. The connection context and `Close` are the lifecycle those writes will select on.

## Package responsibilities

- `connection.go`: public `Connection`, state, metadata, identity validation, finish-once.
- `manager.go`: the two indexes and snapshot lookups. Not a separate package; the type is unexported and the server is the only caller, which avoids an import cycle with the public connection type.
- `errors.go`: sentinels this milestone returns.
- `server.go`: accept path calls the hooks, then hello, then the read loop.

`internal/ws` is unchanged.

## Trade-offs

- One mutex for both indexes, not `sync.Map`. The critical sections are map inserts. A benchmark can split the locks later if it shows contention. Two locks would need a documented order for a connection that moves identity while another goroutine removes it.
- Lock order is registry mutex, then connection mutex. Identity changes take the registry lock for the whole update so a lookup that returns after `SetIdentity` returns cannot observe the old index.
- `OnDisconnect` receives a reason string, not an error. Transport errors stay in the log. The hook runs after the socket is closed, so it cannot send a farewell. A goodbye frame belongs to messaging.
- Connect-hook panics are recovered. The panic value and stack are logged at error level. The connection is rejected. The process stays up. That recovery is visible; the failure is not turned into a successful connect.
- Rejected connects do not call `OnDisconnect`. The alternative was to always pair the hooks. Pairing them forces every reject path to look like a session that started. Hooks that allocate in `OnConnect` and free in `OnDisconnect` must free on the error return instead when connect fails.

## Concurrency

- Accept rechecks the registry under the same lock that inserts. `Shutdown` flips the accepting flag and takes the current map under that lock.
- `SetIdentity` and `remove` share the registry lock, so an identity cannot stay indexed after remove, and remove cannot unlink a stale identity that a concurrent `SetIdentity` already moved.
- Hook slices are copied under a separate mutex before they are called. A hook may register another hook. A hook must not need the registry lock to be free of the hook mutex: hook callbacks are invoked without either lock held, except for the connection methods they call.
- `Metadata` returns a copy. The caller can mutate the copy.
- `Connections` and `ConnectionsByIdentity` return snapshots. Membership can change immediately after the call. Order is not defined.

## Security

- The client has no frame that sets identity or metadata. Both are server-side methods. A later event handler must not copy a user id out of a payload into `SetIdentity` unless the application has authenticated it.
- Identity is at most 256 bytes and must not contain control characters, including newlines, so a value cannot forge a log line if the application logs it. This library does not log identity or metadata.
- Metadata keys are 1 to 64 bytes, values are at most 256 bytes, and a connection holds at most 32 keys. Control characters are rejected.
- `Info` includes metadata. Logging `Info` is the application's choice. The library does not log it.
- `RemoteAddr` is the HTTP server's peer address, not `X-Forwarded-For`.

## Public API

- `Connection` with `ID`, `Identity`, `SetIdentity`, `Metadata`, `SetMetadata`, `DeleteMetadata`, `Info`, `Context`, `State`, `Close`
- `StateConnecting`, `StateOpen`, `StateClosed`
- `Info`
- `ReasonClientClose`, `ReasonServerClose`, `ReasonShutdown`, `ReasonReadError`, `ReasonWriteError`, `ReasonConnectRejected`
- `ConnectHandler`, `DisconnectHandler`
- `Server.OnConnect`, `OnDisconnect`, `Connection`, `Connections`, `ConnectionsByIdentity`
- `ErrConnectionClosed`, `ErrInvalidIdentity`, `ErrInvalidMetadata`

`Connection.Close` is idempotent and returns no error. The second close is a no-op.

## Tests

- Connect hook sets identity and metadata; hello is sent; lookup by id and by identity sees them.
- Two sockets, one identity; closing one leaves the other.
- `SetIdentity` moves the index, and empty identity removes it.
- Invalid identity and metadata are rejected. A metadata copy does not alias the connection.
- Connect error and connect panic: no hello, count returns to zero, disconnect hook is not called, panic text is logged.
- Disconnect hook runs once when the client closes, when `Close` is called twice, and on shutdown, with the matching reason.
- `SetIdentity` and `SetMetadata` after close return `ErrConnectionClosed`.
- Inbound frame updates `LastReadAt` and is not retained.
- Concurrent identity changes, lookups, and closes. Concurrent add and remove of connections with no socket, under the race detector.
- Existing handshake, origin, and shutdown tests still pass.

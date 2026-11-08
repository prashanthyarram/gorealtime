# Milestone 3 — event system

## Proposed design

Clients and the server exchange one JSON text frame per envelope. Version is `1`. The types used now are `hello`, `event`, `ack`, and `error`. `hello` is server-only and already shipped. This milestone parses client frames and dispatches `event` and `ack`.

A handler is `func(ctx, *Connection, Event) error`. `Server.On` registers it by event name. The read goroutine calls handlers one at a time for that connection, in arrival order. It does not start a goroutine per event. Two connections dispatch independently.

The handler owns `Event.Payload`. The read buffer underneath the WebSocket library is not kept. Unknown events, malformed frames, and handler errors produce an `error` envelope and leave the socket open. A handler panic is recovered, logged with its stack, and reported as `handler_failed`. The panic value is not copied into the client message.

`origin` on a client frame is ignored. The server stamps its own instance id on frames it creates. A client cannot supply an identity in the envelope.

Sending application events, queues, and broadcast are the next milestone. This milestone writes only hello (already) and error replies, synchronously, from the connection goroutine between reads.

Shutdown and `Connection.Close` cancel the connection context and abort the socket. The read goroutine observes that, exits the handler if it is waiting on the context, and then runs cleanup. Handlers must return when `ctx` is done. They must not call `Shutdown`.

## Package responsibilities

- `internal/protocol`: encode, decode, and validate envelopes. No dependency on the server.
- `dispatch.go`: registration, dispatch, error replies, public error mapping.
- `server.go`: the read loop hands each data frame to dispatch.

## Trade-offs

- Handlers are sequential per connection. A concurrent handler pool would need a bound, a panic story, and an ordering rule. Ordering is the useful default for a single client stream, and the bound is one. The option to run handlers concurrently is rejected until a measured workload needs it.
- Unknown JSON fields are ignored so a later optional field does not break older peers. Unknown `type` values and the wrong `v` are rejected. The alternative, rejecting unknown fields, makes forward-compatible clients harder for no security gain at this size limit.
- Client `origin` is dropped rather than rejected. Rejecting would punish a client that echoes a server frame. The value is still untrusted.
- Error replies include a short public message, never `error.Error()`, unless the handler opted in with `WithMessage`. Internal errors are logged.
- The read limit is max payload plus 4 KiB of envelope overhead. A frame over that limit closes the socket inside `coder/websocket`; the library does not get a chance to send an error envelope. A payload over the configured max but inside the frame limit gets `payload_too_large` and the socket stays up.
- `On` panics on a bad event name or a nil handler. That is a programming error, same as a bad origin pattern.

## Concurrency

- Handler maps use the same mutex as the lifecycle hooks. The mutex is not held while a handler runs.
- Cleanup still runs once. `Close` and `Shutdown` record the reason, cancel the context, and abort the socket when the read loop is running. The read loop performs removal and `OnDisconnect`, so a handler and its disconnect hook do not overlap: the hook runs after the handler returns.
- A handler that waits for the next frame on the same connection deadlocks, because it is the read loop. The design note and the godoc say so.

## Security

- Event names, ids, and correlation ids are ASCII tokens with a length cap. Control characters in trace values are rejected.
- Payload must be JSON and at most the configured size (default 32 KiB, hard cap 8 MiB). The bytes are not logged.
- Binary frames are rejected as invalid messages. They are not decoded as text.
- Handler errors logged by the library can contain whatever the handler put in the error. Handlers should not put secrets in errors. The client receives a fixed sentence unless `WithMessage` set a public one.
- There is still no authentication. `ErrUnauthorized` exists so a handler can fail closed. The library does not decide who is unauthorized.

## Public API

- `Event`, `Handler`, `AckHandler`
- `Server.On`, `Server.OnAck`
- `WithMaxPayloadBytes`
- `WithMessage`
- `ErrInvalidEvent`, `ErrInvalidPayload`, `ErrPayloadTooLarge`, `ErrHandlerFailed`, `ErrSerialization`, `ErrUnauthorized`
- `CodeInvalidMessage`, `CodeInvalidEvent`, `CodeInvalidPayload`, `CodePayloadTooLarge`, `CodeHandlerFailed`, `CodeUnauthorized`

## Tests

- Protocol: round trip, bad version, bad type, bad name, bad id, bad trace, invalid JSON, payload over the max, unknown field kept out of the way, client origin not required.
- A client event reaches the handler with name, id, correlation id, and payload bytes.
- Unknown event, malformed JSON, bad version, binary frame: error envelope, socket stays, payload not logged.
- Payload over the max and under the frame cap: `payload_too_large`. Frame over the read limit: connection closes.
- Handler error text stays on the server. `WithMessage` is what the client sees. Panic becomes `handler_failed` and is logged.
- Two events on one connection run in order; the second waits until the first returns. Another connection proceeds during that wait.
- Ack with and without `OnAck`.
- Shutdown cancels a handler blocked on `ctx`.
- Replacing `On` for the same name uses the new handler.

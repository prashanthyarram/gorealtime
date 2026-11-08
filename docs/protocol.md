# Protocol

A gorealtime connection carries one JSON text frame per message. The server speaks version 1. Binary frames are rejected. This is not the SignalR protocol.

## Envelope

```json
{"v":1,"type":"event","event":"chat.message","id":"evt-1","corr":"evt-0","origin":"instance","trace":{"span":"s"},"payload":{"text":"hi"}}
```

| Field | JSON | Meaning |
| --- | --- | --- |
| Version | `v` | Integer. Only `1` is accepted. |
| Type | `type` | `hello`, `event`, `ack`, `error`, or `bye`. |
| Event name | `event` | Present on `event`. ASCII token, 1 to 128 bytes: letters, digits, `.`, `_`, `:`, `-`. |
| Frame id | `id` | Optional token, 1 to 64 bytes, same alphabet. Identifies this frame. |
| Correlation | `corr` | Optional token. On an `ack`, the id being acknowledged. Required for `ack`. |
| Origin | `origin` | Server instance id on frames the server creates. Ignored on client frames. |
| Trace | `trace` | Optional string map, at most 4 entries. Keys are tokens of at most 32 bytes. Values are at most 128 bytes and must not contain control characters. |
| Payload | `payload` | A JSON value. Absent is different from `null`. Default maximum 32 KiB. |
| Error | `error` | `{code, message}` on `error` frames. |

Unknown fields are ignored. A wrong version or an unknown type is rejected. The bytes of a payload are not logged.

## Types

`hello` is the first server frame after a successful connect hook:

```json
{"v":1,"type":"hello","id":"<frame-id>","origin":"<instance-id>","payload":{"connectionId":"<connection-id>"}}
```

`event` is an application message. The client may send it. A handler registered with `On` runs on the connection's read goroutine. One handler runs at a time per connection.

`ack` is an application-level signal that the peer has seen a frame id. It is not a delivery guarantee. `OnAck` observes it. With no handler, the ack is discarded.

`error` is how the server reports a rejected client frame or a failed handler. `corr` is the client frame id when the client sent one.

```json
{"v":1,"type":"error","id":"<frame-id>","corr":"<client-id>","origin":"<instance-id>","error":{"code":"handler_failed","message":"handler failed"}}
```

Codes:

| Code | When |
| --- | --- |
| `invalid_message` | Malformed JSON, wrong version, binary frame, or a type the client must not send. |
| `invalid_event` | Bad or unknown event name. |
| `invalid_payload` | Payload is not JSON, or a handler returned `ErrInvalidPayload`. |
| `payload_too_large` | Payload exceeds the configured maximum. |
| `unauthorized` | An authorizer or handler returned `ErrUnauthorized`. |
| `handler_failed` | A handler returned another error, or panicked. |

The `message` is a short public sentence. Handler error text is logged on the server and is not copied to the client unless the handler used `WithMessage`.

`bye` is a server frame sent once, best-effort, during `Shutdown`. The payload is `{"reason":"shutdown"}`. Clients cannot send `bye`. A queued application frame may be dropped instead of flushed.

## Limits

The WebSocket read limit is the payload maximum plus 4 KiB. A frame larger than that closes the socket; there is no error envelope, because the peer already violated the frame cap. A payload that fits in the frame but exceeds the payload maximum produces `payload_too_large` and the socket stays open.

The default payload maximum is 32 KiB. `WithMaxPayloadBytes` sets it, from 1 byte to 8 MiB.

## What a write means

A successful WebSocket write means the server handed the bytes to the connection. It does not mean the client applied them, stored them, or will see them after a reconnect. Acks are how an application records that a peer claims to have seen a frame id. The library does not replay missed frames.

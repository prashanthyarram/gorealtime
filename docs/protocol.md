# Protocol

A gorealtime connection carries one JSON text frame per message. The server speaks version 1. Binary frames are rejected. This is not the SignalR protocol. The same envelope is used on WebSocket, server-sent events, and long polling.

## Negotiation

`POST <path>/negotiate` returns the transports this process will accept, in the order a client should try them.

```json
{
  "protocolVersion": 1,
  "connectionId": "<id>",
  "connectionToken": "<token>",
  "expiresAt": "2026-01-01T00:00:00Z",
  "availableTransports": [
    {"name": "WebSockets", "transferFormats": ["Text"]},
    {"name": "ServerSentEvents", "transferFormats": ["Text"]},
    {"name": "LongPolling", "transferFormats": ["Text"]}
  ]
}
```

`connectionToken` expires if the transport is not established in time (default 30 seconds). After that it identifies that one connection until the connection closes. Send it in the `X-Gorealtime-Connection-Token` header. Do not put it in a query string. A second establish attempt receives HTTP 409. A missing or expired token receives HTTP 401.

WebSocket is `GET <path>` with the usual upgrade headers. The token header is optional there. SSE is `GET <path>/sse` and requires the token. The response is `text/event-stream`. Each envelope is one `data:` field. A heartbeat is a comment line, `: ping`. Long polling is `GET <path>/poll`. The body is `{"messages":[<envelope>, ...]}`. An empty wait ends as HTTP 204. Client events for SSE and long polling are `POST <path>/send` with one envelope and the same token. The response is HTTP 202 when the frame was queued for the connection's read loop, which is not the same as the handler having finished.

`ShouldFallback` is the client rule. Fall back only when the attempt never became an authenticated session: no TCP connection, HTTP 404, 501, 502, or 504. Do not fall back on 400, 401, 403, 409, 413, 429, or 503.

A queued or written frame is not an acknowledged frame. Reconnect does not replay. The server does not promise exactly-once delivery.

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

## Compression

Compression is off unless the server is built with `WithCompression` and the client offers `permessage-deflate`. The server accepts that extension only when every parameter in the chosen offer is one it implements. Other extensions are left out of the response. The handshake still completes.

The response lists the agreement. With the defaults, it is:

```text
permessage-deflate; server_no_context_takeover; client_no_context_takeover
```

| Parameter | What this process does |
| --- | --- |
| `server_no_context_takeover` | Included unless `ServerContextTakeover` is set and the client did not send this parameter. Each message the server sends then starts from an empty window. |
| `client_no_context_takeover` | Included unless `ClientContextTakeover` is set. A client hint alone does not disable the server's inflater history, because the client may still use context takeover when the response omits the parameter. |
| `server_max_window_bits` | Accepted only at 15. A value below 15 declines that offer. When the client sends 15, the response echoes `server_max_window_bits=15`. Otherwise the parameter is omitted, which means the same 32,768-byte window. |
| `client_max_window_bits` | A value from 8 to 15, or the name with no value, is accepted as a hint. The response does not include it. The inflater stays at 32,768 bytes. |

RSV1 is set on the first frame of a message this process compressed. It is clear on continuation frames, on uncompressed data frames, and on every control frame. A compressed message is the DEFLATE output of `compress/flate` after a sync flush, with the trailing `00 00 ff ff` removed, as RFC 7692 requires. The receiver appends those four bytes and inflates.

Fragmented input is concatenated, then inflated once. This process does not fragment outbound messages.

Messages shorter than `MinSize` (default 128 bytes) are sent with RSV1 clear and do not touch the compressor. When context takeover is off, a message that does not shrink is also sent uncompressed. When context takeover is on, a message that entered the compressor is sent compressed even if it grew, because the window cannot be rolled back.

The compressed size is bounded by `MaxCompressedBytes`, or by the WebSocket read limit when that field is zero. The inflated size is bounded by `MaxDecompressedBytes`, or by the same read limit when that field is zero. Inflation stops when the cap is crossed. The connection is closed. There is no error envelope for that, same as any other read-limit failure.

`Connection.Compression` reports the agreement for that socket, including the extension response string. The open log line includes `extensions` when negotiation succeeded.

## What a write means

A successful WebSocket write means the server handed the bytes to the connection. It does not mean the client applied them, stored them, or will see them after a reconnect. Acks are how an application records that a peer claims to have seen a frame id. The library does not replay missed frames.

# Security

The application terminates TLS and owns accounts, tokens, and room permissions. gorealtime checks the shape of frames, the size of payloads, and the calls the application asked it to make.

## Handshake

`WithOriginPatterns` compares the `Origin` host with `path.Match`. The request host is always allowed. A missing `Origin` is allowed because non-browser clients omit it. `*` and patterns that contain `://` or `/` panic at startup. `WithInsecureSkipOriginCheck` turns the check off and is for development.

`WithAuthenticator` and `WithIdentity` run before the upgrade. `WithRequireAuth` returns HTTP 401 when the identity is empty. The body is the word `unauthorized`. The log line is `auth.reject` plus a bounded error string. The request URI, headers, identity, and metadata are not logged. An authenticator that includes a token in the error it returns will put that token in the log.

A client frame cannot set identity or metadata. `SetIdentity` and `SetMetadata` are server methods.

## Frames

Inbound data is JSON text. Binary frames get an error envelope. The read limit is the payload maximum plus 4 KiB. A larger WebSocket frame closes the socket. A payload over the maximum, inside that frame, returns `payload_too_large` and stays open. The default payload maximum is 32 KiB. The ceiling is 8 MiB.

Event names, ids, and group names are ASCII tokens. Trace values reject control characters. Error sentences sent to the client are one line, control characters stripped, at most 200 runes. `WithMessage` is the only way to choose that sentence.

## Liveness

`WithWriteTimeout` bounds one write. The default is 10 seconds. Heartbeats are a ping every 30 seconds and a pong within 10 seconds, unless `WithHeartbeat` changes them. The ping context is canceled when the call returns. A timeout cancels it and the transport closes the socket. The reason is `heartbeat_timeout`.

Canceling the context passed to a long-lived `Read` also closes the socket, so the library does not use a read deadline that way. A peer that answers pings and sends no application data stays connected.

## What is not checked

The library does not parse `X-Forwarded-For`. `RemoteAddr` is the peer the HTTP server sees. A reverse proxy that should hide clients has to overwrite that before the handler, and it has to forward `Connection: Upgrade` and `Upgrade: websocket` without buffering the byte stream.

Join, broadcast, and send are authorized only when the application installed an authorizer or checked inside the handler. There is no JWT implementation and no permission model for rooms.

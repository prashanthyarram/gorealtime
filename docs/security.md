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

The library does not parse `X-Forwarded-For` or any other forwarded header. `RemoteAddr` is the peer the HTTP server sees. A reverse proxy that should hide clients has to overwrite that before the handler, and it has to forward `Connection: Upgrade` and `Upgrade: websocket` without buffering the byte stream. There is no trusted-proxy mode that reads those headers.

The negotiate token is scoped to one connection. It is compared as a map key, it is not logged, and it is not accepted from the query string. CORS echoes an allowed `Origin` and does not use `*`. A rejected origin is HTTP 403 on the HTTP transports and is not a reason to try the next transport.

Join, broadcast, and send are authorized only when the application installed an authorizer or checked inside the handler. There is no JWT implementation and no permission model for rooms.

## Compression

`WithCompression` is off unless `Enabled` is set. Leave it off for sockets that carry tokens, session identifiers, or other secrets in the same frame as data a peer can influence.

Permessage-deflate is the same class of leak as CRIME and BREACH. The ciphertext length depends on how well the plaintext compresses. An attacker who can choose part of a message and observe the compressed size can learn about the other bytes in that compression window. Context takeover extends the window across messages, so a secret in one frame can affect the size of a later frame. The default agreement sends `server_no_context_takeover` and `client_no_context_takeover`, which keeps each message in its own window. That does not make a single mixed frame safe.

The library does not inspect event names or payload fields. When compression is on, the whole JSON text frame is eligible. There is no separate switch for "sensitive" events. The application decides per server, which means per listener, by enabling compression or not.

Decompression does not trust the compressed length. Output is capped by `MaxDecompressedBytes`, which defaults to the WebSocket read limit (the payload maximum plus 4 KiB). A stream that would grow past that cap closes the socket. Compressed input is capped by `MaxCompressedBytes`, which cannot exceed that same read limit. `compress/flate` still allocates its 32,768-byte window for each direction of a negotiated connection. `Close` drops those buffers.

Offers this process cannot honor are not accepted. That includes `server_max_window_bits` below 15, duplicate parameters, and unknown parameters. The handshake continues without the extension. Unknown RSV bits, RSV1 on a control or continuation frame, and RSV1 when compression was not negotiated close the socket.

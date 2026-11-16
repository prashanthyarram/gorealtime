// Package gorealtime is a process-local realtime library for application-defined events.
//
// The application owns authentication, authorization, and storage. This package
// owns connection lifecycle, event transport, dispatch, connection routing, and
// local groups. It does not speak the SignalR protocol. It does not issue
// application credentials, and it does not persist or replay events. Delivery
// is at-most-once within one process: a queued frame is not a delivered frame,
// and nothing is replayed after a reconnect.
//
// WebSocket is the primary transport. Server-sent events and long polling are
// fallbacks negotiated at POST <path>/negotiate. Those two are not full-duplex
// sockets: the server writes the stream or the poll response, and the client
// posts events to <path>/send. A direct WebSocket upgrade on the mount path
// still works without negotiation.
//
// A server is mounted on the caller's net/http stack. The caller chooses the
// router, middleware, and TLS terminator. Shutdown stops new handshakes and
// closes connections that are already open; it is safe to call more than once.
package gorealtime

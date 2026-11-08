// Package gorealtime is a WebSocket library for application-defined realtime events.
//
// The application owns authentication, authorization, and storage. This package
// owns the socket lifecycle, event transport, dispatch, connection routing, and
// local groups. It does not speak the SignalR protocol and it does not persist
// messages.
//
// A server is mounted on the caller's net/http stack. The caller chooses the
// router, middleware, and TLS terminator. Shutdown stops new handshakes and
// closes sockets that are already open; it is safe to call more than once.
package gorealtime

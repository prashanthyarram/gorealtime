package gorealtime

import (
	"net/http"
	"path"
	"strings"
)

// Transport names returned by negotiation and reported on Connection.
const (
	// TransportWebSocket is the primary transport. It is a full-duplex
	// RFC 6455 socket on the mount path.
	TransportWebSocket = "WebSockets"
	// TransportSSE is a one-way event stream. Client events are a POST
	// to the send endpoint. This is not a bidirectional socket.
	TransportSSE = "ServerSentEvents"
	// TransportLongPolling holds a GET open until a frame is ready or the
	// poll timeout elapses. Client events are a POST to the send endpoint.
	TransportLongPolling = "LongPolling"
)

const (
	connectionTokenHeader = "X-Gorealtime-Connection-Token"

	epWebSocket = "websocket"
	epNegotiate = "negotiate"
	epSSE       = "sse"
	epSend      = "send"
	epPoll      = "poll"
	epUnknown   = "unknown"
)

// Handler is the HTTP endpoint for negotiation and every transport.
// Mount it with RegisterRoutes so the mount path and its subpaths are
// both routed. A client that only speaks WebSocket can still dial the
// mount path directly.
func (s *Server) Handler() http.Handler { return s }

// ServeHTTP dispatches negotiation, WebSocket, SSE, long polling, and
// the client-to-server POST. It applies WithMiddleware outside the dispatch.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := http.Handler(http.HandlerFunc(s.dispatch))
	for i := len(s.opts.middleware) - 1; i >= 0; i-- {
		h = s.opts.middleware[i](h)
	}
	h.ServeHTTP(w, r)
}

// RegisterRoutes mounts the server on mux at Path and at the negotiation,
// SSE, send, and long-poll paths under it. Each pattern is exact, so a
// request for /realtime is not redirected to /realtime/.
// A path of / is mounted as the mux root; subpaths are still dispatched
// by ServeHTTP.
func (s *Server) RegisterRoutes(mux RouteRegistrar) {
	p := s.opts.path
	if p == "/" {
		mux.Handle("/", s)
		return
	}
	mux.Handle(p, s)
	for _, sub := range []string{"/negotiate", "/sse", "/send", "/poll"} {
		mux.Handle(p+sub, s)
	}
}

func (s *Server) dispatch(w http.ResponseWriter, r *http.Request) {
	ep := endpoint(r.URL.Path, s.opts.path)
	if r.Method == http.MethodOptions && ep != epWebSocket && ep != epUnknown {
		s.writeCORS(w, r, true)
		return
	}
	switch ep {
	case epNegotiate:
		s.serveNegotiate(w, r)
	case epSSE:
		s.serveSSE(w, r)
	case epSend:
		s.serveSend(w, r)
	case epPoll:
		s.servePoll(w, r)
	case epUnknown:
		http.NotFound(w, r)
	default:
		s.serveWebSocket(w, r)
	}
}

func endpoint(urlPath, base string) string {
	p := path.Clean("/" + urlPath)
	base = path.Clean(base)
	if base != "/" && strings.HasPrefix(p, base+"/") {
		switch strings.TrimPrefix(p, base) {
		case "/negotiate":
			return epNegotiate
		case "/sse":
			return epSSE
		case "/send":
			return epSend
		case "/poll":
			return epPoll
		default:
			return epUnknown
		}
	}
	if p == base || p == "/" {
		return epWebSocket
	}
	switch p {
	case "/negotiate", "/sse", "/send", "/poll":
		return p[1:]
	default:
		return epWebSocket
	}
}

// ShouldFallback reports whether a client that failed to open a transport
// with this HTTP status may try the next transport from the negotiation
// response. Status 0 means the TCP connection never completed; that is a
// transport failure and fallback is appropriate.
//
// Authentication, authorization, malformed requests, policy rejections,
// and a closed or full server are not transport failures. Trying another
// transport will not change those outcomes.
func ShouldFallback(statusCode int) bool {
	switch statusCode {
	case 0, http.StatusNotFound, http.StatusNotImplemented, http.StatusBadGateway, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func tokenFrom(r *http.Request) string {
	if r == nil {
		return ""
	}
	return strings.TrimSpace(r.Header.Get(connectionTokenHeader))
}

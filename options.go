package gorealtime

import (
	"io"
	"log/slog"
	"path"
	"strings"
)

const (
	defaultPath       = "/realtime"
	defaultMaxPayload = 32 * 1024
	// maxPayloadLimit caps a single payload so a configuration cannot ask
	// for an unbounded read buffer.
	maxPayloadLimit = 8 << 20
)

// Option configures a Server. Options are applied in order at NewServer.
type Option func(*options)

type options struct {
	path               string
	log                *slog.Logger
	originPatterns     []string
	insecureSkipOrigin bool
	maxPayload         int
}

func defaultOptions() options {
	return options{
		path:       defaultPath,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		maxPayload: defaultMaxPayload,
	}
}

// WithPath sets the URL path RegisterRoutes mounts. The path is cleaned.
// An empty path selects /realtime. Handler itself does not check the path;
// mount it wherever the router should expose the handshake.
func WithPath(p string) Option {
	return func(o *options) {
		o.path = p
	}
}

// WithLogger sets the logger. nil selects a logger that discards every record.
// The library logs connection ids, remote addresses, and errors. It does not
// log frame bodies, tokens, or metadata values.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		if logger == nil {
			o.log = slog.New(slog.NewTextHandler(io.Discard, nil))
			return
		}
		o.log = logger
	}
}

// WithOriginPatterns authorizes cross-origin browser handshakes.
// Each pattern is matched against the Origin host with path.Match, case
// insensitively. The request host is always authorized. Examples: "example.com",
// "*.example.com".
//
// Patterns are hosts, not URLs. "*" is rejected because it matches every host;
// use WithInsecureSkipOriginCheck for that, and only in development. A pattern
// containing "://" or "/" panics because this release matches hosts only and a
// scheme-shaped pattern would never match.
//
// A missing Origin header is allowed. Non-browser clients omit it. Browsers
// send it. Origin checks reduce cross-site handshakes from browsers; they are
// not authentication. A non-browser client can set any Origin it wants.
func WithOriginPatterns(patterns ...string) Option {
	return func(o *options) {
		for _, p := range patterns {
			switch {
			case p == "":
				panic("gorealtime: origin pattern is empty")
			case p == "*":
				panic(`gorealtime: origin pattern "*" matches every host; use WithInsecureSkipOriginCheck only for local development`)
			case strings.Contains(p, "://") || strings.Contains(p, "/"):
				panic(`gorealtime: origin patterns match hosts only, for example "example.com" or "*.example.com"`)
			}
			o.originPatterns = append(o.originPatterns, p)
		}
	}
}

// WithInsecureSkipOriginCheck disables Origin verification.
// Any web page can then open a connection. Use it for local development or
// for a listener that is not reachable by browsers. Do not use it on a
// browser-facing deployment.
// WithMaxPayloadBytes sets the maximum JSON payload size in bytes.
// The WebSocket read limit is this value plus 4 KiB for the envelope.
// n must be between 1 and 8 MiB inclusive. Any other value panics.
// The default is 32 KiB.
func WithMaxPayloadBytes(n int) Option {
	return func(o *options) {
		if n < 1 || n > maxPayloadLimit {
			panic("gorealtime: max payload must be between 1 byte and 8 MiB")
		}
		o.maxPayload = n
	}
}

func WithInsecureSkipOriginCheck() Option {
	return func(o *options) {
		o.insecureSkipOrigin = true
	}
}

func (o *options) normalize() {
	if strings.TrimSpace(o.path) == "" {
		o.path = defaultPath
	}
	if !strings.HasPrefix(o.path, "/") {
		o.path = "/" + o.path
	}
	o.path = path.Clean(o.path)
	if o.log == nil {
		o.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

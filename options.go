package gorealtime

import (
	"io"
	"log/slog"
	"path"
	"strings"
	"time"
)

const (
	defaultPath       = "/realtime"
	defaultMaxPayload = 32 * 1024
	// maxPayloadLimit caps a single payload so a configuration cannot ask
	// for an unbounded read buffer.
	maxPayloadLimit         = 8 << 20
	defaultQueueSize        = 32
	maxQueueSize            = 4096
	defaultSlowTimeout      = 15 * time.Second
	defaultHeartbeat        = 30 * time.Second
	defaultHeartbeatTimeout = 10 * time.Second
	defaultWriteTimeout     = 10 * time.Second
)

// Option configures a Server. Options are applied in order at NewServer.
type Option func(*options)

type options struct {
	path               string
	log                *slog.Logger
	originPatterns     []string
	insecureSkipOrigin bool
	maxPayload         int
	queueSize          int
	slowTimeout        time.Duration
	heartbeat          time.Duration
	heartbeatTimeout   time.Duration
	writeTimeout       time.Duration
	requireAuth        bool
	authenticator      Authenticator
	metrics            *Metrics
}

func defaultOptions() options {
	return options{
		path:             defaultPath,
		log:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		maxPayload:       defaultMaxPayload,
		queueSize:        defaultQueueSize,
		slowTimeout:      defaultSlowTimeout,
		heartbeat:        defaultHeartbeat,
		heartbeatTimeout: defaultHeartbeatTimeout,
		writeTimeout:     defaultWriteTimeout,
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

// WithQueueSize sets how many outbound frames each connection buffers.
// n must be between 1 and 4096. The default is 32.
// A full queue rejects the send. It does not grow.
func WithQueueSize(n int) Option {
	return func(o *options) {
		if n < 1 || n > maxQueueSize {
			panic("gorealtime: queue size must be between 1 and 4096")
		}
		o.queueSize = n
	}
}

// WithSlowConsumerTimeout closes a connection that stays queue-full for d.
// The clock starts on the first rejected enqueue and resets when an enqueue
// succeeds. Zero disables the close; callers still receive ErrQueueFull.
// A negative duration panics. The default is 15 seconds.
func WithSlowConsumerTimeout(d time.Duration) Option {
	return func(o *options) {
		if d < 0 {
			panic("gorealtime: slow consumer timeout must not be negative")
		}
		o.slowTimeout = d
	}
}

// WithRequireAuth rejects the handshake with HTTP 401 when the identity is
// empty after middleware and the authenticator. The socket is not opened.
func WithRequireAuth() Option {
	return func(o *options) {
		o.requireAuth = true
	}
}

// WithAuthenticator sets the callback that runs before the upgrade.
// nil clears it. See Authenticator for what the callback may return.
func WithAuthenticator(fn Authenticator) Option {
	return func(o *options) {
		o.authenticator = fn
	}
}

// WithHeartbeat sets how often a ping is written and how long the pong may take.
// The defaults are 30s and 10s. interval 0 disables heartbeats and ignores timeout.
// A negative duration panics. A positive interval with a zero timeout panics.
// Pings start after the read loop is blocked in Read.
func WithHeartbeat(interval, timeout time.Duration) Option {
	return func(o *options) {
		switch {
		case interval < 0 || timeout < 0:
			panic("gorealtime: heartbeat duration must not be negative")
		case interval == 0:
			o.heartbeat = 0
			o.heartbeatTimeout = 0
		case timeout == 0:
			panic("gorealtime: heartbeat timeout must be positive when heartbeats are enabled")
		default:
			o.heartbeat = interval
			o.heartbeatTimeout = timeout
		}
	}
}

// WithWriteTimeout bounds one outbound data frame.
// d must be positive and at most 2 minutes. The default is 10 seconds.
// The write goroutine does not start another frame or a ping while a write
// is in progress, so this also bounds how long a heartbeat can be delayed.
func WithWriteTimeout(d time.Duration) Option {
	return func(o *options) {
		if d <= 0 || d > 2*time.Minute {
			panic("gorealtime: write timeout must be positive and at most 2 minutes")
		}
		o.writeTimeout = d
	}
}

// WithMetrics installs optional counters. nil clears them.
// See Metrics for which funcs are called and what they are not given.
func WithMetrics(m *Metrics) Option {
	return func(o *options) {
		o.metrics = m
	}
}

// WithInsecureSkipOriginCheck disables Origin verification.
// Any web page can then open a connection. Use it for local development or
// for a listener that is not reachable by browsers. Do not use it on a
// browser-facing deployment.
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

package gorealtime

import (
	"compress/flate"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/prashanthyarram/gorealtime/internal/protocol"
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
	defaultTokenTTL         = 30 * time.Second
	defaultPollTimeout      = 20 * time.Second
	defaultMaxLongPolls     = 1024
	// defaultMinCompress is the smallest payload that is compressed.
	defaultMinCompress = 128
	// maxMessageLimit is the largest compressed or decompressed message
	// a configuration may name. It matches the payload ceiling plus the
	// envelope allowance.
	maxMessageLimit = maxPayloadLimit + protocol.MaxOverhead
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
	compression        compressionSettings
	enabledTransports  map[string]bool
	maxConns           int
	maxLongPolls       int
	pollTimeout        time.Duration
	tokenTTL           time.Duration
	middleware         []func(http.Handler) http.Handler
}

type compressionSettings struct {
	enabled         bool
	level           int
	serverContext   bool
	clientContext   bool
	minSize         int
	maxDecompressed int
	maxCompressed   int
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
		maxLongPolls:     defaultMaxLongPolls,
		pollTimeout:      defaultPollTimeout,
		tokenTTL:         defaultTokenTTL,
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
	if o.maxLongPolls == 0 {
		o.maxLongPolls = defaultMaxLongPolls
	}
	if o.pollTimeout == 0 {
		o.pollTimeout = defaultPollTimeout
	}
	if o.tokenTTL == 0 {
		o.tokenTTL = defaultTokenTTL
	}
	if !o.compression.enabled {
		return
	}
	if o.compression.level == 0 {
		o.compression.level = flate.DefaultCompression
	}
	if o.compression.minSize == 0 {
		o.compression.minSize = defaultMinCompress
	}
	readLimit := o.maxPayload + protocol.MaxOverhead
	if o.compression.maxDecompressed == 0 {
		o.compression.maxDecompressed = readLimit
	}
	if o.compression.maxCompressed == 0 || o.compression.maxCompressed > readLimit {
		// A compressed frame cannot be larger than the read the socket
		// is willing to buffer. A higher setting would not be enforced.
		o.compression.maxCompressed = readLimit
	}
}

// CompressionConfig is the permessage-deflate policy for a server.
// Compression is off unless Enabled is set. The zero value of
// WithCompression(CompressionConfig{}) leaves compression off.
//
// Level 0 selects compress/flate.DefaultCompression. Explicit levels
// are HuffmanOnly (-2), DefaultCompression (-1), and 1 through 9.
// flate.NoCompression is not selected by 0; leave Enabled false instead.
//
// ServerContextTakeover and ClientContextTakeover are off unless set.
// Off is the safe default: each message is compressed on its own, which
// avoids carrying a secret from one frame into the next. See docs/security.md.
//
// MaxDecompressedBytes is the largest message accepted after inflation.
// Zero uses the WebSocket read limit, which is the payload maximum plus
// 4 KiB. Inflation stops when that cap is crossed.
// MaxCompressedBytes is the largest compressed message accepted.
// Zero uses the same read limit. A larger value is reduced to that limit
// because the socket will not buffer more than that.
//
// MinSize is the smallest payload that is compressed. Zero selects 128
// bytes. Shorter messages are sent uncompressed. Messages that do not
// shrink, and that are not using context takeover, are also sent uncompressed.
type CompressionConfig struct {
	Enabled               bool
	Level                 int
	ServerContextTakeover bool
	ClientContextTakeover bool
	MaxDecompressedBytes  int
	MaxCompressedBytes    int
	MinSize               int
}

// WithTransports restricts which transports negotiation offers and which
// endpoints accept a connection. Names are TransportWebSocket,
// TransportSSE, and TransportLongPolling. The default is all three, in
// that order. An unknown name panics. An empty list panics.
// WebSocket remains the direct upgrade on the mount path when it is enabled.
func WithTransports(names ...string) Option {
	return func(o *options) {
		if len(names) == 0 {
			panic("gorealtime: WithTransports requires at least one transport")
		}
		set := make(map[string]bool, len(names))
		for _, name := range names {
			switch name {
			case TransportWebSocket, TransportSSE, TransportLongPolling:
				set[name] = true
			default:
				panic("gorealtime: unknown transport " + name)
			}
		}
		o.enabledTransports = set
	}
}

// WithMaxConnections caps registered connections. n must be at least 1.
// A new handshake above the cap receives HTTP 503. Zero is not accepted;
// omit the option for no cap.
func WithMaxConnections(n int) Option {
	return func(o *options) {
		if n < 1 {
			panic("gorealtime: max connections must be at least 1")
		}
		o.maxConns = n
	}
}

// WithMaxLongPolls caps how many long-poll requests may wait at once
// across the process. n must be between 1 and 100000. The default is 1024.
func WithMaxLongPolls(n int) Option {
	return func(o *options) {
		if n < 1 || n > 100000 {
			panic("gorealtime: max long polls must be between 1 and 100000")
		}
		o.maxLongPolls = n
	}
}

// WithPollTimeout sets how long one long poll waits for a frame before
// answering 204. d must be positive and at most 2 minutes. The default
// is 20 seconds, short enough that a proxy idle timeout usually fires later.
func WithPollTimeout(d time.Duration) Option {
	return func(o *options) {
		if d <= 0 || d > 2*time.Minute {
			panic("gorealtime: poll timeout must be positive and at most 2 minutes")
		}
		o.pollTimeout = d
	}
}

// WithConnectionTokenTTL sets how long a negotiated connection token may
// sit unused before the transport is established. d must be at least one
// second and at most five minutes. The default is 30 seconds. After the
// transport is established the token identifies that connection until it
// closes. It is not an application credential.
func WithConnectionTokenTTL(d time.Duration) Option {
	return func(o *options) {
		if d < time.Second || d > 5*time.Minute {
			panic("gorealtime: connection token TTL must be from 1s to 5m")
		}
		o.tokenTTL = d
	}
}

// WithMiddleware wraps every request this server handles, including the
// WebSocket upgrade. The first function is the outermost wrapper.
// A nil function panics.
func WithMiddleware(mw ...func(http.Handler) http.Handler) Option {
	return func(o *options) {
		for _, fn := range mw {
			if fn == nil {
				panic("gorealtime: nil middleware")
			}
			o.middleware = append(o.middleware, fn)
		}
	}
}

func (o *options) transportEnabled(name string) bool {
	if len(o.enabledTransports) == 0 {
		return true
	}
	return o.enabledTransports[name]
}

// WithCompression turns on permessage-deflate negotiation.
// The extension is advertised only when the client offers a parameter
// set this process can honor. Clients that do not offer it stay uncompressed.
// Enabled false clears a previous WithCompression.
func WithCompression(cfg CompressionConfig) Option {
	return func(o *options) {
		if !cfg.Enabled {
			o.compression = compressionSettings{}
			return
		}
		switch cfg.Level {
		case 0, flate.DefaultCompression, flate.HuffmanOnly:
		default:
			if cfg.Level < flate.BestSpeed || cfg.Level > flate.BestCompression {
				panic("gorealtime: compression level must be 0, -2, -1, or 1 through 9")
			}
		}
		if cfg.MaxDecompressedBytes < 0 || cfg.MaxCompressedBytes < 0 || cfg.MinSize < 0 {
			panic("gorealtime: compression sizes must not be negative")
		}
		if cfg.MaxDecompressedBytes > maxMessageLimit || cfg.MaxCompressedBytes > maxMessageLimit {
			panic("gorealtime: compression limit is larger than 8 MiB plus 4 KiB")
		}
		o.compression = compressionSettings{
			enabled:         true,
			level:           cfg.Level,
			serverContext:   cfg.ServerContextTakeover,
			clientContext:   cfg.ClientContextTakeover,
			minSize:         cfg.MinSize,
			maxDecompressed: cfg.MaxDecompressedBytes,
			maxCompressed:   cfg.MaxCompressedBytes,
		}
	}
}

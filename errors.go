package gorealtime

import "errors"

var (
	// ErrConnectionClosed is returned when an operation needs an open connection.
	ErrConnectionClosed = errors.New("gorealtime: connection closed")

	// ErrInvalidIdentity is returned when an identity is too long or contains a control character.
	ErrInvalidIdentity = errors.New("gorealtime: invalid identity")

	// ErrInvalidMetadata is returned when a metadata key or value is rejected,
	// or when the connection already holds the maximum number of keys.
	ErrInvalidMetadata = errors.New("gorealtime: invalid metadata")

	// ErrInvalidEvent is returned for a missing or illegal event name.
	ErrInvalidEvent = errors.New("gorealtime: invalid event")

	// ErrInvalidPayload is returned when a payload is not JSON the application accepts.
	ErrInvalidPayload = errors.New("gorealtime: invalid payload")

	// ErrPayloadTooLarge is returned when a payload exceeds the configured limit.
	ErrPayloadTooLarge = errors.New("gorealtime: payload too large")

	// ErrHandlerFailed is returned when a handler returns an unclassified error or panics.
	ErrHandlerFailed = errors.New("gorealtime: handler failed")

	// ErrSerialization is returned when a frame cannot be encoded.
	ErrSerialization = errors.New("gorealtime: serialization failed")

	// ErrUnauthorized is returned when an authenticator, authorizer, or the
	// application rejects a handshake, event, or group operation.
	ErrUnauthorized = errors.New("gorealtime: unauthorized")

	// ErrConnectionNotFound means the connection id or identity is not registered.
	ErrConnectionNotFound = errors.New("gorealtime: connection not found")

	// ErrServerClosed means the server is no longer accepting or delivering.
	ErrServerClosed = errors.New("gorealtime: server closed")

	// ErrQueueFull means the connection's outbound queue rejected a frame.
	ErrQueueFull = errors.New("gorealtime: outbound queue full")

	// ErrSlowConsumer means the queue stayed full long enough that the
	// connection is being closed.
	ErrSlowConsumer = errors.New("gorealtime: slow consumer")

	// ErrInvalidGroup means the group name is empty or not a token,
	// or the operation does not apply.
	ErrInvalidGroup = errors.New("gorealtime: invalid group operation")
)

// Client-visible error codes. They travel in error envelopes.
const (
	CodeInvalidMessage  = "invalid_message"
	CodeInvalidEvent    = "invalid_event"
	CodeInvalidPayload  = "invalid_payload"
	CodePayloadTooLarge = "payload_too_large"
	CodeHandlerFailed   = "handler_failed"
	CodeUnauthorized    = "unauthorized"
)

// withMessage attaches a sentence that is safe to send to the client.
type withMessage struct {
	error
	msg string
}

func (e *withMessage) Unwrap() error { return e.error }

// WithMessage marks err with a public sentence.
// The client receives that sentence. The server log still has err.
// A nil err returns nil. Do not put secrets in public.
func WithMessage(err error, public string) error {
	if err == nil {
		return nil
	}
	return &withMessage{error: err, msg: public}
}

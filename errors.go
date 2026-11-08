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

	// ErrUnauthorized is returned by application code that rejects an event.
	// The library does not decide authorization by itself.
	ErrUnauthorized = errors.New("gorealtime: unauthorized")
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

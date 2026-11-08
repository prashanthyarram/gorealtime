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
)

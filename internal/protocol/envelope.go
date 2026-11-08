// Package protocol encodes and validates the gorealtime JSON envelope.
// It has no dependency on the server.
package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

const (
	// Version is the only envelope version this package accepts.
	Version = 1

	// MaxOverhead is extra frame bytes allowed above the application payload
	// for the envelope fields around it.
	MaxOverhead = 4 * 1024

	maxEventBytes = 128
	maxIDBytes    = 64
	maxTraceKeys  = 4
	maxTraceKey   = 32
	maxTraceValue = 128
)

const (
	TypeHello = "hello"
	TypeEvent = "event"
	TypeAck   = "ack"
	TypeError = "error"
	TypeBye   = "bye"
)

var (
	// ErrVersion means v is missing or not Version.
	ErrVersion = errors.New("protocol: invalid version")
	// ErrType means type is missing or not a client type.
	ErrType = errors.New("protocol: invalid type")
	// ErrEvent means the event name is missing or not a token.
	ErrEvent = errors.New("protocol: invalid event")
	// ErrPayload means the frame is not one JSON value or the payload is not JSON.
	ErrPayload = errors.New("protocol: invalid payload")
	// ErrTooLarge means the payload exceeds the configured maximum.
	ErrTooLarge = errors.New("protocol: payload too large")
	// ErrField means an id, correlation id, or trace entry is not allowed.
	ErrField = errors.New("protocol: invalid field")
)

// ErrorBody is the client-visible part of an error envelope.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Envelope is one versioned frame.
// Payload is raw JSON. Nil means the field was absent.
type Envelope struct {
	V       int               `json:"v"`
	Type    string            `json:"type"`
	Event   string            `json:"event,omitempty"`
	ID      string            `json:"id,omitempty"`
	Corr    string            `json:"corr,omitempty"`
	Origin  string            `json:"origin,omitempty"`
	Trace   map[string]string `json:"trace,omitempty"`
	Payload json.RawMessage   `json:"payload,omitempty"`
	Error   *ErrorBody        `json:"error,omitempty"`
}

// Encode serializes e. A zero V is written as Version.
// Encode does not apply client validation; the server uses it for its own frames.
func Encode(e Envelope) ([]byte, error) {
	if e.V == 0 {
		e.V = Version
	}
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("protocol: encode: %w", err)
	}
	return b, nil
}

// DecodeClient parses one client frame.
// maxPayload is the maximum length of the payload field in bytes.
// The error does not include frame contents.
func DecodeClient(data []byte, maxPayload int) (Envelope, error) {
	if maxPayload < 1 {
		return Envelope{}, fmt.Errorf("%w: payload limit", ErrField)
	}
	if len(data) == 0 || !json.Valid(data) {
		return Envelope{}, fmt.Errorf("%w: malformed json", ErrPayload)
	}
	if len(data) > maxPayload+MaxOverhead {
		return Envelope{}, ErrTooLarge
	}
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, fmt.Errorf("%w: malformed json", ErrPayload)
	}
	if env.V != Version {
		return Envelope{}, ErrVersion
	}
	switch env.Type {
	case TypeEvent, TypeAck:
	default:
		return Envelope{}, ErrType
	}
	if err := validateToken(env.ID, maxIDBytes, true); err != nil {
		return Envelope{}, fmt.Errorf("%w: id", ErrField)
	}
	if err := validateToken(env.Corr, maxIDBytes, true); err != nil {
		return Envelope{}, fmt.Errorf("%w: corr", ErrField)
	}
	if err := validateTrace(env.Trace); err != nil {
		return Envelope{}, err
	}
	if env.Payload != nil {
		if len(env.Payload) > maxPayload {
			return Envelope{}, ErrTooLarge
		}
		if !json.Valid(env.Payload) {
			return Envelope{}, fmt.Errorf("%w: payload", ErrPayload)
		}
	}
	switch env.Type {
	case TypeEvent:
		if err := ValidateEventName(env.Event); err != nil {
			return Envelope{}, err
		}
	case TypeAck:
		if env.Corr == "" {
			return Envelope{}, fmt.Errorf("%w: ack requires corr", ErrField)
		}
	}
	return env, nil
}

// ValidateEventName reports whether name is a non-empty ASCII token.
func ValidateEventName(name string) error {
	if err := validateToken(name, maxEventBytes, false); err != nil {
		return fmt.Errorf("%w: %v", ErrEvent, err)
	}
	return nil
}

func validateToken(s string, max int, optional bool) error {
	if s == "" {
		if optional {
			return nil
		}
		return errors.New("empty")
	}
	if len(s) > max || !utf8.ValidString(s) {
		return errors.New("length")
	}
	for i := 0; i < len(s); i++ {
		if !isTokenByte(s[i]) {
			return errors.New("character")
		}
	}
	return nil
}

func isTokenByte(b byte) bool {
	return b == '.' || b == '_' || b == ':' || b == '-' ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9')
}

func validateTrace(trace map[string]string) error {
	if len(trace) > maxTraceKeys {
		return fmt.Errorf("%w: trace", ErrField)
	}
	for k, v := range trace {
		if err := validateToken(k, maxTraceKey, false); err != nil {
			return fmt.Errorf("%w: trace key", ErrField)
		}
		if len(v) > maxTraceValue || !utf8.ValidString(v) || hasControl(v) {
			return fmt.Errorf("%w: trace value", ErrField)
		}
	}
	return nil
}

func hasControl(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

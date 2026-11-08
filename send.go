package gorealtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/prashanth-yarram/gorealtime/internal/protocol"
)

// SendOption adjusts one outbound event.
type SendOption func(*sendConfig)

type sendConfig struct {
	id      string
	corr    string
	trace   map[string]string
	exclude map[string]struct{}
}

// ExcludeConnections skips these connection ids in a fan-out.
// Unknown ids are ignored. It has no effect on Server.Send.
func ExcludeConnections(ids ...string) SendOption {
	return func(cfg *sendConfig) {
		if cfg.exclude == nil {
			cfg.exclude = make(map[string]struct{}, len(ids))
		}
		for _, id := range ids {
			if id != "" {
				cfg.exclude[id] = struct{}{}
			}
		}
	}
}

// WithEventID sets the frame id. The default is a random id.
// The id must be an ASCII token of at most 64 bytes.
func WithEventID(id string) SendOption {
	return func(cfg *sendConfig) { cfg.id = id }
}

// WithCorrelationID sets the correlation id on the frame.
func WithCorrelationID(id string) SendOption {
	return func(cfg *sendConfig) { cfg.corr = id }
}

// WithTrace copies trace metadata onto the frame.
// At most 4 entries are kept. Keys are tokens. Values must not contain
// control characters. Invalid trace data fails the send.
func WithTrace(trace map[string]string) SendOption {
	return func(cfg *sendConfig) {
		if len(trace) == 0 {
			return
		}
		cfg.trace = make(map[string]string, len(trace))
		for k, v := range trace {
			cfg.trace[k] = v
		}
	}
}

func applySendOptions(opts []SendOption) sendConfig {
	var cfg sendConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// Send writes one event to the connection.
// The frame is queued, not written, before Send returns. A canceled ctx
// prevents the queue insert. A full queue returns ErrQueueFull or
// ErrSlowConsumer. payload nil is JSON null. A []byte or json.RawMessage
// must already be JSON. Other values are encoded with encoding/json.
func (c *Connection) Send(ctx context.Context, event string, payload any, opts ...SendOption) error {
	if c.server == nil {
		return ErrConnectionClosed
	}
	frame, err := c.server.buildEvent(event, payload, applySendOptions(opts))
	if err != nil {
		return err
	}
	return c.enqueue(ctx, frame)
}

// Send writes one event to the connection with id.
// A missing id is ErrConnectionNotFound while the server is accepting,
// and ErrServerClosed after shutdown.
func (s *Server) Send(ctx context.Context, connID, event string, payload any, opts ...SendOption) error {
	c, ok := s.reg.get(connID)
	if !ok {
		if !s.reg.acceptingNow() {
			return ErrServerClosed
		}
		return ErrConnectionNotFound
	}
	return c.Send(ctx, event, payload, opts...)
}

// SendToIdentity writes one event to every connection indexed under identity.
// Zero connections is ErrConnectionNotFound. An empty identity is ErrInvalidIdentity.
func (s *Server) SendToIdentity(ctx context.Context, identity, event string, payload any, opts ...SendOption) error {
	if identity == "" {
		return ErrInvalidIdentity
	}
	conns := s.ConnectionsByIdentity(identity)
	if len(conns) == 0 {
		if !s.reg.acceptingNow() {
			return ErrServerClosed
		}
		return ErrConnectionNotFound
	}
	return s.deliver(ctx, conns, event, payload, opts)
}

// Broadcast writes one event to every registered connection.
// It returns ErrServerClosed when the server is no longer accepting.
// An empty server returns nil.
func (s *Server) Broadcast(ctx context.Context, event string, payload any, opts ...SendOption) error {
	if !s.reg.acceptingNow() {
		return ErrServerClosed
	}
	return s.deliver(ctx, s.Connections(), event, payload, opts)
}

// SendToGroup writes one event to the current members of a process-local group.
// An unknown or empty group delivers to nobody and returns nil.
// ExcludeConnections skips members. The group name uses the event-name token grammar.
func (s *Server) SendToGroup(ctx context.Context, group, event string, payload any, opts ...SendOption) error {
	if err := validateGroup(group); err != nil {
		return err
	}
	if !s.reg.acceptingNow() {
		return ErrServerClosed
	}
	return s.deliver(ctx, s.groups.snapshot(group), event, payload, opts)
}

// AddToGroup joins connID to a process-local group.
// The group is created if needed. This is a trusted server call.
func (s *Server) AddToGroup(connID, group string) error {
	if err := validateGroup(group); err != nil {
		return err
	}
	c, ok := s.reg.get(connID)
	if !ok {
		if !s.reg.acceptingNow() {
			return ErrServerClosed
		}
		return ErrConnectionNotFound
	}
	if err := s.authorizeGroup(c, group, GroupJoin); err != nil {
		return err
	}
	return s.groups.add(c, group)
}

// RemoveFromGroup leaves the group. A missing membership is not an error.
func (s *Server) RemoveFromGroup(connID, group string) error {
	if err := validateGroup(group); err != nil {
		return err
	}
	c, ok := s.reg.get(connID)
	if !ok {
		if !s.reg.acceptingNow() {
			return ErrServerClosed
		}
		return ErrConnectionNotFound
	}
	if err := s.authorizeGroup(c, group, GroupLeave); err != nil {
		return err
	}
	s.groups.remove(c, group)
	return nil
}

// GroupSize returns how many connections are in the group.
func (s *Server) GroupSize(group string) int { return s.groups.size(group) }

// Join adds this connection to a process-local group.
// AuthorizeGroups, when set, runs first with action GroupJoin.
func (c *Connection) Join(group string) error {
	if c.server == nil {
		return ErrConnectionClosed
	}
	if err := validateGroup(group); err != nil {
		return err
	}
	if err := c.server.authorizeGroup(c, group, GroupJoin); err != nil {
		return err
	}
	return c.server.groups.add(c, group)
}

// Leave removes this connection from the group.
// Leaving a group the connection is not in is not an error.
// AuthorizeGroups, when set, runs first with action GroupLeave.
func (c *Connection) Leave(group string) error {
	if err := validateGroup(group); err != nil {
		return err
	}
	if c.server == nil {
		return ErrConnectionClosed
	}
	if err := c.server.authorizeGroup(c, group, GroupLeave); err != nil {
		return err
	}
	c.server.groups.remove(c, group)
	return nil
}

func (s *Server) deliver(ctx context.Context, conns []*Connection, event string, payload any, opts []SendOption) error {
	cfg := applySendOptions(opts)
	frame, err := s.buildEvent(event, payload, cfg)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var first error
	for _, c := range conns {
		if cfg.exclude != nil {
			if _, skip := cfg.exclude[c.id]; skip {
				continue
			}
		}
		err := c.enqueue(ctx, frame)
		if err == nil || errors.Is(err, ErrConnectionClosed) {
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if first == nil {
			first = err
		}
	}
	return first
}

func (s *Server) buildEvent(event string, payload any, cfg sendConfig) ([]byte, error) {
	if err := protocol.ValidateEventName(event); err != nil {
		return nil, ErrInvalidEvent
	}
	if cfg.id != "" {
		if err := protocol.ValidateID(cfg.id); err != nil {
			return nil, ErrInvalidEvent
		}
	}
	if cfg.corr != "" {
		if err := protocol.ValidateID(cfg.corr); err != nil {
			return nil, ErrInvalidEvent
		}
	}
	if err := validateSendTrace(cfg.trace); err != nil {
		return nil, err
	}
	raw, err := marshalPayload(payload, s.opts.maxPayload)
	if err != nil {
		return nil, err
	}
	id := cfg.id
	if id == "" {
		id = newEventID()
	}
	return protocol.Encode(protocol.Envelope{
		V:       protocol.Version,
		Type:    protocol.TypeEvent,
		Event:   event,
		ID:      id,
		Corr:    cfg.corr,
		Origin:  s.instanceID,
		Trace:   cfg.trace,
		Payload: raw,
	})
}

func marshalPayload(payload any, max int) (json.RawMessage, error) {
	switch p := payload.(type) {
	case nil:
		return json.RawMessage("null"), nil
	case json.RawMessage:
		return checkRaw(p, max)
	case []byte:
		return checkRaw(p, max)
	default:
		b, err := json.Marshal(p)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrSerialization, err)
		}
		if len(b) > max {
			return nil, ErrPayloadTooLarge
		}
		return b, nil
	}
}

func checkRaw(p []byte, max int) (json.RawMessage, error) {
	if len(p) > max {
		return nil, ErrPayloadTooLarge
	}
	if !json.Valid(p) {
		return nil, ErrInvalidPayload
	}
	return append(json.RawMessage(nil), p...), nil
}

func validateSendTrace(trace map[string]string) error {
	if len(trace) == 0 {
		return nil
	}
	if len(trace) > 4 {
		return fmt.Errorf("%w: trace", ErrInvalidPayload)
	}
	for k, v := range trace {
		if k == "" || len(k) > 32 || protocol.ValidateEventName(k) != nil {
			return fmt.Errorf("%w: trace", ErrInvalidPayload)
		}
		if len(v) > 128 || hasControl(v) {
			return fmt.Errorf("%w: trace", ErrInvalidPayload)
		}
	}
	return nil
}

package gorealtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"unicode"

	"github.com/prashanthyarram/gorealtime/internal/protocol"
)

// Event is one inbound application event.
// Payload is the raw JSON value, owned by the handler. It is nil when the
// client omitted the field. A JSON null arrives as []byte("null").
type Event struct {
	Name          string
	ID            string
	CorrelationID string
	Payload       []byte
	Trace         map[string]string
}

// Handler handles one event name. Handlers for a connection run one at a time
// on that connection's read goroutine, in arrival order. Do not wait for the
// next frame on the same connection, and do not call Server.Shutdown.
// Return when ctx is done. A panic is recovered, logged, and reported to the
// client as handler_failed. The connection stays open.
type Handler func(ctx context.Context, conn *Connection, ev Event) error

// AckHandler handles an application-level ack. correlationID is the id the
// peer is acknowledging. Acks are not delivery guarantees; they are a signal
// the application can record. A nil registration accepts and discards acks.
type AckHandler func(ctx context.Context, conn *Connection, correlationID string) error

// On registers h for event. A later On for the same name replaces h.
// A nil handler or an illegal event name panics.
func (s *Server) On(event string, h Handler) {
	if h == nil {
		panic("gorealtime: nil event handler")
	}
	if err := protocol.ValidateEventName(event); err != nil {
		panic("gorealtime: invalid event name")
	}
	s.hookMu.Lock()
	if s.handlers == nil {
		s.handlers = make(map[string]Handler)
	}
	s.handlers[event] = h
	s.hookMu.Unlock()
}

// OnAck registers the ack handler. A later call replaces it.
// A nil handler panics. Use this only as an application-level signal.
func (s *Server) OnAck(h AckHandler) {
	if h == nil {
		panic("gorealtime: nil ack handler")
	}
	s.hookMu.Lock()
	s.onAck = h
	s.hookMu.Unlock()
}

func (s *Server) handlerFor(event string) Handler {
	s.hookMu.Lock()
	defer s.hookMu.Unlock()
	return s.handlers[event]
}

func (s *Server) ackHandler() AckHandler {
	s.hookMu.Lock()
	defer s.hookMu.Unlock()
	return s.onAck
}

// handleFrame decodes one data frame and dispatches it.
// A non-nil error means the write path failed and the read loop should stop.
func (s *Server) handleFrame(c *Connection, binary bool, data []byte) error {
	if binary {
		return s.writeError(c, "", CodeInvalidMessage, "binary frames are not accepted")
	}
	env, err := protocol.DecodeClient(data, s.opts.maxPayload)
	if err != nil {
		code, msg := protocolFault(err)
		s.opts.log.Info("frame.reject", "id", c.ID(), "code", code)
		return s.writeError(c, "", code, msg)
	}
	switch env.Type {
	case protocol.TypeEvent:
		return s.dispatchEvent(c, env)
	case protocol.TypeAck:
		return s.dispatchAck(c, env)
	default:
		return s.writeError(c, env.ID, CodeInvalidMessage, "unsupported message type")
	}
}

func (s *Server) dispatchEvent(c *Connection, env protocol.Envelope) error {
	ev := Event{
		Name:          env.Event,
		ID:            env.ID,
		CorrelationID: env.Corr,
		Payload:       append([]byte(nil), env.Payload...),
		Trace:         copyTrace(env.Trace),
	}
	if err := s.authorizeEvent(c, ev.Name); err != nil {
		s.opts.log.Info("event.denied", "id", c.ID(), "event", ev.Name, "err", boundErr(err))
		code, msg := clientFault(err)
		return s.writeError(c, env.ID, code, msg)
	}
	s.noteEventReceived()
	h := s.handlerFor(ev.Name)
	if h == nil {
		s.opts.log.Info("event.unknown", "id", c.ID(), "event", ev.Name)
		return s.writeError(c, env.ID, CodeInvalidEvent, "unknown event")
	}
	err := s.callHandler(c, ev.Name, func() error {
		return h(c.Context(), c, ev)
	})
	if err == nil {
		return nil
	}
	s.opts.log.Error("handler.failed", "id", c.ID(), "event", ev.Name, "err", boundErr(err))
	code, msg := clientFault(err)
	return s.writeError(c, env.ID, code, msg)
}

func (s *Server) dispatchAck(c *Connection, env protocol.Envelope) error {
	h := s.ackHandler()
	if h == nil {
		return nil
	}
	err := s.callHandler(c, "ack", func() error {
		return h(c.Context(), c, env.Corr)
	})
	if err == nil {
		return nil
	}
	s.opts.log.Error("ack.failed", "id", c.ID(), "err", boundErr(err))
	code, msg := clientFault(err)
	return s.writeError(c, env.ID, code, msg)
}

func (s *Server) callHandler(c *Connection, name string, fn func() error) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			s.opts.log.Error("handler.panic",
				"id", c.ID(),
				"event", name,
				"panic", fmt.Sprint(rec),
				"stack", string(debug.Stack()),
			)
			err = fmt.Errorf("%w: panic", ErrHandlerFailed)
		}
		if err != nil {
			s.noteHandlerFailed()
		}
	}()
	return fn()
}

func (s *Server) writeError(c *Connection, corr, code, public string) error {
	frame, err := protocol.Encode(protocol.Envelope{
		V:      protocol.Version,
		Type:   protocol.TypeError,
		ID:     newEventID(),
		Corr:   corr,
		Origin: s.instanceID,
		Error: &protocol.ErrorBody{
			Code:    code,
			Message: publicMessage(public),
		},
	})
	if err != nil {
		s.opts.log.Error("frame.encode", "id", c.ID(), "err", err.Error())
		return fmt.Errorf("%w: %v", ErrSerialization, err)
	}
	if err := c.enqueue(context.Background(), frame); err != nil {
		if errors.Is(err, ErrQueueFull) || errors.Is(err, ErrSlowConsumer) {
			s.opts.log.Info("connection.write", "id", c.ID(), "err", err.Error())
			return nil
		}
		return err
	}
	return nil
}

func protocolFault(err error) (string, string) {
	switch {
	case errors.Is(err, protocol.ErrTooLarge):
		return CodePayloadTooLarge, "payload too large"
	case errors.Is(err, protocol.ErrEvent):
		return CodeInvalidEvent, "invalid event"
	case errors.Is(err, protocol.ErrVersion):
		return CodeInvalidMessage, "unsupported protocol version"
	default:
		return CodeInvalidMessage, "invalid message"
	}
}

func clientFault(err error) (string, string) {
	code := CodeHandlerFailed
	fallback := "handler failed"
	switch {
	case errors.Is(err, ErrUnauthorized):
		code, fallback = CodeUnauthorized, "unauthorized"
	case errors.Is(err, ErrInvalidPayload):
		code, fallback = CodeInvalidPayload, "invalid payload"
	case errors.Is(err, ErrPayloadTooLarge):
		code, fallback = CodePayloadTooLarge, "payload too large"
	case errors.Is(err, ErrInvalidEvent):
		code, fallback = CodeInvalidEvent, "invalid event"
	}
	return code, optedMessage(err, fallback)
}

func optedMessage(err error, fallback string) string {
	var wm *withMessage
	if errors.As(err, &wm) && wm.msg != "" {
		return publicMessage(wm.msg)
	}
	return fallback
}

func publicMessage(s string) string {
	s = oneLine(s)
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r < 0x20 || r == 0x7f || unicode.IsControl(r) {
			continue
		}
		out = append(out, r)
	}
	const max = 200
	if len(out) > max {
		out = out[:max]
	}
	if len(out) == 0 {
		return "invalid message"
	}
	return string(out)
}

func copyTrace(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// writeRaw writes one frame with the server write timeout.
func (c *Connection) writeRaw(frame []byte) error {
	if c.sess == nil {
		return ErrConnectionClosed
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.frameWriteTimeout())
	err := c.sess.Write(ctx, frame)
	cancel()
	if err != nil {
		return err
	}
	c.markWrite()
	return nil
}

// encodeHello builds the accept frame.
func encodeHello(eventID, instanceID, connectionID string) ([]byte, error) {
	payload, err := json.Marshal(struct {
		ConnectionID string `json:"connectionId"`
	}{ConnectionID: connectionID})
	if err != nil {
		return nil, err
	}
	return protocol.Encode(protocol.Envelope{
		V:       protocol.Version,
		Type:    protocol.TypeHello,
		ID:      eventID,
		Origin:  instanceID,
		Payload: payload,
	})
}

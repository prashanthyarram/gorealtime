package gorealtime

import "runtime/debug"

// Metrics counts library events. Every func is optional.
// The library does not pass payloads, identities, metadata, or tokens.
// ConnectionClosed receives a Reason constant. A panic in a func is recovered
// and logged as metrics.panic. There is no Prometheus client in this module.
type Metrics struct {
	ConnectionsActive func(n int)
	ConnectionOpened  func()
	ConnectionClosed  func(reason string)
	EventsReceived    func()
	FramesWritten     func()
	HandlerFailed     func()
	QueueSaturated    func()
	SlowConsumer      func()
	AuthFailed        func()
	WriteFailed       func()
}

func (s *Server) emit(fn func()) {
	if s == nil || fn == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			s.opts.log.Error("metrics.panic", "panic", rec, "stack", string(debug.Stack()))
		}
	}()
	fn()
}

func (s *Server) noteOpened() {
	m := s.opts.metrics
	if m == nil {
		return
	}
	s.emit(m.ConnectionOpened)
	n := s.ConnectionCount()
	if m.ConnectionsActive != nil {
		s.emit(func() { m.ConnectionsActive(n) })
	}
}

func (s *Server) noteClosed(reason string) {
	m := s.opts.metrics
	if m == nil {
		return
	}
	if m.ConnectionClosed != nil {
		s.emit(func() { m.ConnectionClosed(reason) })
	}
	n := s.ConnectionCount()
	if m.ConnectionsActive != nil {
		s.emit(func() { m.ConnectionsActive(n) })
	}
}

func (s *Server) noteEventReceived() {
	if s.opts.metrics == nil {
		return
	}
	s.emit(s.opts.metrics.EventsReceived)
}

func (s *Server) noteFrameWritten() {
	if s.opts.metrics == nil {
		return
	}
	s.emit(s.opts.metrics.FramesWritten)
}

func (s *Server) noteHandlerFailed() {
	if s.opts.metrics == nil {
		return
	}
	s.emit(s.opts.metrics.HandlerFailed)
}

func (s *Server) noteQueueSaturated() {
	if s.opts.metrics == nil {
		return
	}
	s.emit(s.opts.metrics.QueueSaturated)
}

func (s *Server) noteSlowConsumer() {
	if s.opts.metrics == nil {
		return
	}
	s.emit(s.opts.metrics.SlowConsumer)
}

func (s *Server) noteAuthFailed() {
	if s.opts.metrics == nil {
		return
	}
	s.emit(s.opts.metrics.AuthFailed)
}

func (s *Server) noteWriteFailed() {
	if s.opts.metrics == nil {
		return
	}
	s.emit(s.opts.metrics.WriteFailed)
}

package gorealtime

import (
	"context"
	"fmt"
	"net/http"
	"runtime/debug"
)

// GroupJoin and GroupLeave are the actions passed to a GroupAuthorizer.
const (
	GroupJoin  = "join"
	GroupLeave = "leave"
)

// AuthResult is the identity an authenticator attaches to a new connection.
// Metadata keys and values use the same limits as SetMetadata.
type AuthResult struct {
	Identity string
	Metadata map[string]string
}

// Authenticator inspects the HTTP request before the upgrade.
// A non-nil error rejects the handshake with 401. The error text is logged
// and is not sent to the client. Do not put tokens or request URLs in it.
// A successful result replaces any identity stored with WithIdentity.
type Authenticator func(ctx context.Context, r *http.Request) (AuthResult, error)

// EventAuthorizer allows or refuses one inbound event.
// Return nil to continue to the handler. ErrUnauthorized becomes an error
// frame and the socket stays open. The authorizer runs on the read goroutine.
type EventAuthorizer func(ctx context.Context, c *Connection, event string) error

// GroupAuthorizer allows or refuses a membership change.
// action is GroupJoin or GroupLeave. The authorizer must not call Join,
// Leave, AddToGroup, or RemoveFromGroup.
type GroupAuthorizer func(ctx context.Context, c *Connection, group, action string) error

type identityCtxKey struct{}

// WithIdentity returns a context the handshake will read.
// Middleware uses it. An empty identity clears nothing by itself; the
// handshake treats a missing or empty value as unauthenticated.
func WithIdentity(ctx context.Context, identity string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, identityCtxKey{}, identity)
}

// IdentityFromContext returns the identity stored by WithIdentity.
// The bool is false when the value is missing or empty.
func IdentityFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	v, ok := ctx.Value(identityCtxKey{}).(string)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// AuthorizeEvents installs the event authorizer. A later call replaces it.
// A nil function panics. The authorizer runs before the event handler.
func (s *Server) AuthorizeEvents(fn EventAuthorizer) {
	if fn == nil {
		panic("gorealtime: nil event authorizer")
	}
	s.hookMu.Lock()
	s.eventAuth = fn
	s.hookMu.Unlock()
}

// AuthorizeGroups installs the group authorizer. A later call replaces it.
// A nil function panics.
func (s *Server) AuthorizeGroups(fn GroupAuthorizer) {
	if fn == nil {
		panic("gorealtime: nil group authorizer")
	}
	s.hookMu.Lock()
	s.groupAuth = fn
	s.hookMu.Unlock()
}

func (s *Server) eventAuthorizer() EventAuthorizer {
	s.hookMu.Lock()
	defer s.hookMu.Unlock()
	return s.eventAuth
}

func (s *Server) groupAuthorizer() GroupAuthorizer {
	s.hookMu.Lock()
	defer s.hookMu.Unlock()
	return s.groupAuth
}

// authenticate writes the HTTP response and returns false when the handshake
// must stop. Identity and metadata are safe to apply after a true result.
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (string, map[string]string, bool) {
	identity, _ := IdentityFromContext(r.Context())
	var meta map[string]string
	if s.opts.authenticator != nil {
		res, err := s.runAuthenticator(r)
		if err != nil {
			s.rejectAuth(err)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return "", nil, false
		}
		identity = res.Identity
		meta = res.Metadata
	}
	if identity != "" {
		if err := validateIdentity(identity); err != nil {
			s.rejectAuth(err)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return "", nil, false
		}
	}
	if err := validateMetaMap(meta); err != nil {
		s.rejectAuth(err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return "", nil, false
	}
	if s.opts.requireAuth && identity == "" {
		s.rejectAuth(ErrUnauthorized)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return "", nil, false
	}
	return identity, meta, true
}

func (s *Server) runAuthenticator(r *http.Request) (res AuthResult, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			s.opts.log.Error("auth.panic", "panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
			err = ErrUnauthorized
		}
	}()
	return s.opts.authenticator(r.Context(), r)
}

func (s *Server) rejectAuth(err error) {
	s.noteAuthFailed()
	s.opts.log.Info("auth.reject", "err", boundErr(err))
}

func (s *Server) authorizeEvent(c *Connection, name string) (err error) {
	fn := s.eventAuthorizer()
	if fn == nil {
		return nil
	}
	defer func() {
		if rec := recover(); rec != nil {
			s.opts.log.Error("auth.panic", "id", c.ID(), "event", name, "panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
			err = ErrUnauthorized
		}
	}()
	return fn(c.Context(), c, name)
}

func (s *Server) authorizeGroup(c *Connection, group, action string) (err error) {
	fn := s.groupAuthorizer()
	if fn == nil {
		return nil
	}
	defer func() {
		if rec := recover(); rec != nil {
			s.opts.log.Error("auth.panic", "id", c.ID(), "group", group, "panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
			err = ErrUnauthorized
		}
	}()
	ctx := context.Background()
	if c != nil {
		ctx = c.Context()
	}
	return fn(ctx, c, group, action)
}

func validateMetaMap(meta map[string]string) error {
	if len(meta) == 0 {
		return nil
	}
	if len(meta) > maxMetaKeys {
		return fmt.Errorf("%w: too many keys", ErrInvalidMetadata)
	}
	for k, v := range meta {
		if err := validateMeta(k, v); err != nil {
			return err
		}
	}
	return nil
}

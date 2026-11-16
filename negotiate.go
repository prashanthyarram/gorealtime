package gorealtime

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/prashanthyarram/gorealtime/internal/protocol"
	"github.com/prashanthyarram/gorealtime/internal/ws"
)

const maxPendingTickets = 4096

// TransportDescription is one transport a client may try, in preference order.
// Extensions lists WebSocket extensions this process can negotiate.
// It is empty for server-sent events and long polling. Those transports
// do not use permessage-deflate. HTTP content coding, when the client
// asks for it, is a property of that HTTP response, not of this list.
type TransportDescription struct {
	Name            string   `json:"name"`
	TransferFormats []string `json:"transferFormats"`
	Extensions      []string `json:"extensions,omitempty"`
}

// Negotiation is the body of a successful negotiate response.
// ConnectionToken is a short-lived transport ticket for one connection.
// It is not an application login, and the library does not issue, refresh,
// or store user credentials. Send it in X-Gorealtime-Connection-Token.
// Do not put it in a URL.
type Negotiation struct {
	ProtocolVersion     int                    `json:"protocolVersion"`
	ConnectionID        string                 `json:"connectionId"`
	ConnectionToken     string                 `json:"connectionToken"`
	ExpiresAt           time.Time              `json:"expiresAt"`
	AvailableTransports []TransportDescription `json:"availableTransports"`
}

type ticket struct {
	id       string
	token    string
	identity string
	meta     map[string]string
	expires  time.Time
	bound    bool
	sess     *pipeSession
	conn     *Connection
}

type ticketTable struct {
	mu      sync.Mutex
	byToken map[string]*ticket
}

func newTicketTable() *ticketTable {
	return &ticketTable{byToken: make(map[string]*ticket)}
}

func (t *ticketTable) put(tk *ticket, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sweepLocked(now)
	pending := 0
	for _, cur := range t.byToken {
		if !cur.bound {
			pending++
		}
	}
	if pending >= maxPendingTickets {
		return false
	}
	t.byToken[tk.token] = tk
	return true
}

func (t *ticketTable) sweepLocked(now time.Time) {
	for k, tk := range t.byToken {
		if !tk.bound && now.After(tk.expires) {
			delete(t.byToken, k)
		}
	}
}

// claim marks the token used for a transport. The ticket stays claimed
// if the caller does not release it.
func (t *ticketTable) claim(token string, now time.Time) (*ticket, int) {
	if token == "" {
		return nil, http.StatusUnauthorized
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	tk := t.byToken[token]
	if tk == nil || (!tk.bound && now.After(tk.expires)) {
		if tk != nil {
			delete(t.byToken, token)
		}
		return nil, http.StatusUnauthorized
	}
	if tk.bound {
		return nil, http.StatusConflict
	}
	tk.bound = true
	return tk, http.StatusOK
}

func (t *ticketTable) release(token string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tk := t.byToken[token]
	if tk == nil || tk.conn != nil {
		return
	}
	tk.bound = false
	tk.sess = nil
}

func (t *ticketTable) drop(token string) {
	if token == "" {
		return
	}
	t.mu.Lock()
	delete(t.byToken, token)
	t.mu.Unlock()
}

func (t *ticketTable) attach(token string, sess *pipeSession, c *Connection, identity string, meta map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tk := t.byToken[token]
	if tk == nil {
		return
	}
	tk.sess = sess
	tk.conn = c
	tk.identity = identity
	tk.meta = meta
}

// ticketView is a stable snapshot of the pointers on a ticket.
type ticketView struct {
	token    string
	bound    bool
	sess     *pipeSession
	conn     *Connection
	identity string
	meta     map[string]string
}

func (t *ticketTable) view(token string, now time.Time) (ticketView, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tk := t.byToken[token]
	if tk == nil {
		return ticketView{}, false
	}
	if !tk.bound && now.After(tk.expires) {
		delete(t.byToken, token)
		return ticketView{}, false
	}
	return ticketView{
		token:    tk.token,
		bound:    tk.bound,
		sess:     tk.sess,
		conn:     tk.conn,
		identity: tk.identity,
		meta:     tk.meta,
	}, true
}

func (s *Server) serveNegotiate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.writeCORS(w, r, false)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.originPermitted(r) {
		http.Error(w, "origin rejected", http.StatusForbidden)
		return
	}
	s.writeCORS(w, r, false)
	if !s.reg.acceptingNow() {
		http.Error(w, "server closed", http.StatusServiceUnavailable)
		return
	}
	// The body is optional and unused. A huge body is refused. A malformed
	// body is a protocol error, not a reason to try another transport.
	lr := io.LimitReader(r.Body, int64(s.opts.maxPayload)+1)
	buf, err := io.ReadAll(lr)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if len(buf) > s.opts.maxPayload {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}
	if len(buf) > 0 && !json.Valid(buf) {
		s.noteProtocolError()
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if _, _, ok := s.authenticate(w, r); !ok {
		return
	}
	if s.overCapacity() {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	id, err := newID()
	if err != nil {
		s.opts.log.Error("negotiate.id", "err", err.Error())
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	token, err := newToken()
	if err != nil {
		s.opts.log.Error("negotiate.token", "err", err.Error())
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	now := time.Now()
	tk := &ticket{id: id, token: token, expires: now.Add(s.opts.tokenTTL)}
	if !s.tickets.put(tk, now) {
		http.Error(w, "too many negotiations", http.StatusServiceUnavailable)
		return
	}
	body := Negotiation{
		ProtocolVersion:     protocol.Version,
		ConnectionID:        id,
		ConnectionToken:     token,
		ExpiresAt:           tk.expires.UTC(),
		AvailableTransports: s.offeredTransports(),
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		s.opts.log.Info("negotiate.write", "err", boundErr(err))
	}
}

func (s *Server) offeredTransports() []TransportDescription {
	order := []string{TransportWebSocket, TransportSSE, TransportLongPolling}
	out := make([]TransportDescription, 0, len(order))
	for _, name := range order {
		if !s.opts.transportEnabled(name) {
			continue
		}
		item := TransportDescription{
			Name:            name,
			TransferFormats: []string{"Text"},
		}
		if name == TransportWebSocket && s.opts.compression.enabled {
			item.Extensions = []string{"permessage-deflate"}
		}
		out = append(out, item)
	}
	return out
}

func (s *Server) originPermitted(r *http.Request) bool {
	return ws.OriginOK(r, s.opts.originPatterns, s.opts.insecureSkipOrigin)
}

func (s *Server) overCapacity() bool {
	if s.opts.maxConns <= 0 {
		return false
	}
	return s.ConnectionCount() >= s.opts.maxConns
}

func (s *Server) writeCORS(w http.ResponseWriter, r *http.Request, preflight bool) {
	origin := r.Header.Get("Origin")
	if origin == "" || !s.originPermitted(r) {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Vary", "Origin")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Expose-Headers", connectionTokenHeader)
	if preflight {
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, "+connectionTokenHeader+", Authorization")
		w.Header().Set("Access-Control-Max-Age", "600")
		w.WriteHeader(http.StatusNoContent)
	}
}

func newToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

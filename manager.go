package gorealtime

import "sync"

// registry is the process-local connection table.
//
// mu is the registry lock. Lock order is mu, then Connection.mu.
// Methods named *Locked require mu to be held by the caller.
type registry struct {
	mu        sync.Mutex
	accepting bool
	byID      map[string]*Connection
	byIdent   map[string]map[string]*Connection
}

func newRegistry() registry {
	return registry{
		accepting: true,
		byID:      make(map[string]*Connection),
		byIdent:   make(map[string]map[string]*Connection),
	}
}

// tryAdd inserts c if the server is still accepting and the id is free.
func (r *registry) tryAdd(c *Connection) bool {
	return r.insert(c, nil)
}

// insert registers c. track runs under the registry lock only after c is
// visible, so Shutdown's Wait cannot observe zero goroutines for a
// connection that was already published.
func (r *registry) insert(c *Connection, track func()) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepting {
		return false
	}
	if _, exists := r.byID[c.id]; exists {
		return false
	}
	r.byID[c.id] = c
	if track != nil {
		track()
	}
	return true
}

// remove drops c. It returns false if c was not the registered connection.
func (r *registry) remove(c *Connection) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur, ok := r.byID[c.id]
	if !ok || cur != c {
		return false
	}
	delete(r.byID, c.id)
	c.mu.Lock()
	ident := c.identity
	c.mu.Unlock()
	r.unlinkLocked(ident, c.id)
	return true
}

func (r *registry) acceptingNow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.accepting
}

func (r *registry) get(id string) (*Connection, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.byID[id]
	return c, ok
}

func (r *registry) list() []*Connection {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Connection, 0, len(r.byID))
	for _, c := range r.byID {
		out = append(out, c)
	}
	return out
}

func (r *registry) byIdentity(identity string) []*Connection {
	r.mu.Lock()
	defer r.mu.Unlock()
	set := r.byIdent[identity]
	if len(set) == 0 {
		return nil
	}
	out := make([]*Connection, 0, len(set))
	for _, c := range set {
		out = append(out, c)
	}
	return out
}

func (r *registry) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byID)
}

// setIdentity moves c to identity. The previous index entry is read under mu
// so two callers cannot leave c in both sets.
func (r *registry) setIdentity(c *Connection, identity string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byID[c.id]; !ok {
		return ErrConnectionClosed
	}
	c.mu.Lock()
	if c.state == StateClosed {
		c.mu.Unlock()
		return ErrConnectionClosed
	}
	prev := c.identity
	c.identity = identity
	c.mu.Unlock()
	if prev != identity {
		r.unlinkLocked(prev, c.id)
		r.linkLocked(identity, c.id, c)
	}
	return nil
}

// drain refuses new connections and returns the ones currently registered.
// The maps are empty afterward. Shutdown is the caller.
func (r *registry) drain() []*Connection {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.accepting = false
	out := make([]*Connection, 0, len(r.byID))
	for _, c := range r.byID {
		out = append(out, c)
	}
	r.byID = make(map[string]*Connection)
	r.byIdent = make(map[string]map[string]*Connection)
	return out
}

func (r *registry) linkLocked(identity, id string, c *Connection) {
	if identity == "" {
		return
	}
	set := r.byIdent[identity]
	if set == nil {
		set = make(map[string]*Connection)
		r.byIdent[identity] = set
	}
	set[id] = c
}

func (r *registry) unlinkLocked(identity, id string) {
	set := r.byIdent[identity]
	if set == nil {
		return
	}
	delete(set, id)
	if len(set) == 0 {
		delete(r.byIdent, identity)
	}
}

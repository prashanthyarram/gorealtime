package gorealtime

import "sync"

import "github.com/prashanth-yarram/gorealtime/internal/protocol"

// groupTable is process-local membership.
// mu is independent of the registry lock. Do not take the registry lock
// while holding mu. finish drops groups without holding the connection lock.
type groupTable struct {
	mu      sync.Mutex
	members map[string]map[string]*Connection
	byConn  map[string]map[string]struct{}
}

func newGroupTable() groupTable {
	return groupTable{
		members: make(map[string]map[string]*Connection),
		byConn:  make(map[string]map[string]struct{}),
	}
}

func validateGroup(name string) error {
	if err := protocol.ValidateEventName(name); err != nil {
		return ErrInvalidGroup
	}
	return nil
}

func (g *groupTable) add(c *Connection, name string) error {
	if err := validateGroup(name); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	c.mu.Lock()
	closed := c.state == StateClosed
	c.mu.Unlock()
	if closed {
		return ErrConnectionClosed
	}
	set := g.members[name]
	if set == nil {
		set = make(map[string]*Connection)
		g.members[name] = set
	}
	set[c.id] = c
	owned := g.byConn[c.id]
	if owned == nil {
		owned = make(map[string]struct{})
		g.byConn[c.id] = owned
	}
	owned[name] = struct{}{}
	return nil
}

func (g *groupTable) remove(c *Connection, name string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removeLocked(c.id, name)
}

func (g *groupTable) drop(c *Connection) {
	g.mu.Lock()
	defer g.mu.Unlock()
	owned := g.byConn[c.id]
	for name := range owned {
		g.removeLocked(c.id, name)
	}
	delete(g.byConn, c.id)
}

func (g *groupTable) removeLocked(id, name string) {
	if set := g.members[name]; set != nil {
		delete(set, id)
		if len(set) == 0 {
			delete(g.members, name)
		}
	}
	if owned := g.byConn[id]; owned != nil {
		delete(owned, name)
		if len(owned) == 0 {
			delete(g.byConn, id)
		}
	}
}

func (g *groupTable) snapshot(name string) []*Connection {
	g.mu.Lock()
	defer g.mu.Unlock()
	set := g.members[name]
	if len(set) == 0 {
		return nil
	}
	out := make([]*Connection, 0, len(set))
	for _, c := range set {
		out = append(out, c)
	}
	return out
}

func (g *groupTable) size(name string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.members[name])
}

func (g *groupTable) list(id string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	owned := g.byConn[id]
	if len(owned) == 0 {
		return nil
	}
	out := make([]string, 0, len(owned))
	for name := range owned {
		out = append(out, name)
	}
	return out
}

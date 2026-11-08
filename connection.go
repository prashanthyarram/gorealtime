package gorealtime

import (
	"sync"

	"github.com/prashanth-yarram/gorealtime/internal/ws"
)

// conn is one accepted client in this process.
// The type stays unexported until connection management is part of the public API.
type conn struct {
	id         string
	remoteAddr string
	sess       *ws.Session

	once sync.Once
}

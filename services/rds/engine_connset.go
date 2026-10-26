package rds

import (
	"net"
	"sync"
	"sync/atomic"
)

// connSet tracks a listener's live client connections so shutdown can sever them.
type connSet struct {
	m      sync.Map
	closed atomic.Bool
}

// add registers c; if the set is already closed it closes c and reports false.
// Store-then-check pairs with closeAll's flag-then-sweep: one side always sees the other.
func (s *connSet) add(c net.Conn) bool {
	s.m.Store(c, struct{}{})

	if s.closed.Load() {
		s.m.Delete(c)
		_ = c.Close()

		return false
	}

	return true
}

func (s *connSet) remove(c net.Conn) { s.m.Delete(c) }

func (s *connSet) closeAll() {
	s.closed.Store(true)

	s.m.Range(func(k, _ any) bool {
		if c, ok := k.(net.Conn); ok {
			_ = c.Close()
		}

		return true
	})
}

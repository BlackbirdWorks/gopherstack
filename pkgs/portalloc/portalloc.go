// Package portalloc provides a central port allocator for Gopherstack services.
// Services that expose real network endpoints (e.g., ElastiCache, Lambda function URLs)
// can request a dedicated port from the pool and release it when the resource is deleted.
package portalloc

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// ErrNoPortsAvailable is returned when the pool has no free ports.
var ErrNoPortsAvailable = errors.New("no ports available in range")

// ErrPortNotAllocated is returned when trying to release a port that was not allocated.
var ErrPortNotAllocated = errors.New("port not allocated")

// ErrInvalidRange is returned when the port range is invalid.
var ErrInvalidRange = errors.New("invalid port range: start must be ≥ 1 and end > start")

// Allocator manages a pool of ports within a configurable range.
// It is safe for concurrent use.
type Allocator struct {
	used  map[int]string
	mu    *lockmetrics.RWMutex
	start int
	end   int
	next  int
}

// New creates a new Allocator for the half-open range [start, end).
// start must be ≥ 1 and end must be > start.
func New(start, end int) (*Allocator, error) {
	if start < 1 || end <= start {
		return nil, fmt.Errorf("[%d, %d): %w", start, end, ErrInvalidRange)
	}

	return &Allocator{
		start: start,
		end:   end,
		next:  start,
		used:  make(map[int]string),
		mu:    lockmetrics.New("portalloc"),
	}, nil
}

// Acquire returns the next free port in the range and associates it with label.
// Returns ErrNoPortsAvailable when the pool is exhausted.
func (a *Allocator) Acquire(label string) (int, error) {
	a.mu.Lock("Acquire")
	defer a.mu.Unlock()

	for range a.end - a.start {
		port := a.next
		if _, taken := a.used[port]; !taken {
			a.used[port] = label
			a.advanceNext(port)

			return port, nil
		}
		a.advanceNext(port)
	}

	return 0, ErrNoPortsAvailable
}

// Release returns a previously allocated port back to the pool.
// Returns ErrPortNotAllocated if the port was not allocated.
func (a *Allocator) Release(port int) error {
	a.mu.Lock("Release")
	defer a.mu.Unlock()

	if _, ok := a.used[port]; !ok {
		return fmt.Errorf("%w: %d", ErrPortNotAllocated, port)
	}

	delete(a.used, port)

	return nil
}

// ErrPortAlreadyReserved is returned by Reserve when port is already marked
// used (by a prior Reserve or Acquire call).
var ErrPortAlreadyReserved = errors.New("port already reserved or allocated")

// Reserve permanently marks port as unavailable in the pool, associating it
// with label, without actually binding anything -- for services that bind a
// fixed port of their own outside this allocator entirely (e.g. a
// protocol-conventional default port), so Acquire never hands that same
// port to a different caller and causes a surprise address-in-use failure
// later. Intended to be called once at startup, before any Acquire calls.
//
// A port outside [start, end) is a no-op (nil, nil): Acquire never
// considers it anyway, so there is nothing to protect. Returns
// ErrPortAlreadyReserved if port is already marked used.
func (a *Allocator) Reserve(port int, label string) error {
	a.mu.Lock("Reserve")
	defer a.mu.Unlock()

	if port < a.start || port >= a.end {
		return nil
	}

	if _, taken := a.used[port]; taken {
		return fmt.Errorf("%w: %d", ErrPortAlreadyReserved, port)
	}

	a.used[port] = label

	return nil
}

func (a *Allocator) advanceNext(port int) {
	a.next = port + 1
	if a.next >= a.end {
		a.next = a.start
	}
}

// IsAllocated reports whether port is currently allocated.
func (a *Allocator) IsAllocated(port int) bool {
	a.mu.RLock("IsAllocated")
	defer a.mu.RUnlock()

	_, ok := a.used[port]

	return ok
}

// Allocated returns a snapshot of all currently allocated ports and their labels.
func (a *Allocator) Allocated() map[int]string {
	a.mu.RLock("Allocated")
	defer a.mu.RUnlock()

	out := make(map[int]string, len(a.used))
	maps.Copy(out, a.used)

	return out
}

// Available returns the number of unallocated ports in the range.
func (a *Allocator) Available() int {
	a.mu.RLock("Available")
	defer a.mu.RUnlock()

	return (a.end - a.start) - len(a.used)
}

// IsListening performs a TCP health check on the given address to detect
// zombie listeners — ports that are allocated but no longer serving connections.
// Returns true if a listener is detected.
func IsListening(addr string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var d net.Dialer

	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}

	_ = conn.Close()

	return true
}

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
	"sync"
	"time"
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
	start int
	end   int
	mu    sync.Mutex
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
		used:  make(map[int]string),
	}, nil
}

// Acquire returns the next free port in the range and associates it with label.
// Returns ErrNoPortsAvailable when the pool is exhausted.
func (a *Allocator) Acquire(label string) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for port := a.start; port < a.end; port++ {
		if _, taken := a.used[port]; !taken {
			a.used[port] = label

			return port, nil
		}
	}

	return 0, ErrNoPortsAvailable
}

// Release returns a previously allocated port back to the pool.
// Returns ErrPortNotAllocated if the port was not allocated.
func (a *Allocator) Release(port int) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.used[port]; !ok {
		return fmt.Errorf("%w: %d", ErrPortNotAllocated, port)
	}

	delete(a.used, port)

	return nil
}

// IsAllocated reports whether port is currently allocated.
func (a *Allocator) IsAllocated(port int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, ok := a.used[port]

	return ok
}

// Allocated returns a snapshot of all currently allocated ports and their labels.
func (a *Allocator) Allocated() map[int]string {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := make(map[int]string, len(a.used))
	maps.Copy(out, a.used)

	return out
}

// Available returns the number of unallocated ports in the range.
func (a *Allocator) Available() int {
	a.mu.Lock()
	defer a.mu.Unlock()

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

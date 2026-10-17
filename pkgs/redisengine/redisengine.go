// Package redisengine runs an in-process Redis-protocol server (miniredis)
// on an allocator-provided or ephemeral port.
package redisengine

import (
	"fmt"
	"net"
	"strconv"

	"github.com/alicebob/miniredis/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

// Engine is a running in-process Redis-protocol server.
type Engine struct {
	Mini          *miniredis.Miniredis
	alloc         *portalloc.Allocator
	Host          string
	Port          int
	AllocatedPort int
}

// Start launches a server. With a non-nil allocator the port is acquired from it
// under label and bound on all interfaces; otherwise an ephemeral loopback port is used.
func Start(alloc *portalloc.Allocator, label string) (*Engine, error) {
	mr := miniredis.NewMiniRedis()
	e := &Engine{Mini: mr, alloc: alloc}

	if alloc != nil {
		port, err := alloc.Acquire(label)
		if err != nil {
			return nil, fmt.Errorf("allocate miniredis port: %w", err)
		}

		if startErr := mr.StartAddr(fmt.Sprintf("0.0.0.0:%d", port)); startErr != nil {
			_ = alloc.Release(port)

			return nil, fmt.Errorf("start miniredis on port %d: %w", port, startErr)
		}

		e.AllocatedPort = port
	} else if err := mr.Start(); err != nil {
		return nil, fmt.Errorf("start miniredis: %w", err)
	}

	addr := mr.Server().Addr()
	e.Port = addr.Port
	e.Host = addr.IP.String()

	if addr.IP.IsUnspecified() {
		e.Host = "127.0.0.1"
	}

	return e, nil
}

// Addr returns the connectable host:port.
func (e *Engine) Addr() string { return net.JoinHostPort(e.Host, strconv.Itoa(e.Port)) }

// Close stops the server and releases its allocated port. Nil-safe and idempotent.
func (e *Engine) Close() {
	if e == nil || e.Mini == nil {
		return
	}

	e.Mini.Close()
	e.Mini = nil

	if e.alloc != nil && e.AllocatedPort > 0 {
		_ = e.alloc.Release(e.AllocatedPort)
		e.AllocatedPort = 0
	}
}

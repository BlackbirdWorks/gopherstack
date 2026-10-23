// Package redisengine runs an in-process Redis-protocol server (miniredis)
// on an allocator-provided or ephemeral port.
package redisengine

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"

	"github.com/alicebob/miniredis/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

// Engine is a running in-process Redis-protocol server.
type Engine struct {
	Mini  *miniredis.Miniredis
	alloc *portalloc.Allocator
	// Front relays clients to Mini behind an admit check; nil for ungated engines.
	Front *sgfilter.Proxy
	// Gate decides Front's admissions; deny-all until Set.
	Gate          *Gate
	Host          string
	Port          int
	AllocatedPort int
}

// Start launches a server. With a non-nil allocator the port is acquired from it
// under label and bound on all interfaces; otherwise an ephemeral loopback port is used.
func Start(alloc *portalloc.Allocator, label string) (*Engine, error) {
	return StartTLS(alloc, label, nil)
}

// StartTLS is Start serving TLS with cfg; a nil cfg serves plaintext.
func StartTLS(alloc *portalloc.Allocator, label string, cfg *tls.Config) (*Engine, error) {
	mr := miniredis.NewMiniRedis()
	e := &Engine{Mini: mr, alloc: alloc}

	if alloc != nil {
		port, err := alloc.Acquire(label)
		if err != nil {
			return nil, fmt.Errorf("allocate miniredis port: %w", err)
		}

		if startErr := listenAddr(mr, fmt.Sprintf("0.0.0.0:%d", port), cfg); startErr != nil {
			_ = alloc.Release(port)

			return nil, fmt.Errorf("start miniredis on port %d: %w", port, startErr)
		}

		e.AllocatedPort = port
	} else if err := listenAddr(mr, "127.0.0.1:0", cfg); err != nil {
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

func listenAddr(mr *miniredis.Miniredis, addr string, cfg *tls.Config) error {
	if cfg != nil {
		return mr.StartAddrTLS(addr, cfg)
	}

	return mr.StartAddr(addr)
}

// Gate holds a connection check installed after the engine starts, once its owner can name the target;
// until then every client is refused.
type Gate struct {
	p atomic.Pointer[sgfilter.Admitter]
}

// Set installs the check.
func (g *Gate) Set(a sgfilter.Admitter) { g.p.Store(&a) }

// Admit implements sgfilter.Admitter.
func (g *Gate) Admit(remote net.Addr) bool {
	a := g.p.Load()

	return a != nil && *a != nil && (*a)(remote)
}

// StartGated is Start with the client-facing port served by a relay in front of a loopback-only server, so
// Gate can refuse clients. The allocator port (or an ephemeral loopback one) is the relay's.
func StartGated(alloc *portalloc.Allocator, label string) (*Engine, error) {
	mr := miniredis.NewMiniRedis()
	if err := mr.StartAddr("127.0.0.1:0"); err != nil {
		return nil, fmt.Errorf("start miniredis: %w", err)
	}

	e := &Engine{Mini: mr, alloc: alloc, Gate: &Gate{}}
	bind := "127.0.0.1:0"

	if alloc != nil {
		port, err := alloc.Acquire(label)
		if err != nil {
			mr.Close()

			return nil, fmt.Errorf("allocate redis port: %w", err)
		}

		e.AllocatedPort, bind = port, net.JoinHostPort("0.0.0.0", strconv.Itoa(port))
	}

	front, err := sgfilter.Front(context.Background(), bind, mr.Addr(), e.Gate.Admit)
	if err != nil {
		e.Close()

		return nil, fmt.Errorf("start redis relay: %w", err)
	}

	e.Front = front
	e.setAddr(front.Addr())

	return e, nil
}

func (e *Engine) setAddr(addr net.Addr) {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return
	}

	e.Port, e.Host = tcp.Port, tcp.IP.String()
	if tcp.IP.IsUnspecified() {
		e.Host = "127.0.0.1"
	}
}

// Addr returns the connectable host:port.
func (e *Engine) Addr() string { return net.JoinHostPort(e.Host, strconv.Itoa(e.Port)) }

// Close stops the server and releases its allocated port. Nil-safe and idempotent.
func (e *Engine) Close() {
	if e == nil || e.Mini == nil {
		return
	}

	if e.Front != nil {
		e.Front.Close()
		e.Front = nil
	}

	e.Mini.Close()
	e.Mini = nil

	if e.alloc != nil && e.AllocatedPort > 0 {
		_ = e.alloc.Release(e.AllocatedPort)
		e.AllocatedPort = 0
	}
}

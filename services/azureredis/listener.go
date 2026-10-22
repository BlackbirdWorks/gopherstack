package azureredis

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

// front is a client-facing listener relaying to a cache's loopback Redis server.
type front struct {
	proxy *sgfilter.Proxy
	alloc *portalloc.Allocator
	port  int
}

// openFront listens on a port from alloc (all interfaces) or an ephemeral loopback port, serving TLS when cfg is
// non-nil, and relays accepted clients to upstream.
func openFront(alloc *portalloc.Allocator, label, upstream string, cfg *tls.Config) (*front, error) {
	port, err := portalloc.AcquireFrom(alloc, label)
	if err != nil {
		return nil, fmt.Errorf("allocate redis port: %w", err)
	}

	bind := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	if alloc != nil {
		bind = net.JoinHostPort("0.0.0.0", strconv.Itoa(port))
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", bind)
	if err != nil {
		portalloc.ReleaseTo(alloc, port)

		return nil, fmt.Errorf("listen on %s: %w", bind, err)
	}

	if cfg != nil {
		ln = tls.NewListener(ln, cfg)
	}

	p := sgfilter.NewProxy(ln, upstream, nil, nil)
	p.Start()

	return &front{proxy: p, alloc: alloc, port: port}, nil
}

func (f *front) close() {
	if f == nil {
		return
	}

	f.proxy.Close()
	portalloc.ReleaseTo(f.alloc, f.port)
}

func (f *front) portOr0() int {
	if f == nil {
		return 0
	}

	return f.port
}

// minTLS is a live-updatable minimum TLS version.
type minTLS struct{ v atomic.Uint32 }

func tlsVersion(s string) uint16 {
	switch s {
	case "1.0":
		return tls.VersionTLS10
	case "1.1":
		return tls.VersionTLS11
	default:
		return tls.VersionTLS12
	}
}

func (m *minTLS) set(s string) { m.v.Store(uint32(tlsVersion(s))) }

// config serves cert with the version floor read at handshake time, so a minimumTlsVersion change takes effect
// without restarting the listener.
func (m *minTLS) config(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   uint16(m.v.Load()), //nolint:gosec // only ever set from tlsVersion
			}, nil
		},
	}
}

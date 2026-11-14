package app

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"
)

const fixedPortPoolSpan = 10

// fixedPortPools returns a probed-free pool and the ports to configure services with: distinct
// ports inside the pool when inPool, else the given ports (which must not fall in the test range).
func fixedPortPools(tb testing.TB, inPool bool, ports ...int) (*portalloc.Allocator, []int) {
	tb.Helper()

	alloc := portalloctest.New(tb, fixedPortPoolSpan)
	if !inPool {
		return alloc, ports
	}

	return alloc, poolPorts(tb, alloc, len(ports))
}

// fixedPortPool is fixedPortPools for a single port.
func fixedPortPool(tb testing.TB, inPool bool, port int) (*portalloc.Allocator, int) {
	tb.Helper()

	alloc, ports := fixedPortPools(tb, inPool, port)

	return alloc, ports[0]
}

// poolPorts returns n distinct ports inside alloc's range, left unallocated.
func poolPorts(tb testing.TB, alloc *portalloc.Allocator, n int) []int {
	tb.Helper()

	ports := make([]int, 0, n)

	for range n {
		p, err := alloc.Acquire("probe")
		if err != nil {
			tb.Fatalf("acquire: %v", err)
		}

		ports = append(ports, p)
	}

	for _, p := range ports {
		if err := alloc.Release(p); err != nil {
			tb.Fatalf("release: %v", err)
		}
	}

	return ports
}

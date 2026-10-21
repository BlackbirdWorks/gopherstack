package portalloc

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// ephemeralAttempts bounds the OS probes spent finding a port nobody in the process holds.
const ephemeralAttempts = 64

type ephemeralSet struct {
	held map[int]struct{}
	mu   *lockmetrics.RWMutex
}

var ephemeral = &ephemeralSet{ //nolint:gochecknoglobals // process-wide: uniqueness must span every backend
	held: make(map[int]struct{}),
	mu:   lockmetrics.New("portalloc.ephemeral"),
}

// AcquireFrom takes a port from alloc when non-nil, else a process-unique ephemeral loopback port.
// Pair with ReleaseTo.
func AcquireFrom(alloc *Allocator, label string) (int, error) {
	if alloc != nil {
		return alloc.Acquire(label)
	}

	return AcquireEphemeral()
}

// ReleaseTo returns a port taken by AcquireFrom. Zero and unknown ports are ignored.
func ReleaseTo(alloc *Allocator, port int) {
	if port == 0 {
		return
	}

	if alloc != nil {
		_ = alloc.Release(port)

		return
	}

	ReleaseEphemeral(port)
}

// AcquireEphemeral probes the OS for a free loopback port and records it process-wide, so no other
// caller is handed the same number until ReleaseEphemeral.
func AcquireEphemeral() (int, error) {
	for range ephemeralAttempts {
		p, err := probeFreePort()
		if err != nil {
			return 0, err
		}

		ephemeral.mu.Lock("AcquireEphemeral")
		_, taken := ephemeral.held[p]

		if !taken {
			ephemeral.held[p] = struct{}{}
		}

		ephemeral.mu.Unlock()

		if !taken {
			return p, nil
		}
	}

	return 0, fmt.Errorf("find free port: %w", ErrNoPortsAvailable)
}

// ReleaseEphemeral makes a port from AcquireEphemeral reusable.
func ReleaseEphemeral(port int) {
	ephemeral.mu.Lock("ReleaseEphemeral")
	delete(ephemeral.held, port)
	ephemeral.mu.Unlock()
}

func probeFreePort() (int, error) {
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}

	defer func() { _ = l.Close() }()

	a, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("find free port: %w", io.ErrUnexpectedEOF)
	}

	return a.Port, nil
}

package portalloc

import (
	"context"
	"fmt"
	"net"
)

// ListenTCP binds ":port" and returns the listener with the port actually bound.
// Port 0 asks the OS for an ephemeral port, so callers never pre-pick one.
func ListenTCP(ctx context.Context, port int) (net.Listener, int, error) {
	var lc net.ListenConfig

	ln, err := lc.Listen(ctx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, 0, fmt.Errorf("listen tcp port %d: %w", port, err)
	}

	if addr, ok := ln.Addr().(*net.TCPAddr); ok {
		return ln, addr.Port, nil
	}

	return ln, port, nil
}

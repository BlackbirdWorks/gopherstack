// Package dnstest starts embedded pkgs/dns servers on free ports for tests.
package dnstest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
)

const maxAttempts = 10

var errUnexpectedAddr = errors.New("unexpected local addr type")

// Start runs a DNS server on a free 127.0.0.1 port, retrying when the UDP or
// TCP side of the chosen port is already taken. Stop is registered in t.Cleanup;
// the returned cancel func cancels the server context and is safe to call twice.
func Start(t *testing.T, cfg gopherDNS.Config) (*gopherDNS.Server, string, context.CancelFunc) {
	t.Helper()

	return startWith(t, cfg, freeUDPAddr)
}

func startWith(
	t *testing.T,
	cfg gopherDNS.Config,
	pick func(*testing.T) (string, error),
) (*gopherDNS.Server, string, context.CancelFunc) {
	t.Helper()

	for range maxAttempts {
		addr, err := pick(t)
		if err != nil {
			t.Fatalf("dnstest: pick port: %v", err)
		}

		cfg.ListenAddr = addr

		srv, err := gopherDNS.New(cfg)
		if err != nil {
			t.Fatalf("dnstest: new server: %v", err)
		}

		ctx, cancel := context.WithCancel(t.Context())

		if err = srv.Start(ctx); err != nil {
			cancel()
			_ = srv.Stop()

			if errors.Is(err, syscall.EADDRINUSE) {
				continue
			}

			t.Fatalf("dnstest: start server: %v", err)
		}

		t.Cleanup(func() {
			cancel()
			_ = srv.Stop()
		})

		return srv, addr, cancel
	}

	t.Fatalf("dnstest: no free port after %d attempts", maxAttempts)

	return nil, "", nil
}

// FreeAddr returns a 127.0.0.1 address whose port was free on both UDP and TCP
// at call time, for code that starts its own DNS server (still racy, but narrower).
func FreeAddr(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig

	for range maxAttempts {
		l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("dnstest: listen tcp: %v", err)
		}

		addr := l.Addr().String()
		pc, err := lc.ListenPacket(t.Context(), "udp", addr)
		_ = l.Close()

		if err != nil {
			continue
		}

		_ = pc.Close()

		return addr
	}

	t.Fatalf("dnstest: no port free on both udp and tcp")

	return ""
}

func freeUDPAddr(t *testing.T) (string, error) {
	t.Helper()

	var lc net.ListenConfig

	pc, err := lc.ListenPacket(t.Context(), "udp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("listen udp: %w", err)
	}

	defer func() { _ = pc.Close() }()

	udpAddr, ok := pc.LocalAddr().(*net.UDPAddr)
	if !ok {
		return "", errUnexpectedAddr
	}

	return fmt.Sprintf("127.0.0.1:%d", udpAddr.Port), nil
}

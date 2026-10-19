package dnstest

import (
	"context"
	"testing"

	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
)

func StartWith(
	t *testing.T,
	cfg gopherDNS.Config,
	pick func(*testing.T) (string, error),
) (*gopherDNS.Server, string, context.CancelFunc) {
	t.Helper()

	return startWith(t, cfg, pick)
}

func FreeUDPAddr(t *testing.T) (string, error) {
	t.Helper()

	return freeUDPAddr(t)
}

package ociapigateway_test

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGatewayIPAddresses(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.mkGateway(t, "gw-a")
	b := e.mkGateway(t, "gw-b")

	pool := netip.MustParsePrefix("198.18.0.0/15")
	seen := map[string]bool{}

	for _, g := range []string{*a.Id, *b.Id} {
		got := e.mkGatewayByID(t, g)
		require.Len(t, got.IpAddresses, 1)

		ip := *got.IpAddresses[0].IpAddress
		assert.True(t, pool.Contains(netip.MustParseAddr(ip)))
		assert.False(t, seen[ip], "addresses are unique")

		seen[ip] = true
	}
}

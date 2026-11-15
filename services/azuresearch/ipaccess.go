package azuresearch

import (
	"net"
	"net/netip"
	"strings"
)

// ipAllowed reports whether the peer address matches one of the IPv4 addresses or CIDR ranges; no rules allow everyone.
func ipAllowed(remote string, rules []string) bool {
	if len(rules) == 0 {
		return true
	}

	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}

	addr, err := netip.ParseAddr(strings.TrimSpace(host))
	if err != nil {
		return false
	}

	addr = addr.Unmap()

	for _, r := range rules {
		if p, perr := netip.ParsePrefix(r); perr == nil && p.Contains(addr) {
			return true
		}

		if a, aerr := netip.ParseAddr(r); aerr == nil && a == addr {
			return true
		}
	}

	return false
}

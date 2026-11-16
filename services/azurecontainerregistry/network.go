package azurecontainerregistry

import (
	"net"
	"net/http"
	"net/netip"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// NetworkRules is the public-endpoint firewall of a registry. Disabled public access overrides every IP rule
// (Learn: container-registry-access-selected-networks).
type NetworkRules struct {
	Allow          []string `json:"allow,omitempty"`
	PublicDisabled bool     `json:"publicDisabled,omitempty"`
	DefaultDeny    bool     `json:"defaultDeny,omitempty"`
}

func parseRule(s string) (netip.Prefix, bool) {
	if p, err := netip.ParsePrefix(s); err == nil {
		return p.Masked(), p.Addr().Is4()
	}

	a, err := netip.ParseAddr(s)
	if err != nil || !a.Is4() {
		return netip.Prefix{}, false
	}

	return netip.PrefixFrom(a, a.BitLen()), true
}

// SetNetworkRules replaces the registry's firewall.
func (b *InMemoryBackend) SetNetworkRules(name string, n NetworkRules) error {
	for _, rule := range n.Allow {
		if _, ok := parseRule(rule); !ok {
			return ErrInvalidNetworkRule
		}
	}

	b.mu.Lock("SetNetworkRules")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(name)]
	if !ok {
		return ErrRegistryNotFound
	}

	r.Network = n

	return nil
}

// clientAllowed reports whether the request's source address may reach the registry's public endpoint.
func (b *InMemoryBackend) clientAllowed(name string, r *http.Request) bool {
	b.mu.RLock("clientAllowed")
	defer b.mu.RUnlock()

	reg, ok := b.regs[strs.Fold(name)]
	if !ok {
		return true
	}

	n := reg.Network

	switch {
	case n.PublicDisabled:
		return false
	case !n.DefaultDeny:
		return true
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}

	for _, rule := range n.Allow {
		if p, valid := parseRule(rule); valid && p.Contains(addr.Unmap()) {
			return true
		}
	}

	return false
}

func writeNetworkDenied(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	writeRegistryError(w, http.StatusForbidden, "DENIED",
		"client with IP '"+host+"' is not allowed access. Refer https://aka.ms/acr/firewall to grant access.")
}

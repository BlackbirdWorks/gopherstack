package azurearm

import (
	"net/http"
	"net/netip"
	"strings"
)

const (
	apimCIPFilter     = "ip-filter"
	apimCAction       = "action"
	apimCFrom         = "from"
	apimCAddress      = "address"
	apimCAddressRange = "address-range"
	apimIPAllow       = "allow"
	apimIPForbid      = "forbid"
)

func apimParseAddr(s string) (netip.Addr, bool) {
	a, err := netip.ParseAddr(strings.TrimSpace(s))

	return a.Unmap(), err == nil && a.Zone() == ""
}

// validateIPFilter checks the ip-filter statement of learn.microsoft.com/azure/api-management/ip-filter-policy:
// action allow or forbid and at least one address or address-range, whose ends are one address family in order.
func validateIPFilter(n *polNode) error {
	if a := n.Attrs[apimCAction]; a != apimIPAllow && a != apimIPForbid {
		return polErr(n, "The attribute 'action' must be allow or forbid.")
	}

	if len(n.Kids) == 0 {
		return polErr(n, "At least one address or address-range element is required.")
	}

	for _, k := range n.Kids {
		if k.Name == apimCAddress {
			if _, ok := apimParseAddr(k.Text); !ok {
				return polErr(k, "The address '%s' is not a valid IP address.", strings.TrimSpace(k.Text))
			}

			continue
		}

		from, ok1 := apimParseAddr(k.Attrs[apimCFrom])
		to, ok2 := apimParseAddr(k.Attrs["to"])

		if !ok1 || !ok2 || from.Is4() != to.Is4() || from.Compare(to) > 0 {
			return polErr(
				k,
				"The address-range needs valid from and to addresses of one family with from not above to.",
			)
		}
	}

	return nil
}

func apimIPMatches(n *polNode, ip netip.Addr) bool {
	for _, k := range n.Kids {
		if k.Name == apimCAddress {
			if a, ok := apimParseAddr(k.Text); ok && a == ip {
				return true
			}

			continue
		}

		from, _ := apimParseAddr(k.Attrs[apimCFrom])
		to, _ := apimParseAddr(k.Attrs["to"])

		if from.Is4() == ip.Is4() && from.Compare(ip) <= 0 && ip.Compare(to) <= 0 {
			return true
		}
	}

	return false
}

func (g *gwState) ipFilter(n *polNode) {
	ip, ok := apimParseAddr(hostOnly(g.r.RemoteAddr))
	if !ok {
		g.fail(http.StatusForbidden, "The client IP address could not be determined.")

		return
	}

	if apimIPMatches(n, ip) == (n.Attrs[apimCAction] == apimIPAllow) {
		return
	}

	g.fail(http.StatusForbidden, "Request originating from IP address "+ip.String()+" is blocked.")
}

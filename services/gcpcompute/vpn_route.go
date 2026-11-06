package gcpcompute

import (
	"net/netip"

	computeapi "google.golang.org/api/compute/v1"
)

func (b *InMemoryBackend) resolveNextHopTunnelLocked(project string, in, out *computeapi.Route) *APIError {
	r, err := refLocated(in.NextHopVpnTunnel, collVPNTunnels, scopeRegion, project, "")
	if err != nil {
		return errField("resource.nextHopVpnTunnel", in.NextHopVpnTunnel, "The URL is malformed.")
	}

	t, ok := b.vpnTunnels[key(r.project, r.loc, r.name)]
	if !ok {
		return errNotFound(relPath(r.project, scopeRegion, r.loc, collVPNTunnels, r.name))
	}

	out.NextHopVpnTunnel = t.SelfLink

	return nil
}

// resolveNextHopILBLocked accepts a forwarding rule URL or the IP address of an INTERNAL forwarding rule.
func (b *InMemoryBackend) resolveNextHopILBLocked(project string, in, out *computeapi.Route) *APIError {
	if ip, err := netip.ParseAddr(in.NextHopIlb); err == nil {
		for _, fr := range b.fwdRules {
			if fr.IPAddress == ip.String() && fr.LoadBalancingScheme == schemeInternal {
				out.NextHopIlb = ip.String()

				return nil
			}
		}

		return errField("resource.nextHopIlb", in.NextHopIlb, "No INTERNAL forwarding rule has this IP address.")
	}

	r, err := refLocated(in.NextHopIlb, collFwdRules, scopeRegion, project, "")
	if err != nil {
		return errField("resource.nextHopIlb", in.NextHopIlb, "The URL is malformed.")
	}

	fr, ok := b.fwdRules[key(r.project, r.loc, r.name)]
	if !ok {
		return errNotFound(relPath(r.project, scopeRegion, r.loc, collFwdRules, r.name))
	}

	if fr.LoadBalancingScheme != schemeInternal {
		return errField("resource.nextHopIlb", in.NextHopIlb, "The forwarding rule must have the INTERNAL scheme.")
	}

	out.NextHopIlb = fr.SelfLink

	return nil
}

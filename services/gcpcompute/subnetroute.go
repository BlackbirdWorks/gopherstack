package gcpcompute

import (
	"crypto/sha256"
	"encoding/hex"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	routeTypeSubnet = "SUBNET"
	routeActive     = "ACTIVE"
)

func isSubnetRoute(r *computeapi.Route) bool { return r.RouteType == routeTypeSubnet }

// subnetRouteName derives a stable default-route-<16 hex> name per subnetwork range.
func subnetRouteName(subnetSelf, rangeName string) string {
	sum := sha256.Sum256([]byte(subnetSelf + "|" + rangeName))

	return sysRoutePrefix + hex.EncodeToString(sum[:8])
}

func subnetRouteDesc(cidr string) string {
	return "Default local route to the subnetwork " + cidr + "."
}

// syncSubnetRoutesLocked makes the network's subnet routes match its subnetworks: one per primary and secondary range.
func (b *InMemoryBackend) syncSubnetRoutesLocked(project string, n *computeapi.Network) {
	want := map[string]string{}

	for _, s := range b.subnets {
		if s.Network != n.SelfLink {
			continue
		}

		want[subnetRouteName(s.SelfLink, "")] = s.IpCidrRange

		for _, sec := range s.SecondaryIpRanges {
			want[subnetRouteName(s.SelfLink, sec.RangeName)] = sec.IpCidrRange
		}
	}

	for k, r := range b.routes {
		if r.Network != n.SelfLink || !isSubnetRoute(r) {
			continue
		}

		if cidr, ok := want[r.Name]; !ok {
			delete(b.routes, k)
		} else if r.DestRange != cidr {
			r.DestRange, r.Description = cidr, subnetRouteDesc(cidr)
		}
	}

	for name, cidr := range want {
		if _, ok := b.routes[key(project, name)]; ok {
			continue
		}

		b.routes[key(project, name)] = &computeapi.Route{
			Kind: "compute#route", Id: newID(), CreationTimestamp: b.stamp(), Name: name, Network: n.SelfLink,
			Description: subnetRouteDesc(cidr), DestRange: cidr, NextHopNetwork: n.SelfLink,
			SelfLink: globalLink(project, "routes", name), RouteType: routeTypeSubnet, RouteStatus: routeActive,
		}
	}
}

func (b *InMemoryBackend) syncNetworkRoutesBySelfLinkLocked(project, network string) {
	for _, n := range b.networks {
		if n.SelfLink == network {
			b.syncSubnetRoutesLocked(project, n)

			return
		}
	}
}

func viewRoute(r *computeapi.Route) *computeapi.Route {
	out := clone(r)
	out.ForceSendFields = []string{"Priority"}

	return out
}

package gcpcompute

import (
	"net/netip"
	"slices"
	"sort"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	peerActive     = "ACTIVE"
	peerInactive   = "INACTIVE"
	maxPeerings    = 25
	stackIPv4Only  = "IPV4_ONLY"
	stackIPv4IPv6  = "IPV4_IPV6"
	stratIndep     = "INDEPENDENT"
	dirIncoming    = "INCOMING"
	dirOutgoing    = "OUTGOING"
	peerRouteSub   = "SUBNET_PEERING_ROUTE"
	peerRouteStat  = "STATIC_PEERING_ROUTE"
	peerDetailOn   = "Connected."
	peerDetailWait = "Waiting for the peer network to create the matching peering."
)

type addPeeringRequest struct {
	NetworkPeering   *computeapi.NetworkPeering `json:"networkPeering"`
	Name             string                     `json:"name"`
	PeerNetwork      string                     `json:"peerNetwork"`
	AutoCreateRoutes bool                       `json:"autoCreateRoutes"`
}

type updatePeeringRequest struct {
	NetworkPeering *computeapi.NetworkPeering `json:"networkPeering"`
}

type removePeeringRequest struct {
	Name string `json:"name"`
}

func (b *InMemoryBackend) networkBySelfLinkLocked(self string) *computeapi.Network {
	for _, n := range b.networks {
		if n.SelfLink == self {
			return n
		}
	}

	return nil
}

func peeringTo(n *computeapi.Network, peerSelf string) *computeapi.NetworkPeering {
	for _, p := range n.Peerings {
		if p.Network == peerSelf {
			return p
		}
	}

	return nil
}

// peerSideLocked returns the peer network and its matching peering, or nils when absent.
func (b *InMemoryBackend) peerSideLocked(n *computeapi.Network, p *computeapi.NetworkPeering) (
	*computeapi.Network, *computeapi.NetworkPeering,
) {
	peer := b.networkBySelfLinkLocked(p.Network)
	if peer == nil {
		return nil, nil
	}

	return peer, peeringTo(peer, n.SelfLink)
}

func (b *InMemoryBackend) activePeersLocked(n *computeapi.Network) []*computeapi.Network {
	var out []*computeapi.Network

	for _, p := range n.Peerings {
		if peer, back := b.peerSideLocked(n, p); back != nil {
			out = append(out, peer)
		}
	}

	return out
}

func (b *InMemoryBackend) peeringsViewLocked(n *computeapi.Network) []*computeapi.NetworkPeering {
	out := make([]*computeapi.NetworkPeering, 0, len(n.Peerings))

	for _, p := range n.Peerings {
		v := clone(p)
		v.ForceSendFields = []string{"ExchangeSubnetRoutes", "ExportCustomRoutes", "ImportCustomRoutes"}

		peer, back := b.peerSideLocked(n, p)
		if peer != nil {
			v.PeerMtu = peer.Mtu
		}

		if back != nil {
			v.State, v.StateDetails = peerActive, peerDetailOn
		} else {
			v.State, v.StateDetails = peerInactive, peerDetailWait
		}

		out = append(out, v)
	}

	return out
}

func (b *InMemoryBackend) networkRangesWithOwnerLocked(n *computeapi.Network) map[netip.Prefix]string {
	out := map[netip.Prefix]string{}

	for _, s := range b.subnets {
		if s.Network != n.SelfLink {
			continue
		}

		if p, err := netip.ParsePrefix(s.IpCidrRange); err == nil {
			out[p] = trimURL(s.SelfLink)
		}

		for _, sec := range s.SecondaryIpRanges {
			if p, err := netip.ParsePrefix(sec.IpCidrRange); err == nil {
				out[p] = trimURL(s.SelfLink)
			}
		}
	}

	return out
}

func (b *InMemoryBackend) peerOverlapLocked(local, peer *computeapi.Network) *APIError {
	mine, theirs := b.networkRangesWithOwnerLocked(local), b.networkRangesWithOwnerLocked(peer)
	keys := make([]netip.Prefix, 0, len(mine))

	for p := range mine {
		keys = append(keys, p)
	}

	slices.SortFunc(keys, func(a, c netip.Prefix) int { return strings.Compare(a.String(), c.String()) })

	for _, lp := range keys {
		for tp, owner := range theirs {
			if lp.Overlaps(tp) {
				return errInvalid(
					"An IP range in the peer network (%s) overlaps with an IP range in the local network (%s) "+
						"allocated by resource (%s).", tp, lp, owner)
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) buildPeeringLocked(
	project string,
	n *computeapi.Network,
	req *addPeeringRequest,
	raw map[string]any,
) (*computeapi.NetworkPeering, *computeapi.Network, *APIError) {
	p := req.NetworkPeering
	nested, _ := raw["networkPeering"].(map[string]any)

	if p == nil {
		p = &computeapi.NetworkPeering{
			Name: req.Name, Network: req.PeerNetwork, ExchangeSubnetRoutes: true,
		}
		if _, ok := raw["autoCreateRoutes"]; ok {
			p.ExchangeSubnetRoutes = req.AutoCreateRoutes
		}

		nested = map[string]any{}
	} else {
		p = clone(p)
		if _, ok := nested["exchangeSubnetRoutes"]; !ok {
			p.ExchangeSubnetRoutes = true
		}
	}

	if err := validName("networkPeering.name", p.Name); err != nil {
		return nil, nil, err
	}

	if p.Network == "" {
		return nil, nil, errRequired("networkPeering.network")
	}

	if _, ok := nested["exportSubnetRoutesWithPublicIp"]; !ok {
		p.ExportSubnetRoutesWithPublicIp = true
	}

	r, err := refGlobal(p.Network, "networks", project)
	if err != nil {
		return nil, nil, errField("networkPeering.network", p.Network, "The URL is malformed.")
	}

	peer, perr := b.getNetworkLocked(r.project, r.name)
	if perr != nil {
		return nil, nil, perr
	}

	if peer.SelfLink == n.SelfLink {
		return nil, nil, errField("networkPeering.network", p.Network, "A network cannot be peered with itself.")
	}

	if verr := validPeeringEnums(p); verr != nil {
		return nil, nil, verr
	}

	p.Network = peer.SelfLink
	p.AutoCreateRoutes = p.ExchangeSubnetRoutes
	p.State, p.StateDetails, p.PeerMtu, p.ConnectionStatus = "", "", 0, nil

	return p, peer, nil
}

func validPeeringEnums(p *computeapi.NetworkPeering) *APIError {
	switch p.StackType {
	case "", stackIPv4Only, stackIPv4IPv6:
	default:
		return errField("networkPeering.stackType", p.StackType, "Must be IPV4_ONLY or IPV4_IPV6.")
	}

	switch p.UpdateStrategy {
	case "", "UNSPECIFIED", stratIndep:
	case "CONSENSUS":
		return errField("networkPeering.updateStrategy", p.UpdateStrategy,
			"CONSENSUS peerings are not supported by this emulator.")
	default:
		return errField("networkPeering.updateStrategy", p.UpdateStrategy, "")
	}

	if p.StackType == "" {
		p.StackType = stackIPv4Only
	}

	p.UpdateStrategy = stratIndep

	return nil
}

func (b *InMemoryBackend) addPeering(
	project, name string,
	req *addPeeringRequest,
	raw map[string]any,
) (*computeapi.Operation, error) {
	b.mu.Lock("AddPeering")
	defer b.mu.Unlock()

	n, err := b.getNetworkLocked(project, name)
	if err != nil {
		return nil, err
	}

	p, peer, err := b.buildPeeringLocked(project, n, req, raw)
	if err != nil {
		return nil, err
	}

	if len(n.Peerings) >= maxPeerings {
		return nil, errInvalid("Quota 'PEERINGS_PER_NETWORK' exceeded. Limit: %d.", maxPeerings)
	}

	for _, have := range n.Peerings {
		if have.Name == p.Name {
			return nil, errInvalid("Invalid value for field 'networkPeering.name': '%s'. A peering with this name "+
				"already exists in the network.", p.Name)
		}

		if have.Network == p.Network {
			return nil, errInvalid("A peering from network '%s' to network '%s' already exists.", n.Name, peer.Name)
		}
	}

	if err = b.peerOverlapLocked(n, peer); err != nil {
		return nil, err
	}

	n.Peerings = append(n.Peerings, p)

	return b.newOpLocked("addPeering", netTarget(project, n), b.timing.Done), nil
}

func (b *InMemoryBackend) removePeering(project, name, peering string) (*computeapi.Operation, error) {
	b.mu.Lock("RemovePeering")
	defer b.mu.Unlock()

	n, err := b.getNetworkLocked(project, name)
	if err != nil {
		return nil, err
	}

	i := slices.IndexFunc(n.Peerings, func(p *computeapi.NetworkPeering) bool { return p.Name == peering })
	if i < 0 {
		return nil, errField("name", peering, "Peering not found in the network.")
	}

	n.Peerings = slices.Delete(n.Peerings, i, i+1)

	return b.newOpLocked("removePeering", netTarget(project, n), b.timing.Done), nil
}

var peeringImmutable = []string{ //nolint:gochecknoglobals // read-only lookup table
	"exchangeSubnetRoutes", "autoCreateRoutes", "exportSubnetRoutesWithPublicIp",
	"importSubnetRoutesWithPublicIp", "stackType", "updateStrategy",
}

func (b *InMemoryBackend) updatePeering(
	project, name string,
	req *updatePeeringRequest,
	raw map[string]any,
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdatePeering")
	defer b.mu.Unlock()

	n, err := b.getNetworkLocked(project, name)
	if err != nil {
		return nil, err
	}

	if req.NetworkPeering == nil {
		return nil, errRequired("networkPeering")
	}

	in := req.NetworkPeering
	nested, _ := raw["networkPeering"].(map[string]any)

	i := slices.IndexFunc(n.Peerings, func(p *computeapi.NetworkPeering) bool { return p.Name == in.Name })
	if i < 0 {
		return nil, errField("networkPeering.name", in.Name, "Peering not found in the network.")
	}

	cur := n.Peerings[i]
	want := map[string]any{
		"exchangeSubnetRoutes": cur.ExchangeSubnetRoutes, "autoCreateRoutes": cur.AutoCreateRoutes,
		"exportSubnetRoutesWithPublicIp": cur.ExportSubnetRoutesWithPublicIp,
		"importSubnetRoutesWithPublicIp": cur.ImportSubnetRoutesWithPublicIp,
		"stackType":                      cur.StackType, "updateStrategy": cur.UpdateStrategy,
	}

	for _, k := range peeringImmutable {
		if v, ok := nested[k]; ok && v != want[k] && !(k == "updateStrategy" && v == "UNSPECIFIED") {
			return nil, errField("networkPeering."+k, "", "Only exportCustomRoutes and importCustomRoutes can be updated.")
		}
	}

	if _, ok := nested["exportCustomRoutes"]; ok {
		cur.ExportCustomRoutes = in.ExportCustomRoutes
	}

	if _, ok := nested["importCustomRoutes"]; ok {
		cur.ImportCustomRoutes = in.ImportCustomRoutes
	}

	return b.newOpLocked("updatePeering", netTarget(project, n), b.timing.Done), nil
}

// customRoutesLocked returns the static routes a peering can carry: everything except system and internet-gateway routes.
func (b *InMemoryBackend) customRoutesLocked(n *computeapi.Network) []*computeapi.Route {
	var out []*computeapi.Route

	for _, r := range b.routes {
		if r.Network == n.SelfLink && !isSystemRoute(r) && r.NextHopGateway == "" {
			out = append(out, r)
		}
	}

	return out
}

func (b *InMemoryBackend) exportedRoutesLocked(
	from *computeapi.Network,
	exp *computeapi.NetworkPeering,
	subnetImported, customImported bool,
) []*computeapi.ExchangedPeeringRoute {
	var out []*computeapi.ExchangedPeeringRoute

	if exp.ExchangeSubnetRoutes {
		ranges := make([]string, 0)

		for p := range b.networkRangesWithOwnerLocked(from) {
			ranges = append(ranges, p.String())
		}

		sort.Strings(ranges)

		for _, cidr := range ranges {
			out = append(out, &computeapi.ExchangedPeeringRoute{DestRange: cidr, Type: peerRouteSub, Imported: subnetImported})
		}
	}

	if exp.ExportCustomRoutes {
		customs := b.customRoutesLocked(from)
		sort.Slice(customs, func(i, j int) bool { return customs[i].Name < customs[j].Name })

		for _, r := range customs {
			out = append(out, &computeapi.ExchangedPeeringRoute{
				DestRange: r.DestRange, Type: peerRouteStat, Priority: r.Priority, Imported: customImported,
			})
		}
	}

	return out
}

func (b *InMemoryBackend) listPeeringRoutes(project, name, peering, direction string) ([]entry, error) {
	b.mu.RLock("ListPeeringRoutes")
	defer b.mu.RUnlock()

	n, err := b.getNetworkLocked(project, name)
	if err != nil {
		return nil, err
	}

	switch direction {
	case "", dirIncoming, dirOutgoing:
	default:
		return nil, errField("direction", direction, "Must be INCOMING or OUTGOING.")
	}

	if peering != "" && !slices.ContainsFunc(n.Peerings, func(p *computeapi.NetworkPeering) bool {
		return p.Name == peering
	}) {
		return nil, errField("peeringName", peering, "Peering not found in the network.")
	}

	var out []entry

	for _, p := range n.Peerings {
		if peering != "" && p.Name != peering {
			continue
		}

		peer, back := b.peerSideLocked(n, p)
		if back == nil {
			continue
		}

		var routes []*computeapi.ExchangedPeeringRoute
		if direction == dirOutgoing {
			routes = b.exportedRoutesLocked(n, p, back.ExchangeSubnetRoutes, back.ImportCustomRoutes)
		} else {
			routes = b.exportedRoutesLocked(peer, back, p.ExchangeSubnetRoutes, p.ImportCustomRoutes)
		}

		for _, r := range routes {
			out = append(out, entry{scope: scopeGlobal, item: r})
		}
	}

	return out, nil
}

func (h *Handler) registerPeeringOps() {
	g := scopeGlobal
	h.reg("networks", g, "addPeering", func(h *Handler, rq *request) (any, error) {
		in, raw, err := decode[addPeeringRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.addPeering(rq.rt.project, rq.rt.name, in, raw))
	})
	h.reg("networks", g, "removePeering", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[removePeeringRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.removePeering(rq.rt.project, rq.rt.name, in.Name))
	})
	h.reg("networks", g, "updatePeering", func(h *Handler, rq *request) (any, error) {
		in, raw, err := decode[updatePeeringRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.updatePeering(rq.rt.project, rq.rt.name, in, raw))
	})
	h.reg("networks", g, "listPeeringRoutes", func(h *Handler, rq *request) (any, error) {
		entries, err := h.Backend.listPeeringRoutes(
			rq.rt.project, rq.rt.name, rq.q.Get("peeringName"), rq.q.Get("direction"))
		if err != nil {
			return nil, err
		}

		return listBody(rq, "compute#exchangedPeeringRoutesList", entries)
	})
}

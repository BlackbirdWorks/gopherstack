package gcpcompute

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/netip"
	"slices"
	"time"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collVPNGateways       = "vpnGateways"
	collTargetVPNGateways = "targetVpnGateways"
	collVPNTunnels        = "vpnTunnels"
	collExternalVPN       = "externalVpnGateways"
	vpnInterfaceCount     = 2
	externalMaxInterfaces = 4
	ikeV1                 = 1
	ikeV2                 = 2
	tunnelEstablished     = "ESTABLISHED"
	tunnelProvisioning    = "PROVISIONING"
	tunnelAllocating      = "ALLOCATING_RESOURCES"
	tunnelWaitingConfig   = "WAITING_FOR_FULL_CONFIG"
	tunnelAuthError       = "AUTHORIZATION_ERROR"
	tunnelNoPackets       = "NO_INCOMING_PACKETS"
	gatewayReady          = "READY"
	anyIPv4               = "0.0.0.0/0"
	espProtocol           = "ESP"
	ikePort               = "500-500"
	natTPort              = "4500-4500"
)

func vpnGatewaySpec() lbSpec[computeapi.VpnGateway] {
	return lbSpec[computeapi.VpnGateway]{
		coll: collVPNGateways, kind: "compute#vpnGateway", noun: "vpn_gateway",
		m: func(b *InMemoryBackend) map[string]*computeapi.VpnGateway { return b.vpnGateways },
		meta: func(v *computeapi.VpnGateway) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, id: &v.Id,
			}
		},
		prep: prepVPNGateway,
	}
}

func targetVPNSpec() lbSpec[computeapi.TargetVpnGateway] {
	return lbSpec[computeapi.TargetVpnGateway]{
		coll: collTargetVPNGateways, kind: "compute#targetVpnGateway", noun: "target_vpn_gateway",
		m: func(b *InMemoryBackend) map[string]*computeapi.TargetVpnGateway { return b.targetVPNs },
		meta: func(v *computeapi.TargetVpnGateway) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, id: &v.Id,
			}
		},
		prep:  prepTargetVPN,
		viewB: viewTargetVPN,
	}
}

func vpnTunnelSpec() lbSpec[computeapi.VpnTunnel] {
	return lbSpec[computeapi.VpnTunnel]{
		coll: collVPNTunnels, kind: "compute#vpnTunnel", noun: "vpn_tunnel",
		m: func(b *InMemoryBackend) map[string]*computeapi.VpnTunnel { return b.vpnTunnels },
		meta: func(v *computeapi.VpnTunnel) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, id: &v.Id,
			}
		},
		prep:  prepVPNTunnel,
		viewB: viewVPNTunnel,
		refs: func(v *computeapi.VpnTunnel) []string {
			return []string{v.VpnGateway, v.TargetVpnGateway, v.PeerGcpGateway, v.PeerExternalGateway, v.Router}
		},
	}
}

func externalVPNSpec() lbSpec[computeapi.ExternalVpnGateway] {
	return lbSpec[computeapi.ExternalVpnGateway]{
		coll: collExternalVPN, kind: "compute#externalVpnGateway", noun: "external_vpn_gateway",
		m: func(b *InMemoryBackend) map[string]*computeapi.ExternalVpnGateway { return b.externalVPNs },
		meta: func(v *computeapi.ExternalVpnGateway) lbMeta {
			if v.Id == nil {
				v.Id = new(uint64)
			}

			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink, id: v.Id,
			}
		},
		prep: prepExternalVPN,
	}
}

func (b *InMemoryBackend) resolveNetworkRefLocked(c lbCtx, in *string) *APIError {
	if *in == "" {
		return errRequired("resource.network")
	}

	n, err := b.resolveNetworkLocked(c.project, *in)
	if err != nil {
		return err
	}

	*in = n.SelfLink

	return nil
}

func prepVPNGateway(b *InMemoryBackend, c lbCtx, in *computeapi.VpnGateway, raw map[string]any) *APIError {
	if err := b.resolveNetworkRefLocked(c, &in.Network); err != nil {
		return err
	}

	switch in.GatewayIpVersion {
	case "":
		in.GatewayIpVersion = ipv4
	case ipv4, ipv6:
	default:
		return errField("resource.gatewayIpVersion", in.GatewayIpVersion, "Must be IPV4 or IPV6.")
	}

	switch in.StackType {
	case "":
		in.StackType = "IPV4_ONLY"
		if in.GatewayIpVersion == ipv6 {
			in.StackType = "IPV4_IPV6"
		}
	case "IPV4_ONLY", "IPV4_IPV6", "IPV6_ONLY":
	default:
		return errField("resource.stackType", in.StackType, "")
	}

	if err := validLabels(in.Labels); err != nil {
		return err
	}

	in.LabelFingerprint = labelFP(in.Labels)

	for _, vi := range in.VpnInterfaces {
		if vi.InterconnectAttachment != "" {
			return errField("resource.vpnInterfaces.interconnectAttachment", vi.InterconnectAttachment,
				"HA VPN over Cloud Interconnect is not supported by this emulator.")
		}
	}

	return b.allocVPNInterfacesLocked(c, in, raw != nil)
}

// allocVPNInterfacesLocked gives an HA gateway its two interfaces; addresses are kept unless fresh is set.
func (b *InMemoryBackend) allocVPNInterfacesLocked(c lbCtx, in *computeapi.VpnGateway, fresh bool) *APIError {
	old := in.VpnInterfaces
	in.VpnInterfaces = make([]*computeapi.VpnGatewayVpnGatewayInterface, 0, vpnInterfaceCount)

	var taken []string

	for i := range vpnInterfaceCount {
		vi := &computeapi.VpnGatewayVpnGatewayInterface{Id: int64(i)}
		if !fresh && i < len(old) {
			vi.IpAddress, vi.Ipv6Address = old[i].IpAddress, old[i].Ipv6Address
		}

		if err := b.fillVPNAddressLocked(c, in.GatewayIpVersion, vi, taken); err != nil {
			return err
		}

		taken = append(taken, vi.IpAddress, vi.Ipv6Address)
		in.VpnInterfaces = append(in.VpnInterfaces, vi)
	}

	return nil
}

func (b *InMemoryBackend) fillVPNAddressLocked(
	c lbCtx, version string, vi *computeapi.VpnGatewayVpnGatewayInterface, taken []string,
) *APIError {
	if version == ipv6 {
		if vi.Ipv6Address == "" {
			used := b.usedExternalLocked()
			for _, t := range taken {
				used[t] = true
			}

			for n := 1; ; n++ {
				cand := fmt.Sprintf("2600:1900:%x:%x::", regionIndex(c.loc)+1, n)
				if !used[cand] {
					vi.Ipv6Address = cand

					break
				}
			}
		}

		return nil
	}

	if vi.IpAddress != "" {
		return nil
	}

	ip, err := b.allocExternalLocked(c.loc, taken...)
	if err != nil {
		return err
	}

	vi.IpAddress = ip

	return nil
}

func prepTargetVPN(b *InMemoryBackend, c lbCtx, in *computeapi.TargetVpnGateway, _ map[string]any) *APIError {
	if err := b.resolveNetworkRefLocked(c, &in.Network); err != nil {
		return err
	}

	if err := validLabels(in.Labels); err != nil {
		return err
	}

	in.LabelFingerprint = labelFP(in.Labels)
	in.Status, in.Tunnels, in.ForwardingRules = "", nil, nil

	return nil
}

func viewTargetVPN(b *InMemoryBackend, v *computeapi.TargetVpnGateway) {
	v.Status = gatewayReady

	for _, t := range b.vpnTunnels {
		if t.TargetVpnGateway == v.SelfLink {
			v.Tunnels = append(v.Tunnels, t.SelfLink)
		}
	}

	for _, r := range b.fwdRules {
		if r.Target == v.SelfLink {
			v.ForwardingRules = append(v.ForwardingRules, r.SelfLink)
		}
	}

	slices.Sort(v.Tunnels)
	slices.Sort(v.ForwardingRules)
}

func prepExternalVPN(_ *InMemoryBackend, _ lbCtx, in *computeapi.ExternalVpnGateway, _ map[string]any) *APIError {
	if err := validLabels(in.Labels); err != nil {
		return err
	}

	in.LabelFingerprint = labelFP(in.Labels)

	want := map[string]int{
		"SINGLE_IP_INTERNALLY_REDUNDANT": 1,
		"TWO_IPS_REDUNDANCY":             2,
		"FOUR_IPS_REDUNDANCY":            externalMaxInterfaces,
	}

	n, ok := want[in.RedundancyType]
	if !ok {
		return errField("resource.redundancyType", in.RedundancyType,
			"Must be SINGLE_IP_INTERNALLY_REDUNDANT, TWO_IPS_REDUNDANCY or FOUR_IPS_REDUNDANCY.")
	}

	if len(in.Interfaces) != n {
		return errField("resource.interfaces", "", "The number of interfaces must match the redundancy type.")
	}

	seen := map[int64]bool{}

	for _, ifc := range in.Interfaces {
		if ifc.Id < 0 || ifc.Id >= externalMaxInterfaces || seen[ifc.Id] {
			return errField("resource.interfaces.id", int64Str(ifc.Id), "Interface ids are unique values 0 to 3.")
		}

		seen[ifc.Id] = true

		if ifc.IpAddress == "" && ifc.Ipv6Address == "" {
			return errRequired("resource.interfaces.ipAddress")
		}

		for _, raw := range []string{ifc.IpAddress, ifc.Ipv6Address} {
			if raw == "" {
				continue
			}

			if _, err := netip.ParseAddr(raw); err != nil {
				return errField("resource.interfaces.ipAddress", raw, "Not a valid IP address.")
			}
		}
	}

	return nil
}

func secretHash(secret string) string {
	sum := sha256.Sum256([]byte(secret))

	return base64.RawURLEncoding.EncodeToString(sum[:])[:20]
}

func prepVPNTunnel(b *InMemoryBackend, c lbCtx, in *computeapi.VpnTunnel, _ map[string]any) *APIError {
	if (in.VpnGateway == "") == (in.TargetVpnGateway == "") {
		return errInvalid("Invalid value for field 'resource': exactly one of vpnGateway and targetVpnGateway " +
			"must be specified.")
	}

	if in.SharedSecret != "" {
		in.SharedSecretHash = secretHash(in.SharedSecret)
		in.SharedSecret = ""
	}

	if in.SharedSecretHash == "" {
		return errRequired("resource.sharedSecret")
	}

	switch in.IkeVersion {
	case 0:
		in.IkeVersion = ikeV2
	case ikeV1, ikeV2:
	default:
		return errField("resource.ikeVersion", int64Str(in.IkeVersion), "Acceptable IKE versions are 1 or 2.")
	}

	if err := validLabels(in.Labels); err != nil {
		return err
	}

	in.LabelFingerprint = labelFP(in.Labels)

	if err := b.resolveTunnelRouterLocked(c, in); err != nil {
		return err
	}

	in.Status, in.DetailedStatus = "", ""

	if in.VpnGateway != "" {
		return b.prepHATunnelLocked(c, in)
	}

	return b.prepClassicTunnelLocked(c, in)
}

func (b *InMemoryBackend) resolveTunnelRouterLocked(c lbCtx, in *computeapi.VpnTunnel) *APIError {
	if in.Router == "" {
		return nil
	}

	r, ok := parseRef(in.Router, "routers", c.project, c.loc)
	if !ok || r.loc != c.loc {
		return errField("resource.router", in.Router, "The router must be in the tunnel's region.")
	}

	rec, found := b.routers[key(r.project, c.loc, r.name)]
	if !found {
		return errNotFound(relPath(r.project, scopeRegion, c.loc, "routers", r.name))
	}

	in.Router = rec.Router.SelfLink

	return nil
}

func (b *InMemoryBackend) prepHATunnelLocked(c lbCtx, in *computeapi.VpnTunnel) *APIError {
	l, err := lbResolve(b, vpnGatewaySpec(), c, "resource.vpnGateway", in.VpnGateway)
	if err != nil {
		return err
	}

	in.VpnGateway = l

	if in.VpnGatewayInterface < 0 || in.VpnGatewayInterface >= vpnInterfaceCount {
		return errField("resource.vpnGatewayInterface", int64Str(in.VpnGatewayInterface), "Must be 0 or 1.")
	}

	if in.PeerIp != "" || in.TargetVpnGateway != "" {
		return errInvalid("Invalid value for field 'resource.peerIp': only Classic VPN tunnels take a peerIp.")
	}

	if (in.PeerGcpGateway == "") == (in.PeerExternalGateway == "") {
		return errInvalid("Invalid value for field 'resource': exactly one of peerGcpGateway and " +
			"peerExternalGateway must be specified for an HA VPN tunnel.")
	}

	if err = b.resolveTunnelPeerLocked(c, in); err != nil {
		return err
	}

	in.LocalTrafficSelector, in.RemoteTrafficSelector = []string{anyIPv4}, []string{anyIPv4}

	return nil
}

func (b *InMemoryBackend) resolveTunnelPeerLocked(c lbCtx, in *computeapi.VpnTunnel) *APIError {
	if in.PeerGcpGateway != "" {
		r, ok := parseRef(in.PeerGcpGateway, collVPNGateways, c.project, c.loc)
		if !ok || r.loc == "" {
			return errField("resource.peerGcpGateway", in.PeerGcpGateway, "The URL is malformed.")
		}

		g, found := b.vpnGateways[key(r.project, r.loc, r.name)]
		if !found {
			return errNotFound(relPath(r.project, scopeRegion, r.loc, collVPNGateways, r.name))
		}

		in.PeerGcpGateway = g.SelfLink

		return nil
	}

	l, err := lbResolve(
		b,
		externalVPNSpec(),
		lbCtx{project: c.project, scope: scopeGlobal},
		"resource.peerExternalGateway",
		in.PeerExternalGateway,
	)
	if err != nil {
		return err
	}

	in.PeerExternalGateway = l

	ext := lbFind(b, externalVPNSpec(), l)
	if !slices.ContainsFunc(ext.Interfaces, func(i *computeapi.ExternalVpnGatewayInterface) bool {
		return i.Id == in.PeerExternalGatewayInterface
	}) {
		return errField("resource.peerExternalGatewayInterface", int64Str(in.PeerExternalGatewayInterface),
			"The external VPN gateway has no such interface.")
	}

	return nil
}

func (b *InMemoryBackend) prepClassicTunnelLocked(c lbCtx, in *computeapi.VpnTunnel) *APIError {
	l, err := lbResolve(b, targetVPNSpec(), c, "resource.targetVpnGateway", in.TargetVpnGateway)
	if err != nil {
		return err
	}

	in.TargetVpnGateway = l

	if in.PeerGcpGateway != "" || in.PeerExternalGateway != "" || in.VpnGatewayInterface != 0 {
		return errInvalid("Invalid value for field 'resource.peerGcpGateway': Classic VPN tunnels take a peerIp.")
	}

	if a, perr := netip.ParseAddr(in.PeerIp); perr != nil || !a.Is4() {
		return errField("resource.peerIp", in.PeerIp, "Must be a valid IPv4 address.")
	}

	for field, sel := range map[string]*[]string{
		"localTrafficSelector": &in.LocalTrafficSelector, "remoteTrafficSelector": &in.RemoteTrafficSelector,
	} {
		for _, cidr := range *sel {
			if _, perr := netip.ParsePrefix(cidr); perr != nil {
				return errField("resource."+field, cidr, "Must be a CIDR range such as 192.168.0.0/16.")
			}
		}

		if len(*sel) == 0 {
			*sel = []string{anyIPv4}
		}
	}

	return nil
}

func (b *InMemoryBackend) ruleMatchesLocked(gateway, protocol, ports string) bool {
	for _, r := range b.fwdRules {
		if r.Target == gateway && r.IPProtocol == protocol && (ports == "" || r.PortRange == ports) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) classicConfiguredLocked(t *computeapi.VpnTunnel) bool {
	g := t.TargetVpnGateway
	if !b.ruleMatchesLocked(g, espProtocol, "") || !b.ruleMatchesLocked(g, protoUDP, ikePort) ||
		!b.ruleMatchesLocked(g, protoUDP, natTPort) {
		return false
	}

	if t.Router != "" {
		return true
	}

	for _, r := range b.routes {
		if r.NextHopVpnTunnel == t.SelfLink {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) classicGatewayIPLocked(gateway string) string {
	for _, r := range b.fwdRules {
		if r.Target == gateway && r.IPProtocol == espProtocol {
			return r.IPAddress
		}
	}

	return ""
}

// peerTunnelLocked returns the tunnel in this project that is the other end of t, if the project holds both.
func (b *InMemoryBackend) peerTunnelLocked(t *computeapi.VpnTunnel) *computeapi.VpnTunnel {
	for _, o := range b.vpnTunnels {
		if o.SelfLink == t.SelfLink {
			continue
		}

		if t.VpnGateway != "" && o.VpnGateway == t.PeerGcpGateway && o.PeerGcpGateway == t.VpnGateway &&
			o.VpnGatewayInterface == t.VpnGatewayInterface {
			return o
		}

		if t.TargetVpnGateway != "" && o.TargetVpnGateway != "" {
			mine, theirs := b.classicGatewayIPLocked(t.TargetVpnGateway), b.classicGatewayIPLocked(o.TargetVpnGateway)
			if mine != "" && theirs != "" && t.PeerIp == theirs && o.PeerIp == mine {
				return o
			}
		}
	}

	return nil
}

// tunnelStateLocked models the tunnel state machine: allocation on the clock, then whether the project holds
// a matching other end. No IPsec runs, so a peer outside the project never answers.
func (b *InMemoryBackend) tunnelStateLocked(t *computeapi.VpnTunnel) (string, string) {
	created, err := time.Parse(timeLayout, t.CreationTimestamp)
	if err != nil {
		return tunnelNoPackets, "No incoming packets from the peer."
	}

	switch el := b.now().UTC().Sub(created); {
	case el < b.timing.Pending:
		return tunnelProvisioning, "Resources are being allocated for the tunnel."
	case el < b.timing.Done:
		return tunnelAllocating, "Cloud VPN is allocating the resources for the tunnel."
	}

	if t.TargetVpnGateway != "" && !b.classicConfiguredLocked(t) {
		return tunnelWaitingConfig, "Waiting for the forwarding rules and a route or router for the tunnel."
	}

	peer := b.peerTunnelLocked(t)

	switch {
	case peer == nil:
		return tunnelNoPackets, "No incoming packets from the peer VPN gateway."
	case peer.SharedSecretHash != t.SharedSecretHash:
		return tunnelAuthError, "Authorization failed: the shared secrets of the two tunnels differ."
	}

	return tunnelEstablished, "Tunnel is up and running."
}

func viewVPNTunnel(b *InMemoryBackend, v *computeapi.VpnTunnel) {
	v.Status, v.DetailedStatus = b.tunnelStateLocked(v)
}

func tunnelPeerInterface(t *computeapi.VpnTunnel) int64 {
	if t.PeerGcpGateway != "" {
		return t.VpnGatewayInterface
	}

	return t.PeerExternalGatewayInterface
}

func (b *InMemoryBackend) vpnGatewayStatus(c lbCtx, name string) (*computeapi.VpnGatewaysGetStatusResponse, error) {
	b.mu.RLock("GetVpnGatewayStatus")
	defer b.mu.RUnlock()

	g, ok := b.vpnGateways[key(c.project, c.loc, name)]
	if !ok {
		return nil, errNotFound(relPath(c.project, c.scope, c.loc, collVPNGateways, name))
	}

	conns := map[string]*computeapi.VpnGatewayStatusVpnConnection{}
	order := []string{}

	for _, t := range b.vpnTunnels {
		if t.VpnGateway != g.SelfLink {
			continue
		}

		peer := cmpOr(t.PeerGcpGateway, t.PeerExternalGateway)

		conn, found := conns[peer]
		if !found {
			conn = &computeapi.VpnGatewayStatusVpnConnection{
				PeerGcpGateway:      t.PeerGcpGateway,
				PeerExternalGateway: t.PeerExternalGateway,
			}
			conns[peer] = conn
			order = append(order, peer)
		}

		conn.Tunnels = append(conn.Tunnels, &computeapi.VpnGatewayStatusTunnel{
			LocalGatewayInterface: t.VpnGatewayInterface, PeerGatewayInterface: tunnelPeerInterface(t), TunnelUrl: t.SelfLink,
		})
	}

	slices.Sort(order)

	out := &computeapi.VpnGatewayStatus{}

	for _, peer := range order {
		conn := conns[peer]
		slices.SortFunc(conn.Tunnels, func(a, c *computeapi.VpnGatewayStatusTunnel) int {
			return cmpString(a.TunnelUrl, c.TunnelUrl)
		})

		conn.State = redundancyState(conn.Tunnels)
		out.VpnConnections = append(out.VpnConnections, conn)
	}

	return &computeapi.VpnGatewaysGetStatusResponse{Result: out}, nil
}

func redundancyState(
	tunnels []*computeapi.VpnGatewayStatusTunnel,
) *computeapi.VpnGatewayStatusHighAvailabilityRequirementState {
	local := map[int64]bool{}
	for _, t := range tunnels {
		local[t.LocalGatewayInterface] = true
	}

	if len(local) >= vpnInterfaceCount {
		return &computeapi.VpnGatewayStatusHighAvailabilityRequirementState{State: "CONNECTION_REDUNDANCY_MET"}
	}

	return &computeapi.VpnGatewayStatusHighAvailabilityRequirementState{
		State: "CONNECTION_REDUNDANCY_NOT_MET", UnsatisfiedReason: "INCOMPLETE_TUNNELS_COVERAGE",
	}
}

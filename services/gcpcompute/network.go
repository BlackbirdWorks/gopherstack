package gcpcompute

import (
	"crypto/rand"
	"encoding/hex"
	"net/netip"
	"slices"
	"sort"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	defaultMTU      = 1460
	minMTU          = 1300
	maxMTU          = 8896
	sysRoutePrefix  = "default-route-"
	defaultGateway  = "default-internet-gateway"
	enforceOrderDef = "AFTER_CLASSIC_FIREWALL"
)

func netTarget(project string, n *computeapi.Network) target {
	return target{project: project, scope: scopeGlobal, collection: "networks", name: n.Name, id: n.Id}
}

func (b *InMemoryBackend) networkViewLocked(project string, n *computeapi.Network) *computeapi.Network {
	out := clone(n)
	out.ForceSendFields = []string{"AutoCreateSubnetworks"}
	out.Subnetworks = nil

	for _, s := range b.subnets {
		if s.Network == n.SelfLink && strings.Contains(s.SelfLink, "/projects/"+project+"/") {
			out.Subnetworks = append(out.Subnetworks, s.SelfLink)
		}
	}

	sort.Strings(out.Subnetworks)

	out.Peerings = b.peeringsViewLocked(n)
	if len(out.Peerings) == 0 {
		out.Peerings = nil
	}

	return out
}

func (b *InMemoryBackend) getNetworkLocked(project, name string) (*computeapi.Network, *APIError) {
	n, ok := b.networks[key(project, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeGlobal, "", "networks", name))
	}

	return n, nil
}

func (b *InMemoryBackend) resolveNetworkLocked(project, raw string) (*computeapi.Network, *APIError) {
	r, err := refGlobal(raw, "networks", project)
	if err != nil {
		return nil, errField(collNetwork, raw, "The URL is malformed.")
	}

	return b.getNetworkLocked(r.project, r.name)
}

func newSubnet(project, region, name, network string, p netip.Prefix, stamp string) *computeapi.Subnetwork {
	return &computeapi.Subnetwork{
		Kind: "compute#subnetwork", Id: newID(), CreationTimestamp: stamp, Name: name, Network: network,
		IpCidrRange: p.String(), GatewayAddress: gateway(p).String(), Region: scopeURL(project, scopeRegion, region),
		SelfLink: regionLink(project, region, "subnetworks", name), Fingerprint: newFingerprint(),
		Purpose: "PRIVATE", StackType: "IPV4_ONLY", PrivateIpv6GoogleAccess: "DISABLE_GOOGLE_ACCESS",
	}
}

func (b *InMemoryBackend) createAutoSubnetsLocked(project string, n *computeapi.Network) {
	for _, r := range regionCatalog {
		p := netip.MustParsePrefix(r.autoRange)
		b.subnets[key(project, r.name, n.Name)] = newSubnet(project, r.name, n.Name, n.SelfLink, p, b.stamp())
	}
}

func (b *InMemoryBackend) systemRouteLocked(project string, n *computeapi.Network) {
	var buf [8]byte

	_, _ = rand.Read(buf[:])

	name := sysRoutePrefix + hex.EncodeToString(buf[:])
	b.routes[key(project, name)] = &computeapi.Route{
		Kind: "compute#route", Id: newID(), CreationTimestamp: b.stamp(), Name: name, Network: n.SelfLink,
		Description: "Default route to the Internet.", DestRange: "0.0.0.0/0", Priority: defaultRoutePrio,
		NextHopGateway: globalLink(project, "gateways", defaultGateway), SelfLink: globalLink(project, "routes", name),
	}
}

func validRoutingConfig(rc *computeapi.NetworkRoutingConfig) *APIError {
	if rc != nil && rc.RoutingMode != "" && rc.RoutingMode != "REGIONAL" && rc.RoutingMode != "GLOBAL" {
		return errField("resource.routingConfig.routingMode", rc.RoutingMode, "")
	}

	return nil
}

func (b *InMemoryBackend) insertNetwork(
	project string,
	in *computeapi.Network,
	autoGiven bool,
) (*computeapi.Operation, error) {
	b.mu.Lock("InsertNetwork")
	defer b.mu.Unlock()

	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if in.IPv4Range != "" {
		return nil, errField("resource.IPv4Range", in.IPv4Range, "Legacy networks are not supported.")
	}

	if _, dup := b.networks[key(project, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeGlobal, "", "networks", in.Name))
	}

	if err := validRoutingConfig(in.RoutingConfig); err != nil {
		return nil, err
	}

	if in.Mtu != 0 && (in.Mtu < minMTU || in.Mtu > maxMTU) {
		return nil, errField("resource.mtu", int64Str(in.Mtu), "MTU must be between 1300 and 8896.")
	}

	n := &computeapi.Network{
		Kind: "compute#network", Id: newID(), CreationTimestamp: b.stamp(), Name: in.Name, Description: in.Description,
		AutoCreateSubnetworks: in.AutoCreateSubnetworks || !autoGiven, Mtu: in.Mtu,
		RoutingConfig:                         &computeapi.NetworkRoutingConfig{RoutingMode: "REGIONAL"},
		NetworkFirewallPolicyEnforcementOrder: in.NetworkFirewallPolicyEnforcementOrder,
		SelfLink:                              globalLink(project, "networks", in.Name),
	}

	if n.Mtu == 0 {
		n.Mtu = defaultMTU
	}

	if in.RoutingConfig != nil && in.RoutingConfig.RoutingMode != "" {
		n.RoutingConfig.RoutingMode = in.RoutingConfig.RoutingMode
	}

	if n.NetworkFirewallPolicyEnforcementOrder == "" {
		n.NetworkFirewallPolicyEnforcementOrder = enforceOrderDef
	}

	n.SelfLinkWithId = globalLink(project, "networks", strconv.FormatUint(n.Id, 10))
	b.networks[key(project, n.Name)] = n

	if n.AutoCreateSubnetworks {
		b.createAutoSubnetsLocked(project, n)
	}

	b.systemRouteLocked(project, n)
	b.syncSubnetRoutesLocked(project, n)

	return b.newOpLocked("insert", netTarget(project, n), b.timing.Done), nil
}

func (b *InMemoryBackend) getNetwork(project, name string) (*computeapi.Network, error) {
	b.mu.RLock("GetNetwork")
	defer b.mu.RUnlock()

	n, err := b.getNetworkLocked(project, name)
	if err != nil {
		return nil, err
	}

	return b.networkViewLocked(project, n), nil
}

func (b *InMemoryBackend) listNetworks(project string) []entry {
	b.mu.RLock("ListNetworks")
	defer b.mu.RUnlock()

	var out []entry

	for k, n := range b.networks {
		if strings.HasPrefix(k, project+"/") {
			out = append(out, entry{scope: scopeGlobal, item: b.networkViewLocked(project, n)})
		}
	}

	return out
}

func (b *InMemoryBackend) networkUserLocked(project string, n *computeapi.Network) string {
	if len(n.Peerings) > 0 {
		return trimURL(n.Peerings[0].Network)
	}

	if by := b.networkRefsLocked(project, n); by != "" {
		return by
	}

	for _, r := range b.routers {
		if r.Router.Network == n.SelfLink {
			return trimURL(r.Router.SelfLink)
		}
	}

	for _, r := range b.instances {
		if slices.ContainsFunc(r.Inst.NetworkInterfaces, func(nic *computeapi.NetworkInterface) bool {
			return nic.Network == n.SelfLink
		}) {
			return trimURL(r.Inst.SelfLink)
		}
	}

	return ""
}

func (b *InMemoryBackend) networkRefsLocked(project string, n *computeapi.Network) string {
	for _, f := range b.firewalls {
		if f.Network == n.SelfLink {
			return relPath(project, scopeGlobal, "", "firewalls", f.Name)
		}
	}

	for _, r := range b.routes {
		if r.Network == n.SelfLink && !isSystemRoute(r) {
			return relPath(project, scopeGlobal, "", "routes", r.Name)
		}
	}

	for _, s := range b.subnets {
		if s.Network == n.SelfLink && (!n.AutoCreateSubnetworks || s.Name != n.Name) {
			return trimURL(s.SelfLink)
		}
	}

	return ""
}

func isSystemRoute(r *computeapi.Route) bool {
	return isSubnetRoute(r) || strings.HasPrefix(r.Name, sysRoutePrefix) && r.NextHopGateway != ""
}

func (b *InMemoryBackend) deleteNetwork(project, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteNetwork")
	defer b.mu.Unlock()

	n, err := b.getNetworkLocked(project, name)
	if err != nil {
		return nil, err
	}

	if by := b.networkUserLocked(project, n); by != "" {
		return nil, errInUse(collNetwork, relPath(project, scopeGlobal, "", "networks", name), by)
	}

	for k, s := range b.subnets {
		if s.Network == n.SelfLink {
			delete(b.subnets, k)
		}
	}

	for k, r := range b.routes {
		if r.Network == n.SelfLink {
			delete(b.routes, k)
		}
	}

	delete(b.networks, key(project, name))

	return b.newOpLocked("delete", netTarget(project, n), b.timing.Done), nil
}

var networkPatchable = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
	"name": true, "routingConfig": true, "mtu": true, "description": true,
	"networkFirewallPolicyEnforcementOrder": true, "kind": true, "id": true, "selfLink": true,
	"creationTimestamp": true, "autoCreateSubnetworks": true, "subnetworks": true, "selfLinkWithId": true,
	"peerings": true,
}

func (b *InMemoryBackend) patchNetwork(
	project, name string,
	in *computeapi.Network,
	raw map[string]any,
) (*computeapi.Operation, error) {
	b.mu.Lock("PatchNetwork")
	defer b.mu.Unlock()

	n, err := b.getNetworkLocked(project, name)
	if err != nil {
		return nil, err
	}

	for k := range raw {
		if !networkPatchable[k] {
			return nil, errField("resource."+k, "", "Field is not updatable.")
		}
	}

	if v, ok := raw["autoCreateSubnetworks"].(bool); ok && v != n.AutoCreateSubnetworks {
		return nil, errField("resource.autoCreateSubnetworks", "", "Use switchToCustomMode to change the subnet mode.")
	}

	if err = validRoutingConfig(in.RoutingConfig); err != nil {
		return nil, err
	}

	if in.Mtu != 0 && (in.Mtu < minMTU || in.Mtu > maxMTU) {
		return nil, errField("resource.mtu", int64Str(in.Mtu), "MTU must be between 1300 and 8896.")
	}

	if in.RoutingConfig != nil && in.RoutingConfig.RoutingMode != "" {
		n.RoutingConfig.RoutingMode = in.RoutingConfig.RoutingMode
	}

	if in.Mtu != 0 {
		n.Mtu = in.Mtu
	}

	if _, ok := raw["description"]; ok {
		n.Description = in.Description
	}

	if in.NetworkFirewallPolicyEnforcementOrder != "" {
		n.NetworkFirewallPolicyEnforcementOrder = in.NetworkFirewallPolicyEnforcementOrder
	}

	return b.newOpLocked("patch", netTarget(project, n), b.timing.Done), nil
}

func (b *InMemoryBackend) switchToCustomMode(project, name string) (*computeapi.Operation, error) {
	b.mu.Lock("SwitchToCustomMode")
	defer b.mu.Unlock()

	n, err := b.getNetworkLocked(project, name)
	if err != nil {
		return nil, err
	}

	if !n.AutoCreateSubnetworks {
		return nil, errInvalid("Invalid value for field 'network': '%s'. Network is not in auto subnet mode.", name)
	}

	n.AutoCreateSubnetworks = false

	return b.newOpLocked("switchToCustomMode", netTarget(project, n), b.timing.Done), nil
}

func int64Str(n int64) string { return sizeStr(n) }

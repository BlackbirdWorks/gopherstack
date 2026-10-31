package azurearm

import (
	"maps"
	"net/http"
	"net/netip"
	"strings"

	"github.com/google/uuid"
)

const (
	subnetTypePath     = "Microsoft.Network/virtualNetworks/subnets"
	minSubnetBits      = 29
	peerStateInitiated = "Initiated"
	peerStateConnected = "Connected"
	peerStateDisc      = "Disconnected"
	auxState           = "state"
	propSubnetNSG      = "networkSecurityGroup"
	propSubnetRT       = "routeTable"
)

//nolint:gochecknoglobals // read-only table
var delegationActions = map[string][]string{
	"microsoft.web/serverfarms":                   {"Microsoft.Network/virtualNetworks/subnets/action"},
	"microsoft.containerinstance/containergroups": {"Microsoft.Network/virtualNetworks/subnets/action"},
	"microsoft.dbforpostgresql/flexibleservers":   {subnetJoinAction},
	"microsoft.dbformysql/flexibleservers":        {subnetJoinAction},
	"microsoft.sql/managedinstances": {
		subnetJoinAction,
		"Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
		"Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
	},
}

func vnetSpace(o *netObj) []netip.Prefix {
	space, _ := o.Props["addressSpace"].(map[string]any)
	out, _ := netParsePrefixes(strList(space[netFieldAddrPrefixes]))

	return out
}

func subnetPrefixes(o *netObj) []netip.Prefix {
	out, _ := netParsePrefixes(strList(o.Props[netFieldAddrPrefixes]))

	return out
}

// validateVNetSpace checks the address space and DNS servers of a virtual network definition.
func validateVNetSpace(name string, props map[string]any) ([]string, []netip.Prefix, error) {
	space, _ := props["addressSpace"].(map[string]any)
	prefixes := strList(space[netFieldAddrPrefixes])

	if len(prefixes) == 0 {
		return nil, nil, netBad("Virtual network %s must have at least one address prefix in addressSpace.", name)
	}

	parsed, err := netParsePrefixes(prefixes)
	if err != nil {
		return nil, nil, err
	}

	for i := range parsed {
		for j := i + 1; j < len(parsed); j++ {
			if parsed[i].Overlaps(parsed[j]) {
				return nil, nil, netErr(http.StatusBadRequest, "AddressSpaceOverlap",
					"Address prefixes %s and %s of virtual network %s overlap.", prefixes[i], prefixes[j], name)
			}
		}
	}

	if dns, _ := props["dhcpOptions"].(map[string]any); dns != nil {
		for _, s := range strList(dns["dnsServers"]) {
			if _, perr := netip.ParseAddr(s); perr != nil {
				return nil, nil, netBad("DNS server %s is not a valid IP address.", s)
			}
		}
	}

	return prefixes, parsed, nil
}

func (p *NetworkProvider) putVNet(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)

	prefixes, parsed, err := validateVNetSpace(id.Names[0], props)
	if err != nil {
		return nil, false, err
	}

	old, existed := p.top(netVNets, id.ResourceGroup, id.Names[0])
	loc := normLocation(strOf(body, fieldLocation))

	if loc == "" && existed {
		loc = old.Obj.Location
	}

	subnets, err := p.inlineSubnets(id, old, props, parsed, loc)
	if err != nil {
		return nil, false, err
	}

	peerings, err := p.inlinePeerings(id, old, props)
	if err != nil {
		return nil, false, err
	}

	if existed {
		if rerr := p.checkExistingSubnets(old, parsed, subnets, props); rerr != nil {
			return nil, false, rerr
		}
	}

	it, _, created := p.upsertTop(netVNets, id, body)
	it.Obj.Props = takeProps(body, netSubnets, netPeerings, fieldProvisioningState, netFieldResourceGUID)
	it.Obj.Props["addressSpace"] = map[string]any{netFieldAddrPrefixes: prefixes}

	if subnets != nil {
		it.Kids[netSubnets] = subnets
	}

	if peerings != nil {
		it.Kids[netPeerings] = peerings
	}

	return p.bodyVNet(id, it), created, nil
}

// inlineSubnets returns the normalized subnet set from the request, or nil when the request carries no subnets key.
func (p *NetworkProvider) inlineSubnets(
	id ResourceID, old *netParent, props map[string]any, space []netip.Prefix, loc string,
) ([]*netObj, error) {
	rawVal, has := props[netSubnets]
	if !has {
		return nil, nil
	}

	raw, ok := rawVal.([]any)
	if !ok {
		return nil, netBad("subnets must be an array.")
	}

	out := make([]*netObj, 0, len(raw))

	for _, e := range raw {
		m, _ := e.(map[string]any)
		name := strOf(m, fieldName)

		if err := validNetName(name, netMaxName); err != nil {
			return nil, err
		}

		var prev *netObj
		if old != nil {
			prev, _ = old.kid(netSubnets, name)
		}

		o, err := p.normalizeSubnet(id, space, loc, name, propsOf(m), prev)
		if err != nil {
			return nil, err
		}

		out = append(out, o)
	}

	return out, checkSubnetOverlap(out)
}

func (p *NetworkProvider) inlinePeerings(id ResourceID, old *netParent, props map[string]any) ([]*netObj, error) {
	raw, ok := props[netPeerings].([]any)
	if !ok {
		return nil, nil
	}

	out := make([]*netObj, 0, len(raw))

	for _, e := range raw {
		m, _ := e.(map[string]any)
		name := strOf(m, fieldName)

		if err := validNetName(name, netMaxName); err != nil {
			return nil, err
		}

		var prev *netObj
		if old != nil {
			prev, _ = old.kid(netPeerings, name)
		}

		o, err := p.normalizePeering(id, name, propsOf(m), prev, vnetSpaceOf(old, props))
		if err != nil {
			return nil, err
		}

		out = append(out, o)
	}

	return out, nil
}

func vnetSpaceOf(old *netParent, props map[string]any) []netip.Prefix {
	space, _ := props["addressSpace"].(map[string]any)
	out, _ := netParsePrefixes(strList(space[netFieldAddrPrefixes]))

	if len(out) == 0 && old != nil {
		return vnetSpace(&old.Obj)
	}

	return out
}

// checkExistingSubnets validates what an update leaves in place: subnets kept from the stored set must still fit the
// address space, and removed subnets must not be in use.
func (p *NetworkProvider) checkExistingSubnets(
	old *netParent, space []netip.Prefix, inline []*netObj, props map[string]any,
) error {
	_, replaced := props[netSubnets]
	kept := old.Kids[netSubnets]

	if replaced {
		kept = inline
		for _, s := range old.Kids[netSubnets] {
			if hasObj(inline, s.Name) {
				continue
			}

			if users := p.subnetUsers(subnetIDOf(old, s.Name)); len(users) > 0 {
				return inUseSubnet(s.Name, users[0])
			}
		}
	}

	for _, s := range kept {
		if !allInside(subnetPrefixes(s), space) {
			return netErr(http.StatusBadRequest, "NetcfgInvalidSubnet",
				"Subnet %s is not valid in virtual network %s.", s.Name, old.Obj.Name)
		}
	}

	return nil
}

func hasObj(list []*netObj, name string) bool {
	for _, o := range list {
		if strings.EqualFold(o.Name, name) {
			return true
		}
	}

	return false
}

func allInside(prefixes, space []netip.Prefix) bool {
	for _, pf := range prefixes {
		if !prefixInside(pf, space) {
			return false
		}
	}

	return true
}

func inUseSubnet(subnet, user string) error {
	return netErr(http.StatusBadRequest, "InUseSubnetCannotBeDeleted",
		"Subnet %s is in use by %s and cannot be deleted. In order to delete the subnet, delete all the resources "+
			"within the subnet. See aka.ms/deletesubnet.", subnet, user)
}

func subnetIDOf(vnet *netParent, name string) string {
	return vnetARMID(vnet) + "/" + netSubnets + "/" + name
}

func vnetARMID(vnet *netParent) string { return vnet.armID() }

func checkSubnetOverlap(list []*netObj) error {
	for i := range list {
		for j := i + 1; j < len(list); j++ {
			if prefixesOverlap(subnetPrefixes(list[i]), subnetPrefixes(list[j])) {
				return netErr(http.StatusBadRequest, "NetcfgSubnetRangesOverlap",
					"Subnet %s is not valid because its IP address range overlaps with that of Subnet %s.",
					list[i].Name, list[j].Name)
			}
		}
	}

	return nil
}

// normalizeSubnet validates props and returns the stored subnet.
func (p *NetworkProvider) normalizeSubnet(
	vnetID ResourceID, space []netip.Prefix, loc, name string, props map[string]any, prev *netObj,
) (*netObj, error) {
	prefixes := strList(props[netFieldAddrPrefixes])
	if single := strOf(props, netFieldAddrPrefix); single != "" && len(prefixes) == 0 {
		prefixes = []string{single}
	}

	if len(prefixes) == 0 {
		return nil, netBad("Subnet %s must specify addressPrefix or addressPrefixes.", name)
	}

	parsed, err := netParsePrefixes(prefixes)
	if err != nil {
		return nil, err
	}

	for _, pf := range parsed {
		if !prefixInside(pf, space) || (pf.Addr().Is4() && pf.Bits() > minSubnetBits) {
			return nil, netErr(http.StatusBadRequest, "NetcfgInvalidSubnet",
				"Subnet %s is not valid in virtual network %s. Its address prefix %s must be inside the virtual "+
					"network address space and no smaller than /%d.", name, vnetID.Names[0], pf, minSubnetBits)
		}
	}

	out := takeProps(map[string]any{fieldProperties: props}, netFieldAddrPrefix, fieldProvisioningState,
		"ipConfigurations", "privateEndpoints", "purpose", "resourceNavigationLinks", "serviceAssociationLinks",
		"ipConfigurationProfiles")
	out[netFieldAddrPrefixes] = prefixes

	if err = p.subnetRefs(out); err != nil {
		return nil, err
	}

	if err = normalizeSubnetPolicies(out); err != nil {
		return nil, err
	}

	if out["delegations"], err = normalizeDelegations(out["delegations"]); err != nil {
		return nil, err
	}

	if out["serviceEndpoints"], err = normalizeEndpoints(out["serviceEndpoints"], loc); err != nil {
		return nil, err
	}

	o := &netObj{Name: name, Etag: newEtag(), GUID: uuid.NewString(), Props: out}
	if prev != nil {
		o.GUID = prev.GUID
	}

	return o, nil
}

func (p *NetworkProvider) subnetRefs(props map[string]any) error {
	if id := subID(props, propSubnetNSG); id != "" {
		if _, ok := p.netRef(id, netNSGs); !ok {
			return refMissing(id)
		}

		props[propSubnetNSG] = map[string]any{netFieldID: id}
	}

	if id := subID(props, propNATGateway); id != "" {
		if _, ok := p.netRef(id, netNATs); !ok {
			return refMissing(id)
		}

		props[propNATGateway] = map[string]any{netFieldID: id}
	}

	if id := subID(props, propSubnetRT); id != "" {
		if _, ok := p.netRef(id, netRTs); !ok {
			return refMissing(id)
		}

		props[propSubnetRT] = map[string]any{netFieldID: id}
	}

	return nil
}

func normalizeSubnetPolicies(props map[string]any) error {
	for _, c := range []struct {
		key, def string
		ok       []string
	}{
		{"privateEndpointNetworkPolicies", netDisabled,
			[]string{netEnabled, netDisabled, "NetworkSecurityGroupEnabled", "RouteTableEnabled"}},
		{"privateLinkServiceNetworkPolicies", netEnabled, []string{netEnabled, netDisabled}},
	} {
		v := strOf(props, c.key)
		if v == "" {
			props[c.key] = c.def

			continue
		}

		canon := ""

		for _, o := range c.ok {
			if strings.EqualFold(o, v) {
				canon = o
			}
		}

		if canon == "" {
			return netBad("Invalid value %q for %s.", v, c.key)
		}

		props[c.key] = canon
	}

	return nil
}

func normalizeDelegations(v any) (any, error) {
	raw, _ := v.([]any)
	out := make([]any, 0, len(raw))

	for _, e := range raw {
		m, _ := e.(map[string]any)
		props := propsOf(m)
		svc := strOf(props, "serviceName")

		if strOf(m, fieldName) == "" || svc == "" {
			return nil, netBad("A subnet delegation requires a name and properties.serviceName.")
		}

		np := map[string]any{"serviceName": svc}

		if actions, ok := delegationActions[strings.ToLower(svc)]; ok {
			np["actions"] = actions
		} else if given := strList(props["actions"]); len(given) > 0 {
			np["actions"] = given
		}

		out = append(out, map[string]any{fieldName: strOf(m, fieldName), fieldProperties: np})
	}

	return out, nil
}

func normalizeEndpoints(v any, loc string) (any, error) {
	raw, _ := v.([]any)
	out := make([]any, 0, len(raw))

	for _, e := range raw {
		m, _ := e.(map[string]any)

		svc := strOf(m, "service")
		if svc == "" {
			return nil, netBad("A service endpoint requires the service property.")
		}

		locs := strList(m["locations"])
		if len(locs) == 0 {
			locs = []string{loc}
		}

		out = append(out, map[string]any{"service": svc, "locations": locs, fieldProvisioningState: netStatusSucceeded})
	}

	return out, nil
}

func (p *NetworkProvider) writeSubnet(vnet *netParent, name string, body map[string]any) (bool, error) {
	space := vnetSpace(&vnet.Obj)
	prev, idx := vnet.kid(netSubnets, name)
	vid := ResourceID{Names: []string{vnet.Obj.Name}}

	o, err := p.normalizeSubnet(vid, space, vnet.Obj.Location, name, propsOf(body), prev)
	if err != nil {
		return false, err
	}

	list := append([]*netObj{}, vnet.Kids[netSubnets]...)

	if idx >= 0 {
		list[idx] = o
	} else {
		list = append(list, o)
	}

	if err = checkSubnetOverlap(list); err != nil {
		return false, err
	}

	if prev != nil {
		for _, user := range p.subnetUsers(subnetIDOf(vnet, name)) {
			if !p.addrStillInside(user, o) {
				return false, netErr(http.StatusBadRequest, "InUseSubnetCannotBeUpdated",
					"Subnet %s is in use by %s and its address range cannot exclude the addresses in use.", name, user)
			}
		}
	}

	vnet.Kids[netSubnets] = list
	p.touch(vnet)

	return idx < 0, nil
}

func (p *NetworkProvider) bodyVNet(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	if _, ok := props["enableDdosProtection"]; !ok {
		props["enableDdosProtection"] = false
	}

	subnets := make([]any, 0, len(it.Kids[netSubnets]))
	for _, s := range it.Kids[netSubnets] {
		subnets = append(subnets, p.bodySubnet(kidID(id, netSubnets, s.Name), s))
	}

	peerings := make([]any, 0, len(it.Kids[netPeerings]))
	for _, s := range it.Kids[netPeerings] {
		peerings = append(peerings, p.bodyPeering(it, kidID(id, netPeerings, s.Name), s))
	}

	props[netSubnets] = subnets
	props["virtualNetworkPeerings"] = peerings
	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID

	return p.envelope(id, netVNets, &it.Obj, props)
}

func kidID(parent ResourceID, kt, name string) ResourceID {
	return ResourceID{
		SubscriptionID: parent.SubscriptionID, ResourceGroup: parent.ResourceGroup, Namespace: parent.Namespace,
		Types: []string{parent.Types[0], kt}, Names: []string{parent.Names[0], name},
	}
}

func (p *NetworkProvider) bodySubnet(id ResourceID, s *netObj) map[string]any {
	props := map[string]any{}

	maps.Copy(props, s.Props)

	prefixes := strList(s.Props[netFieldAddrPrefixes])
	if len(prefixes) == 1 {
		props[netFieldAddrPrefix] = prefixes[0]
	}

	delegs := make([]any, 0, len(anyList(s.Props["delegations"])))

	for _, d := range anyList(s.Props["delegations"]) {
		m, _ := d.(map[string]any)
		dp := map[string]any{}

		maps.Copy(dp, propsOf(m))

		dp[fieldProvisioningState] = netStatusSucceeded
		delegs = append(delegs, map[string]any{
			netFieldID: id.ARMID() + "/delegations/" + strOf(m, fieldName), fieldName: strOf(m, fieldName),
			fieldType: "Microsoft.Network/virtualNetworks/subnets/delegations", fieldProperties: dp,
			netFieldEtag: s.Etag,
		})
	}

	props["delegations"] = delegs

	if users := p.subnetUsers(id.ARMID()); len(users) > 0 {
		cfgs := make([]any, 0, len(users))
		for _, u := range users {
			cfgs = append(cfgs, map[string]any{netFieldID: u})
		}

		props["ipConfigurations"] = cfgs
	}

	if pes := p.peOnSubnet(id.ARMID()); len(pes) > 0 {
		props["privateEndpoints"] = idRefs(pes)
	}

	props[fieldProvisioningState] = netStatusSucceeded

	return map[string]any{
		netFieldID: id.ARMID(), fieldName: s.Name, fieldType: subnetTypePath, netFieldEtag: s.Etag,
		fieldProperties: props,
	}
}

func anyList(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	case []string:
		out := make([]any, len(t))
		for i, s := range t {
			out[i] = s
		}

		return out
	}

	return nil
}

// normalizePeering validates and stores a peering definition.
func (p *NetworkProvider) normalizePeering(
	local ResourceID, name string, props map[string]any, prev *netObj, localSpace []netip.Prefix,
) (*netObj, error) {
	remoteID := subID(props, "remoteVirtualNetwork")
	if remoteID == "" {
		return nil, netBad("Peering %s requires properties.remoteVirtualNetwork.id.", name)
	}

	remote, ok := p.netRef(remoteID, netVNets)
	if !ok {
		return nil, refMissing(remoteID)
	}

	if strings.EqualFold(remote.Obj.Name, local.Names[0]) && strings.EqualFold(remote.RG, local.ResourceGroup) {
		return nil, netBad("Peering %s cannot target its own virtual network.", name)
	}

	if prefixesOverlap(localSpace, vnetSpace(&remote.Obj)) {
		return nil, netErr(http.StatusBadRequest, "VnetAddressSpacesOverlap",
			"Cannot create or update peering %s because the address space of virtual network %s overlaps with "+
				"remote virtual network %s.", name, local.Names[0], remote.Obj.Name)
	}

	out := takeProps(map[string]any{fieldProperties: props}, "peeringState", "peeringSyncLevel", "remoteAddressSpace",
		"remoteVirtualNetworkAddressSpace", fieldProvisioningState, netFieldResourceGUID)
	out["remoteVirtualNetwork"] = map[string]any{netFieldID: remoteID}

	for k, def := range map[string]bool{
		"allowVirtualNetworkAccess": true, "allowForwardedTraffic": false, "allowGatewayTransit": false,
		"useRemoteGateways": false, "doNotVerifyRemoteGateways": false, "peerCompleteVnets": true,
	} {
		if _, set := out[k].(bool); !set {
			out[k] = def
		}
	}

	o := &netObj{Name: name, Etag: newEtag(), GUID: uuid.NewString(), Props: out, Aux: map[string]string{}}
	if prev != nil {
		o.GUID, o.Aux = prev.GUID, prev.Aux
	}

	return o, nil
}

func (p *NetworkProvider) writePeering(vnet *netParent, id ResourceID, name string, body map[string]any) (bool, error) {
	prev, idx := vnet.kid(netPeerings, name)

	o, err := p.normalizePeering(id, name, propsOf(body), prev, vnetSpace(&vnet.Obj))
	if err != nil {
		return false, err
	}

	if idx >= 0 {
		vnet.Kids[netPeerings][idx] = o
	} else {
		vnet.Kids[netPeerings] = append(vnet.Kids[netPeerings], o)
	}

	p.touch(vnet)

	return idx < 0, nil
}

func (p *NetworkProvider) bodyPeering(vnet *netParent, id ResourceID, k *netObj) map[string]any {
	props := map[string]any{}

	maps.Copy(props, k.Props)

	state := k.Aux[auxState]
	if state == "" {
		state = peerStateInitiated
	}

	props["peeringState"] = state
	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = k.GUID

	props["localAddressSpace"] = map[string]any{
		netFieldAddrPrefixes: strList(propsMap(vnet.Obj.Props["addressSpace"])[netFieldAddrPrefixes]),
	}
	props["localVirtualNetworkAddressSpace"] = props["localAddressSpace"]

	if remote, ok := p.netRef(subID(k.Props, "remoteVirtualNetwork"), netVNets); ok {
		sp, _ := remote.Obj.Props["addressSpace"].(map[string]any)
		props["remoteAddressSpace"] = map[string]any{netFieldAddrPrefixes: strList(sp[netFieldAddrPrefixes])}
		props["remoteVirtualNetworkAddressSpace"] = props["remoteAddressSpace"]
	}

	if state == peerStateConnected {
		props["peeringSyncLevel"] = "FullyInSync"
	}

	return map[string]any{
		netFieldID:      id.ARMID(),
		fieldName:       k.Name,
		fieldType:       "Microsoft.Network/virtualNetworks/virtualNetworkPeerings",
		netFieldEtag:    k.Etag,
		fieldProperties: props,
	}
}

// reconcilePeeringsLocked derives every peering state from whether the remote side has a matching peering.
func (p *NetworkProvider) reconcilePeeringsLocked() {
	for _, v := range p.ofType(netVNets) {
		for _, pe := range v.Kids[netPeerings] {
			remote, ok := p.netRef(subID(pe.Props, "remoteVirtualNetwork"), netVNets)
			reverse := false

			if ok {
				for _, rp := range remote.Kids[netPeerings] {
					rid := strings.ToLower(subID(rp.Props, "remoteVirtualNetwork"))
					if strings.HasSuffix(rid, "/virtualnetworks/"+strings.ToLower(v.Obj.Name)) &&
						strings.Contains(rid, "/resourcegroups/"+strings.ToLower(v.RG)+"/") {
						reverse = true
					}
				}
			}

			switch {
			case reverse:
				pe.Aux[auxState] = peerStateConnected
			case pe.Aux[auxState] == peerStateConnected || pe.Aux[auxState] == peerStateDisc:
				pe.Aux[auxState] = peerStateDisc
			default:
				pe.Aux[auxState] = peerStateInitiated
			}
		}
	}
}

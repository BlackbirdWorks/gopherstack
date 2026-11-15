package azurearm

import (
	"maps"
	"net/http"
	"net/netip"
	"slices"
	"strings"

	"github.com/google/uuid"
)

const (
	netLNGs          = "localNetworkGateways"
	netVNGs          = "virtualNetworkGateways"
	netConns         = "connections"
	netERCs          = "expressRouteCircuits"
	ercPeerings      = "peerings"
	ercAuths         = "authorizations"
	vpnGatewaySubnet = "GatewaySubnet"
	vpnSKUBasic      = agwRuleBasic
	vpnTierStandard  = skuTierStandard
	vpnStatusUnknown = unknownOperation
	vpnExpressRoute  = "ExpressRoute"
	vpnTypeVpn       = "Vpn"
	vpnActiveConfigs = 2
)

// vpnReservedASNs are the BGP autonomous system numbers Azure reserves
// (learn.microsoft.com/azure/vpn-gateway/vpn-gateway-bgp-overview).
func vpnReservedASNs() []uint32 {
	return []uint32{8074, 8075, 12076, 23456, 65515, 65517, 65518, 65519, 65520}
}

func vpnGatewaySKUs() []string {
	return []string{
		vpnSKUBasic, "VpnGw1", "VpnGw2", "VpnGw3", "VpnGw4", "VpnGw5", "VpnGw1AZ", "VpnGw2AZ", "VpnGw3AZ", "VpnGw4AZ",
		"VpnGw5AZ",
	}
}

func erGatewaySKUs() []string {
	return []string{vpnTierStandard, "HighPerformance", "UltraPerformance", "ErGw1AZ", "ErGw2AZ", "ErGw3AZ"}
}

func ercPeeringTypes() []string {
	return []string{"AzurePrivatePeering", "AzurePublicPeering", "MicrosoftPeering"}
}

func parseIPv4Prefix(s string) bool {
	pfx, err := netip.ParsePrefix(s)

	return err == nil && pfx.Addr().Is4() && pfx == pfx.Masked()
}

func (p *NetworkProvider) putVPNTop(top string, id ResourceID, body map[string]any) (map[string]any, bool, error) {
	switch top {
	case netLNGs:
		return p.putLNG(id, body)
	case netVNGs:
		return p.putVNG(id, body)
	case netConns:
		return p.putConn(id, body)
	default:
		return p.putERC(id, body)
	}
}

func (p *NetworkProvider) bodyVPNTop(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID

	sku := props["sku"]
	if it.Type == netERCs {
		delete(props, "sku")
	}

	switch it.Type {
	case netConns:
		props["connectionStatus"] = vpnStatusUnknown
		props["egressBytesTransferred"], props["ingressBytesTransferred"] = 0, 0
		props["tunnelConnectionStatus"] = []any{}
	case netERCs:
		peerings := make([]any, 0, len(it.Kids[ercPeerings]))
		for _, k := range it.Kids[ercPeerings] {
			peerings = append(peerings, p.bodyERCKid(id, ercPeerings, k))
		}

		auths := make([]any, 0, len(it.Kids[ercAuths]))
		for _, k := range it.Kids[ercAuths] {
			auths = append(auths, p.bodyERCKid(id, ercAuths, k))
		}

		props[ercPeerings], props[ercAuths] = peerings, auths
		props["circuitProvisioningState"] = flexEnabled
		props["serviceProviderProvisioningState"] = "NotProvisioned"
		props["serviceKey"] = it.Obj.GUID
	case netVNGs:
		props["vpnGatewayGeneration"] = strOr(props, "vpnGatewayGeneration", "None")
	}

	out := p.envelope(id, it.Type, &it.Obj, props)
	if it.Type == netERCs {
		out["sku"] = sku
	}

	return out
}

func strOr(m map[string]any, k, d string) string {
	if v := strOf(m, k); v != "" {
		return v
	}

	return d
}

func (p *NetworkProvider) putLNG(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := takeProps(body, fieldProvisioningState, netFieldResourceGUID)

	for _, pfx := range strList(mapOf(props["localNetworkAddressSpace"])["addressPrefixes"]) {
		if !parseIPv4Prefix(pfx) {
			return nil, false, netErr(
				http.StatusBadRequest,
				netCodeCIDR,
				"The address prefix %s is not a valid CIDR in canonical form.",
				pfx,
			)
		}
	}

	ip, fqdn := strOf(props, "gatewayIpAddress"), strOf(props, "fqdn")
	if (ip == "") == (fqdn == "") {
		return nil, false, netBad("Exactly one of gatewayIpAddress and fqdn must be set on a local network gateway.")
	}

	if _, err := netip.ParseAddr(ip); ip != "" && err != nil {
		return nil, false, netBad("The gatewayIpAddress %s is not a valid IP address.", ip)
	}

	if err := checkBGPSettings(mapOf(props["bgpSettings"])); err != nil {
		return nil, false, err
	}

	it, _, created := p.upsertTop(netLNGs, id, body)
	it.Obj.Props = props

	return p.bodyVPNTop(id, it), created, nil
}

func checkBGPSettings(b map[string]any) error {
	if len(b) == 0 {
		return nil
	}

	asn, ok := b["asn"].(float64)
	if !ok || asn < 0 || asn > 4294967295 {
		return netBad("The BGP asn must be a number between 0 and 4294967295.")
	}

	if slices.Contains(vpnReservedASNs(), uint32(asn)) || (asn >= 64496 && asn <= 64511) ||
		(asn >= 65535 && asn <= 65551) {
		return netBad("The BGP asn %d is reserved.", uint32(asn))
	}

	if addr := strOf(b, "bgpPeeringAddress"); addr != "" {
		if _, err := netip.ParseAddr(addr); err != nil {
			return netBad("The bgpPeeringAddress %s is not a valid IP address.", addr)
		}
	}

	return nil
}

func (p *NetworkProvider) putVNG(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := takeProps(body, fieldProvisioningState, netFieldResourceGUID)

	gwType, ok := canonEnum(strOf(props, "gatewayType"), []string{vpnTypeVpn, vpnExpressRoute})
	if !ok {
		return nil, false, netBad("The gatewayType must be Vpn or ExpressRoute.")
	}

	props["gatewayType"] = gwType

	sku := mapOf(props["sku"])
	skus := vpnGatewaySKUs()

	if gwType == vpnExpressRoute {
		skus = erGatewaySKUs()
	}

	skuName, ok := canonEnum(strOf(sku, fieldName), skus)
	if !ok {
		return nil, false, netBad("The sku name '%s' is not valid for a %s gateway.", strOf(sku, fieldName), gwType)
	}

	sku[fieldName], sku["tier"] = skuName, skuName
	props["sku"] = sku

	if err := checkVPNType(props, gwType, skuName); err != nil {
		return nil, false, err
	}

	_, err := p.checkGatewayIPConfigs(id, props, gwType)
	if err != nil {
		return nil, false, err
	}

	if err = checkBGPSettings(mapOf(props["bgpSettings"])); err != nil {
		return nil, false, err
	}

	if enable, _ := props["enableBgp"].(bool); enable && skuName == vpnSKUBasic {
		return nil, false, netBad("BGP is not supported on the Basic gateway SKU.")
	}

	it, _, created := p.upsertTop(netVNGs, id, body)
	it.Obj.Props = props

	return p.bodyVPNTop(id, it), created, nil
}

func checkVPNType(props map[string]any, gwType, skuName string) error {
	if gwType != vpnTypeVpn {
		if props["vpnType"] != nil {
			return netBad("vpnType applies to VPN gateways only.")
		}

		return nil
	}

	vpnType, ok := canonEnum(strOr(props, "vpnType", "RouteBased"), []string{"RouteBased", "PolicyBased"})
	if !ok {
		return netBad("The vpnType must be RouteBased or PolicyBased.")
	}

	props["vpnType"] = vpnType

	if vpnType == "PolicyBased" && skuName != vpnSKUBasic {
		return netBad("PolicyBased VPN gateways are supported on the Basic SKU only.")
	}

	if active, _ := props["activeActive"].(bool); active && skuName == vpnSKUBasic {
		return netBad("Active-active is not supported on the Basic gateway SKU.")
	}

	return nil
}

// checkGatewayIPConfigs validates a gateway's IP configurations: GatewaySubnet of one virtual network, public IP
// addresses that exist and are not held by another gateway, two configurations when active-active, and one gateway
// of each type per virtual network.
func (p *NetworkProvider) checkGatewayIPConfigs(id ResourceID, props map[string]any, gwType string) (string, error) {
	cfgs := anyList(props["ipConfigurations"])
	if len(cfgs) == 0 {
		return "", netBad("A virtual network gateway requires at least one IP configuration.")
	}

	if active, _ := props["activeActive"].(bool); active != (len(cfgs) == vpnActiveConfigs) && gwType == vpnTypeVpn {
		return "", netBad(
			"An active-active gateway needs exactly two IP configurations, an active-standby one exactly one.",
		)
	}

	self := strings.ToLower(id.ARMID())
	vnetID := ""

	for i, c := range cfgs {
		v, err := p.checkGatewayConfig(self, i, mapOf(c))
		if err != nil {
			return "", err
		}

		if vnetID != "" && !strings.EqualFold(vnetID, v) {
			return "", netBad("All IP configurations of a gateway must use the same virtual network.")
		}

		vnetID = v
	}

	if other := p.gatewayOnVNet(self, vnetID, gwType); other != "" {
		return "", netBad("The virtual network %s already has a %s gateway.", vnetID, gwType)
	}

	return vnetID, nil
}

// checkGatewayConfig validates one IP configuration and returns the virtual network of its GatewaySubnet.
func (p *NetworkProvider) checkGatewayConfig(self string, idx int, m map[string]any) (string, error) {
	cp := propsOf(m)

	if strOf(m, fieldName) == "" {
		return "", netBad("IP configuration %d needs a name.", idx)
	}

	sid := subID(cp, "subnet")

	v, sub, ok := p.subnetByID(sid)
	if !ok {
		return "", refMissing(sid)
	}

	if !strings.EqualFold(sub.Name, vpnGatewaySubnet) {
		return "", netBad("The gateway subnet must be named %s.", vpnGatewaySubnet)
	}

	pid := subID(cp, "publicIPAddress")
	if _, found := p.netRef(pid, netPIPs); !found {
		return "", refMissing(pid)
	}

	for _, other := range p.ofType(netVNGs) {
		if strings.EqualFold(other.armID(), self) {
			continue
		}

		for _, oc := range anyList(other.Obj.Props["ipConfigurations"]) {
			if strings.EqualFold(subID(propsOf(mapOf(oc)), "publicIPAddress"), pid) {
				return "", netErr(http.StatusBadRequest, "PublicIPAddressInUse",
					"Public IP address %s is already used by gateway %s.", pid, other.armID())
			}
		}
	}

	return v.armID(), nil
}

// gatewayOnVNet returns another gateway of gwType whose GatewaySubnet lies in vnetID.
func (p *NetworkProvider) gatewayOnVNet(self, vnetID, gwType string) string {
	for _, other := range p.ofType(netVNGs) {
		if strings.EqualFold(other.armID(), self) || !strings.EqualFold(strOf(other.Obj.Props, "gatewayType"), gwType) {
			continue
		}

		oc := anyList(other.Obj.Props["ipConfigurations"])
		if len(oc) > 0 && strings.EqualFold(vnetOfSubnet(subID(propsOf(mapOf(oc[0])), "subnet")), vnetID) {
			return other.armID()
		}
	}

	return ""
}

func vnetOfSubnet(subnetID string) string {
	if i := strings.LastIndex(strings.ToLower(subnetID), "/subnets/"); i > 0 {
		return subnetID[:i]
	}

	return subnetID
}

func (p *NetworkProvider) putConn(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := takeProps(body, fieldProvisioningState, netFieldResourceGUID, "connectionStatus", "tunnelConnectionStatus",
		"egressBytesTransferred", "ingressBytesTransferred")

	kind, ok := canonEnum(strOf(props, "connectionType"), []string{"IPsec", "Vnet2Vnet", vpnExpressRoute, "VPNClient"})
	if !ok {
		return nil, false, netBad("The connectionType must be IPsec, Vnet2Vnet, ExpressRoute or VPNClient.")
	}

	props["connectionType"] = kind

	g1 := subID(props, "virtualNetworkGateway1")
	if _, found := p.netRef(g1, netVNGs); !found {
		return nil, false, refMissing(g1)
	}

	if err := p.checkConnPeer(props, kind, g1); err != nil {
		return nil, false, err
	}

	if w, has := props["routingWeight"].(float64); has && (w < 0 || w > 32000) {
		return nil, false, netBad("The routingWeight must be between 0 and 32000.")
	}

	if proto := strOf(props, "connectionProtocol"); proto != "" {
		canon, valid := canonEnum(proto, []string{"IKEv1", "IKEv2"})
		if !valid {
			return nil, false, netBad("The connectionProtocol must be IKEv1 or IKEv2.")
		}

		props["connectionProtocol"] = canon
	}

	it, _, created := p.upsertTop(netConns, id, body)
	it.Obj.Props = props

	return p.bodyVPNTop(id, it), created, nil
}

func (p *NetworkProvider) checkConnPeer(props map[string]any, kind, g1 string) error {
	switch kind {
	case "IPsec":
		l := subID(props, "localNetworkGateway2")
		if _, found := p.netRef(l, netLNGs); !found {
			return refMissing(l)
		}

		if strOf(props, "sharedKey") == "" {
			return netBad("A sharedKey is required for an IPsec connection.")
		}
	case "Vnet2Vnet":
		g2 := subID(props, "virtualNetworkGateway2")
		if _, found := p.netRef(g2, netVNGs); !found {
			return refMissing(g2)
		}

		if strings.EqualFold(g1, g2) {
			return netBad("A Vnet2Vnet connection needs two different gateways.")
		}

		if strOf(props, "sharedKey") == "" {
			return netBad("A sharedKey is required for a Vnet2Vnet connection.")
		}
	case vpnExpressRoute:
		peer := subID(props, "peer")
		if _, found := p.netRef(strings.Split(strings.ToLower(peer), "/peerings/")[0], netERCs); !found {
			return refMissing(peer)
		}
	}

	return nil
}

func (p *NetworkProvider) putERC(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := takeProps(body, ercPeerings, ercAuths, fieldProvisioningState, netFieldResourceGUID, "serviceKey",
		"circuitProvisioningState", "serviceProviderProvisioningState")

	sku := mapOf(body["sku"])

	name, ok := canonEnum(
		strOf(sku, fieldName),
		[]string{
			"Standard_MeteredData",
			"Standard_UnlimitedData",
			"Premium_MeteredData",
			"Premium_UnlimitedData",
			"Local_MeteredData",
			"Local_UnlimitedData",
			"Basic_MeteredData",
			"Basic_UnlimitedData",
		},
	)
	if !ok {
		return nil, false, netBad("The sku name must be {Standard|Premium|Local|Basic}_{MeteredData|UnlimitedData}.")
	}

	sku[fieldName] = name
	sku["tier"], sku["family"], _ = strings.Cut(name, "_")

	sp := mapOf(props["serviceProviderProperties"])
	if len(sp) == 0 && props["expressRoutePort"] == nil {
		return nil, false, netBad("Either serviceProviderProperties or expressRoutePort is required.")
	}

	if len(sp) > 0 {
		if strOf(sp, "serviceProviderName") == "" || strOf(sp, "peeringLocation") == "" {
			return nil, false, netBad("serviceProviderName and peeringLocation are required.")
		}

		if bw, has := sp["bandwidthInMbps"].(float64); !has || bw <= 0 {
			return nil, false, netBad("bandwidthInMbps must be a positive number.")
		}
	}

	old, _ := p.top(netERCs, id.ResourceGroup, id.Names[0])

	peerings, err := p.inlineKids(body, ercPeerings, ercPeerings, old, normalizeERCPeering)
	if err != nil {
		return nil, false, err
	}

	auths, err := p.inlineKids(body, ercAuths, ercAuths, old, normalizeERCAuth)
	if err != nil {
		return nil, false, err
	}

	if err = checkERCPeerings(peerings); err != nil {
		return nil, false, err
	}

	it, obj, created := p.upsertTop(netERCs, id, body)
	obj.Props = props

	obj.Props["sku"] = sku

	if peerings != nil {
		it.Kids[ercPeerings] = peerings
	}

	if auths != nil {
		it.Kids[ercAuths] = auths
	}

	return p.bodyVPNTop(id, it), created, nil
}

func checkERCPeerings(list []*netObj) error {
	vlans := map[int]string{}

	for _, k := range list {
		v, _ := k.Props["vlanId"].(float64)
		if other, dup := vlans[int(v)]; dup {
			return netBad("Peerings %s and %s use the same vlanId %d.", other, k.Name, int(v))
		}

		vlans[int(v)] = k.Name
	}

	return nil
}

func normalizeERCPeering(name string, props map[string]any, prev *netObj) (*netObj, error) {
	out := takeProps(map[string]any{fieldProperties: props}, fieldProvisioningState, "azureASN", "gatewayManagerEtag",
		"lastModifiedBy", "stats")

	kind, ok := canonEnum(name, ercPeeringTypes())
	if !ok {
		return nil, netBad("The peering name must be AzurePrivatePeering, AzurePublicPeering or MicrosoftPeering.")
	}

	out["peeringType"] = kind

	if asn, has := out["peerASN"].(float64); !has || asn <= 0 || asn > 4294967295 {
		return nil, netBad("peerASN must be a positive number up to 4294967295.")
	}

	for _, k := range []string{"primaryPeerAddressPrefix", "secondaryPeerAddressPrefix"} {
		if !parseIPv4Prefix(strOf(out, k)) {
			return nil, netErr(http.StatusBadRequest, netCodeCIDR, "%s must be a canonical IPv4 CIDR.", k)
		}
	}

	if vlan, has := out["vlanId"].(float64); !has || vlan < 1 || vlan > 4094 {
		return nil, netBad("vlanId must be between 1 and 4094.")
	}

	state, valid := canonEnum(strOr(out, "state", flexEnabled), []string{flexEnabled, apimDisabled})
	if !valid {
		return nil, netBad("The peering state must be Enabled or Disabled.")
	}

	out["state"] = state

	if kind == "MicrosoftPeering" &&
		len(strList(mapOf(out["microsoftPeeringConfig"])["advertisedPublicPrefixes"])) == 0 {
		return nil, netBad("MicrosoftPeering requires microsoftPeeringConfig.advertisedPublicPrefixes.")
	}

	o := &netObj{Name: kind, Etag: newEtag(), GUID: uuid.NewString(), Props: out}
	if prev != nil {
		o.GUID = prev.GUID
	}

	return o, nil
}

func normalizeERCAuth(name string, props map[string]any, prev *netObj) (*netObj, error) {
	out := takeProps(map[string]any{fieldProperties: props}, fieldProvisioningState, "authorizationKey",
		"authorizationUseStatus")

	o := &netObj{Name: name, Etag: newEtag(), GUID: uuid.NewString(), Props: out}
	if prev != nil {
		o.GUID = prev.GUID
	}

	return o, nil
}

func (p *NetworkProvider) bodyERCKid(parent ResourceID, kt string, k *netObj) map[string]any {
	props := maps.Clone(k.Props)
	props[fieldProvisioningState] = netStatusSucceeded

	typ := "Microsoft.Network/expressRouteCircuits/" + kt

	if kt == ercAuths {
		props["authorizationKey"] = k.GUID
		props["authorizationUseStatus"] = "Available"
	} else {
		props["azureASN"] = 12076
		props["state"] = strOr(props, "state", flexEnabled)
	}

	return map[string]any{
		netFieldID: kidID(parent, kt, k.Name).ARMID(), fieldName: k.Name, fieldType: typ, netFieldEtag: k.Etag,
		fieldProperties: props,
	}
}

func (p *NetworkProvider) writeERCKid(parent *netParent, kt, name string, body map[string]any) (bool, error) {
	prev, idx := parent.kid(kt, name)

	normalize := normalizeERCPeering
	if kt == ercAuths {
		normalize = normalizeERCAuth
	}

	o, err := normalize(name, propsOf(body), prev)
	if err != nil {
		return false, err
	}

	list := slices.Clone(parent.Kids[kt])
	if idx >= 0 {
		list[idx] = o
	} else {
		list = append(list, o)
	}

	if kt == ercPeerings {
		if err = checkERCPeerings(list); err != nil {
			return false, err
		}
	}

	parent.Kids[kt] = list
	p.touch(parent)

	return idx < 0, nil
}

// checkVPNTopDelete refuses removing a gateway, local network gateway or circuit a connection still uses.
func (p *NetworkProvider) checkVPNTopDelete(it *netParent) error {
	self := it.armID()

	for _, c := range p.ofType(netConns) {
		for _, k := range []string{"virtualNetworkGateway1", "virtualNetworkGateway2", "localNetworkGateway2", "peer"} {
			ref := subID(c.Obj.Props, k)
			if strings.EqualFold(ref, self) || strings.HasPrefix(strings.ToLower(ref), strings.ToLower(self)+"/") {
				return netErr(http.StatusBadRequest, "InUseConnectionCannotBeDeleted",
					"%s is used by connection %s and cannot be deleted.", self, c.armID())
			}
		}
	}

	return nil
}

func parentID(kid ResourceID) ResourceID {
	return ResourceID{
		SubscriptionID: kid.SubscriptionID, ResourceGroup: kid.ResourceGroup, Namespace: kid.Namespace,
		Types: kid.Types[:1], Names: kid.Names[:1],
	}
}

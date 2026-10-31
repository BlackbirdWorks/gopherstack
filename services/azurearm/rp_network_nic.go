package azurearm

import (
	"maps"
	"net/http"
	"net/netip"
	"regexp"
	"slices"
	"strings"
)

const (
	pipTypePath        = "Microsoft.Network/publicIPAddresses"
	ipConfigTypePath   = "Microsoft.Network/networkInterfaces/ipConfigurations"
	methodStatic       = "Static"
	methodDynamic      = "Dynamic"
	versionV4          = "IPv4"
	versionV6          = "IPv6"
	auxIP              = "ip"
	skuBasic           = "Basic"
	skuStandard        = skuTierStandard
	defaultIdleTimeout = 4
	minIdleTimeout     = 4
	maxIdleTimeout     = 30
	maxSuggestions     = 5
	nicMACPrefix       = "00-0D-3A-"
)

//nolint:gochecknoglobals // read-only tables
var (
	dnsLabelRe = regexp.MustCompile(`^[a-z][a-z0-9-]{1,61}[a-z0-9]$`)
	// pipRanges are the RFC 5737 documentation ranges; the emulator hands out public addresses from them in order,
	// lowest free first, so they never collide with real hosts.
	pipRanges = []netip.Prefix{
		netip.MustParsePrefix("192.0.2.0/24"),
		netip.MustParsePrefix("198.51.100.0/24"),
		netip.MustParsePrefix("203.0.113.0/24"),
	}
	pipV6Base = netip.MustParseAddr("2001:db8::")
)

func (p *NetworkProvider) usedPIPAddrs() map[string]bool {
	used := map[string]bool{}

	for _, ip := range p.ofType(netPIPs) {
		if a := ip.Obj.Aux[auxIP]; a != "" {
			used[a] = true
		}
	}

	for _, pf := range p.ofType(netPrefixes) {
		if r, ok := p.prefixRange(pf); ok {
			for a := r.Addr(); r.Contains(a); a = a.Next() {
				used[a.String()] = true
			}
		}
	}

	return used
}

func (p *NetworkProvider) allocPIP(version string) (string, error) {
	used := p.usedPIPAddrs()

	if version == versionV6 {
		a := pipV6Base.Next()
		for used[a.String()] {
			a = a.Next()
		}

		return a.String(), nil
	}

	for _, pf := range pipRanges {
		base := v4u(pf.Addr())

		for off := uint32(1); off < 255; off++ {
			if a := u4(base + off).String(); !used[a] {
				return a, nil
			}
		}
	}

	return "", netErr(http.StatusBadRequest, "PublicIPCountLimitReached",
		"No more public IP addresses are available in the emulator's documentation ranges.")
}

type pipSettings struct {
	skuName, tier, method, version string
	idle                           int
}

func parsePIPSettings(name string, body map[string]any) (pipSettings, error) {
	props := propsOf(body)
	skuIn, _ := body[netFieldSKU].(map[string]any)
	st := pipSettings{idle: defaultIdleTimeout}

	var ok [4]bool

	st.skuName, ok[0] = canonEnum(firstNonEmpty(strOf(skuIn, fieldName), skuBasic), []string{skuBasic, skuStandard})
	st.tier, ok[1] = canonEnum(firstNonEmpty(strOf(skuIn, netFieldTier), "Regional"), []string{"Regional", "Global"})
	st.method, ok[2] = canonEnum(firstNonEmpty(strOf(props, "publicIPAllocationMethod"), methodDynamic),
		[]string{methodStatic, methodDynamic})
	st.version, ok[3] = canonEnum(firstNonEmpty(strOf(props, "publicIPAddressVersion"), versionV4),
		[]string{versionV4, versionV6})

	switch {
	case !ok[0]:
		return st, netBad("Public IP address SKU name must be Basic or Standard.")
	case !ok[1] || (st.tier == "Global" && st.skuName != skuStandard):
		return st, netBad("Public IP address SKU tier must be Regional, or Global with the Standard SKU.")
	case !ok[2]:
		return st, netBad("publicIPAllocationMethod must be Static or Dynamic.")
	case !ok[3]:
		return st, netBad("publicIPAddressVersion must be IPv4 or IPv6.")
	case st.skuName == skuStandard && st.method != methodStatic:
		return st, netBad("Standard SKU public IP address %s must use the Static allocation method.", name)
	}

	if v, set := props["idleTimeoutInMinutes"]; set {
		st.idle = intOf(v)
	}

	if st.idle < minIdleTimeout || st.idle > maxIdleTimeout {
		return st, netBad("idleTimeoutInMinutes must be between %d and %d.", minIdleTimeout, maxIdleTimeout)
	}

	return st, nil
}

func (p *NetworkProvider) putPIP(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)

	st, err := parsePIPSettings(id.Names[0], body)
	if err != nil {
		return nil, false, err
	}

	skuName, tier, method, version, idle := st.skuName, st.tier, st.method, st.version, st.idle
	prefID := subID(props, propPrefixRef)

	loc := normLocation(strOf(body, fieldLocation))
	old, existed := p.top(netPIPs, id.ResourceGroup, id.Names[0])

	if loc == "" && existed {
		loc = old.Obj.Location
	}

	dns, err := p.pipDNS(id, props, loc)
	if err != nil {
		return nil, false, err
	}

	ip := ""
	if existed && old.Obj.Props["publicIPAddressVersion"] == version {
		ip = old.Obj.Aux[auxIP]
	}

	if prefID != "" {
		if ip, err = p.bindPIPPrefix(id, st, loc, strOf(props, "ipAddress"), props); err != nil {
			return nil, false, err
		}
	}

	if method == methodStatic && ip == "" {
		if ip, err = p.allocPIP(version); err != nil {
			return nil, false, err
		}
	}

	it, obj, created := p.upsertTop(netPIPs, id, body)
	obj.Props = takeProps(body, "ipAddress", "ipConfiguration", fieldProvisioningState, netFieldResourceGUID,
		"linkedPublicIPAddress", "servicePublicIPAddress", propNATGateway, propPrefixRef)

	if prefID != "" {
		obj.Props[propPrefixRef] = idRef(prefID)
	}

	obj.Props["publicIPAllocationMethod"], obj.Props["publicIPAddressVersion"] = method, version
	obj.Props["idleTimeoutInMinutes"] = idle
	obj.Extra = map[string]any{netFieldSKU: map[string]any{fieldName: skuName, netFieldTier: tier}}

	if z, has := body[netFieldZones]; has {
		obj.Extra[netFieldZones] = z
	}

	if dns != nil {
		obj.Props["dnsSettings"] = dns
	}

	obj.Aux[auxIP] = ip

	return p.bodyPIP(id, it), created, nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

func (p *NetworkProvider) pipDNS(id ResourceID, props map[string]any, loc string) (map[string]any, error) {
	d, _ := props["dnsSettings"].(map[string]any)
	if d == nil {
		return nil, nil //nolint:nilnil // no DNS settings requested
	}

	out := map[string]any{}

	maps.Copy(out, d)

	delete(out, "fqdn")

	label := strOf(d, "domainNameLabel")
	if label == "" {
		return out, nil
	}

	label = strings.ToLower(label)
	if !dnsLabelRe.MatchString(label) {
		return nil, netBad("The domain name label %s is invalid.", strOf(d, "domainNameLabel"))
	}

	for _, o := range p.ofType(netPIPs) {
		dn, _ := o.Obj.Props["dnsSettings"].(map[string]any)

		if strings.EqualFold(strOf(dn, "domainNameLabel"), label) && o.Obj.Location == loc &&
			(!strings.EqualFold(o.Obj.Name, id.Names[0]) || !strings.EqualFold(o.RG, id.ResourceGroup)) {
			return nil, netErr(http.StatusConflict, "DnsRecordInUse",
				"DNS record %s.%s.cloudapp.azure.com is already used by another public IP.", label, loc)
		}
	}

	out["domainNameLabel"], out["fqdn"] = label, label+"."+loc+".cloudapp.azure.com"

	return out, nil
}

func (p *NetworkProvider) bodyPIP(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	if ip := it.Obj.Aux[auxIP]; ip != "" {
		props["ipAddress"] = ip
	}

	if user := p.pipUser(it.armID()); user != "" {
		props["ipConfiguration"] = map[string]any{netFieldID: user}
	}

	if nat := p.natOfPIP(it.armID()); nat != "" {
		props[propNATGateway] = idRef(nat)
	}

	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID

	return p.envelope(id, netPIPs, &it.Obj, props)
}

// pipUser returns the id of the NIC IP configuration the public IP is attached to, or "".
func (p *NetworkProvider) pipUser(pipID string) string {
	for _, n := range p.ofType(netNICs) {
		for _, c := range anyList(n.Obj.Props[netIPConfigs]) {
			m, _ := c.(map[string]any)
			if strings.EqualFold(subID(propsOf(m), "publicIPAddress"), pipID) {
				return n.armID() + "/" + netIPConfigs + "/" + strOf(m, fieldName)
			}
		}
	}

	if f := p.lbFrontendUsers("publicIPAddress", pipID); len(f) > 0 {
		return f[0]
	}

	return ""
}

// --- network interfaces ---

func (p *NetworkProvider) subnetByID(subnetID string) (*netParent, *netObj, bool) {
	rid, err := ParseGenericResourcePath(subnetID)
	if err != nil || len(rid.Types) != 2 || !strings.EqualFold(rid.Types[0], netVNets) ||
		!strings.EqualFold(rid.Types[1], netSubnets) {
		return nil, nil, false
	}

	v, ok := p.top(netVNets, rid.ResourceGroup, rid.Names[0])
	if !ok {
		return nil, nil, false
	}

	s, idx := v.kid(netSubnets, rid.Names[1])

	return v, s, idx >= 0
}

// usedAddrs maps a lowercase subnet id to the private addresses its NIC IP configurations hold, skipping skipNIC.
func (p *NetworkProvider) usedAddrs(skipNIC string) map[string]map[string]string {
	return p.usedAddrsSkip(skipNIC, "")
}

// usedAddrsSkip also counts load balancer frontend addresses, skipping skipLB.
func (p *NetworkProvider) usedAddrsSkip(skipNIC, skipLB string) map[string]map[string]string {
	used := map[string]map[string]string{}

	for _, lb := range p.ofType(netLBs) {
		if strings.EqualFold(lb.armID(), skipLB) {
			continue
		}

		for _, f := range lb.Kids[lbFrontends] {
			key := strings.ToLower(subID(f.Props, netFieldSubnet))
			if key == "" {
				continue
			}

			if used[key] == nil {
				used[key] = map[string]string{}
			}

			used[key][strOf(f.Props, "privateIPAddress")] = lb.armID() + "/" + lbFrontends + "/" + f.Name
		}
	}

	for _, n := range p.ofType(netNICs) {
		if strings.EqualFold(n.armID(), skipNIC) {
			continue
		}

		for _, c := range anyList(n.Obj.Props[netIPConfigs]) {
			m, _ := c.(map[string]any)
			cp := propsOf(m)
			key := strings.ToLower(subID(cp, netFieldSubnet))

			if used[key] == nil {
				used[key] = map[string]string{}
			}

			used[key][strOf(cp, "privateIPAddress")] = n.armID() + "/" + netIPConfigs + "/" + strOf(m, fieldName)
		}
	}

	return used
}

func (p *NetworkProvider) subnetUsers(subnetID string) []string {
	var out []string

	for _, n := range p.ofType(netNICs) {
		for _, c := range anyList(n.Obj.Props[netIPConfigs]) {
			m, _ := c.(map[string]any)
			if strings.EqualFold(subID(propsOf(m), netFieldSubnet), subnetID) {
				out = append(out, n.armID()+"/"+netIPConfigs+"/"+strOf(m, fieldName))
			}
		}
	}

	out = append(out, p.lbFrontendUsers(netFieldSubnet, subnetID)...)

	return out
}

// addrStillInside reports whether the IP configuration still has a valid address once its subnet becomes s.
func (p *NetworkProvider) addrStillInside(ipConfigID string, s *netObj) bool {
	for _, n := range p.ofType(netNICs) {
		for _, c := range anyList(n.Obj.Props[netIPConfigs]) {
			m, _ := c.(map[string]any)
			if !strings.EqualFold(n.armID()+"/"+netIPConfigs+"/"+strOf(m, fieldName), ipConfigID) {
				continue
			}

			a, err := netip.ParseAddr(strOf(propsOf(m), "privateIPAddress"))
			if err != nil {
				return true
			}

			for _, pf := range subnetPrefixes(s) {
				if pf.Contains(a) && !subnetReserved(pf, a) {
					return true
				}
			}

			return false
		}
	}

	return true
}

// assignIP picks the private address for a NIC IP configuration.
func assignIP(
	cfgID, method, want string, prefixes []netip.Prefix, taken map[string]string, prev string,
) (string, error) {
	if method == methodStatic {
		return assignStatic(cfgID, want, prefixes, taken)
	}

	if a, err := netip.ParseAddr(prev); err == nil && prev != "" {
		for _, pf := range prefixes {
			if pf.Contains(a) && !subnetReserved(pf, a) && taken[prev] == "" {
				return prev, nil
			}
		}
	}

	for _, pf := range prefixes {
		lo, hi, ok := subnetRange(pf)
		if !ok {
			continue
		}

		for v := lo; v <= hi; v++ {
			if a := u4(v).String(); taken[a] == "" {
				return a, nil
			}
		}
	}

	return "", netErr(http.StatusBadRequest, "SubnetIsFull", "Subnet has no free IP addresses left.")
}

func assignStatic(cfgID, want string, prefixes []netip.Prefix, taken map[string]string) (string, error) {
	a, err := netip.ParseAddr(want)
	if err != nil {
		return "", netBad("IP configuration %s requires a valid privateIPAddress for static allocation.", cfgID)
	}

	for _, pf := range prefixes {
		if !pf.Contains(a) {
			continue
		}

		if subnetReserved(pf, a) {
			return "", netErr(http.StatusBadRequest, "PrivateIPAddressIsReserved",
				"IP configuration %s uses the private IP address %s, which is reserved by Azure in the subnet.",
				cfgID, want)
		}

		if user := taken[a.String()]; user != "" {
			return "", netErr(http.StatusBadRequest, "PrivateIPAddressIsAllocated",
				"IP configuration %s is using the private IP address %s which is already allocated to resource %s.",
				cfgID, want, user)
		}

		return a.String(), nil
	}

	return "", netErr(http.StatusBadRequest, "PrivateIPAddressNotInSubnet",
		"IP configuration %s is using the private IP address %s which is not in the subnet's address range.",
		cfgID, want)
}

func (p *NetworkProvider) putNIC(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)

	rawCfgs := anyList(props[netIPConfigs])
	if len(rawCfgs) == 0 {
		return nil, false, netBad("Network interface %s requires at least one IP configuration.", id.Names[0])
	}

	nicID := ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: id.Namespace,
		Types: []string{netNICs}, Names: []string{id.Names[0]},
	}.ARMID()
	old, existed := p.top(netNICs, id.ResourceGroup, id.Names[0])
	if existed && old.Obj.Aux["pe"] != "" {
		return nil, false, nicInUse(nicID, old.Obj.Aux["pe"])
	}

	used := p.usedAddrs(nicID)

	cfgs, err := p.normalizeIPConfigs(nicID, old, rawCfgs, used)
	if err != nil {
		return nil, false, err
	}

	if nsg := subID(props, propSubnetNSG); nsg != "" {
		if _, ok := p.netRef(nsg, netNSGs); !ok {
			return nil, false, refMissing(nsg)
		}
	}

	loc := normLocation(strOf(body, fieldLocation))
	if loc == "" && existed {
		loc = old.Obj.Location
	}

	if err = p.checkNICRefs(nicID, loc, cfgs); err != nil {
		return nil, false, err
	}

	it, obj, created := p.upsertTop(netNICs, id, body)
	obj.Props = takeProps(body, "macAddress", fieldProvisioningState, netFieldResourceGUID, "virtualMachine",
		"privateEndpoint", "primary", "tapConfigurations", "hostedWorkloads", "vnetEncryptionSupported")
	obj.Props[netIPConfigs] = cfgs

	if nsg := subID(props, propSubnetNSG); nsg != "" {
		obj.Props[propSubnetNSG] = map[string]any{netFieldID: nsg}
	}

	if !existed {
		obj.Aux["mac"] = macFor(obj.GUID)
		obj.Aux["suffix"] = strings.ReplaceAll(obj.GUID, "-", "")[:8] + ".bx.internal.cloudapp.net"
	}

	return p.bodyNIC(id, it), created, nil
}

func macFor(guid string) string {
	h := strings.ToUpper(strings.ReplaceAll(guid, "-", ""))

	return nicMACPrefix + h[0:2] + "-" + h[2:4] + "-" + h[4:6]
}

func (p *NetworkProvider) normalizeIPConfigs(
	nicID string, old *netParent, raw []any, used map[string]map[string]string,
) ([]any, error) {
	prevCfg := map[string]map[string]any{}

	if old != nil {
		for _, c := range anyList(old.Obj.Props[netIPConfigs]) {
			m, _ := c.(map[string]any)
			prevCfg[strings.ToLower(strOf(m, fieldName))] = propsOf(m)
		}
	}

	out := make([]any, 0, len(raw))
	primaries := 0

	for _, e := range raw {
		m, _ := e.(map[string]any)
		name := strOf(m, fieldName)

		if err := validNetName(name, netMaxName); err != nil {
			return nil, err
		}

		cp, err := p.normalizeIPConfig(
			nicID+"/"+netIPConfigs+"/"+name,
			propsOf(m),
			prevCfg[strings.ToLower(name)],
			used,
		)
		if err != nil {
			return nil, err
		}

		if b, _ := cp["primary"].(bool); b {
			primaries++
		}

		out = append(out, map[string]any{fieldName: name, fieldProperties: cp})
	}

	switch {
	case primaries > 1:
		return nil, netBad("A network interface can have only one primary IP configuration.")
	case primaries == 0:
		first, _ := out[0].(map[string]any)
		propsOf(first)["primary"] = true
	}

	return out, nil
}

func parseIPConfigMode(cfgID string, cp map[string]any) (string, string, error) {
	version, ok := canonEnum(firstNonEmpty(strOf(cp, "privateIPAddressVersion"), versionV4),
		[]string{versionV4, versionV6})
	if !ok || version == versionV6 {
		return "", "", netBad("IP configuration %s: only IPv4 private addresses are supported.", cfgID)
	}

	method, ok := canonEnum(firstNonEmpty(strOf(cp, "privateIPAllocationMethod"), methodDynamic),
		[]string{methodStatic, methodDynamic})
	if !ok {
		return "", "", netBad("privateIPAllocationMethod must be Static or Dynamic.")
	}

	return method, version, nil
}

func (p *NetworkProvider) normalizeIPConfig(
	cfgID string, cp, prev map[string]any, used map[string]map[string]string,
) (map[string]any, error) {
	method, version, err := parseIPConfigMode(cfgID, cp)
	if err != nil {
		return nil, err
	}

	sid := subID(cp, netFieldSubnet)
	if sid == "" {
		return nil, netBad("IP configuration %s requires subnet.id.", cfgID)
	}

	_, sn, found := p.subnetByID(sid)
	if !found {
		return nil, refMissing(sid)
	}

	key := strings.ToLower(sid)
	if used[key] == nil {
		used[key] = map[string]string{}
	}

	prevAddr := ""
	if prev != nil && strings.EqualFold(subID(prev, netFieldSubnet), sid) {
		prevAddr = strOf(prev, "privateIPAddress")
	}

	want := strOf(cp, "privateIPAddress")
	if method == methodDynamic && want != "" && prevAddr == "" {
		prevAddr = want
	}

	addr, err := assignIP(cfgID, method, want, subnetPrefixes(sn), used[key], prevAddr)
	if err != nil {
		return nil, err
	}

	used[key][addr] = cfgID

	out := maps.Clone(cp)
	delete(out, fieldProvisioningState)

	out[netFieldSubnet] = map[string]any{netFieldID: sid}
	out["privateIPAddress"], out["privateIPAllocationMethod"], out["privateIPAddressVersion"] = addr, method, version

	if err = p.bindPublicIP(cfgID, cp, out); err != nil {
		return nil, err
	}

	for _, key := range []string{propNICPools, propNICNATs, propNICASGs} {
		if list := anyList(cp[key]); len(list) > 0 {
			ids := make([]string, 0, len(list))
			for _, e := range list {
				ids = append(ids, refIDOf(e))
			}

			out[key] = idRefs(ids)
		} else {
			delete(out, key)
		}
	}

	delete(out, "privateLinkConnectionProperties")

	return out, nil
}

// bindPublicIP validates the public IP reference of an IP configuration and records it in out.
func (p *NetworkProvider) bindPublicIP(cfgID string, cp, out map[string]any) error {
	pid := subID(cp, "publicIPAddress")
	if pid == "" {
		return nil
	}

	pip, exists := p.netRef(pid, netPIPs)
	if !exists {
		return refMissing(pid)
	}

	if user := p.pipHolder(pip.armID()); user != "" && !sameNIC(user, cfgID) {
		return netErr(http.StatusBadRequest, "PublicIPAddressInUse",
			"Public IP address %s is already allocated to resource %s.", pid, user)
	}

	out["publicIPAddress"] = map[string]any{netFieldID: pid}

	return nil
}

func (p *NetworkProvider) bodyNIC(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	cfgs := make([]any, 0, len(anyList(it.Obj.Props[netIPConfigs])))
	for _, c := range anyList(it.Obj.Props[netIPConfigs]) {
		m, _ := c.(map[string]any)
		cfgs = append(cfgs, p.ipConfigBody(it, strOf(m, fieldName), propsOf(m)))
	}

	props[netIPConfigs] = cfgs
	props["macAddress"] = it.Obj.Aux["mac"]
	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID
	props["tapConfigurations"] = []any{}
	props["hostedWorkloads"] = []any{}
	props["nicType"] = skuTierStandard
	props["vnetEncryptionSupported"] = false

	for _, k := range []string{"enableAcceleratedNetworking", "enableIPForwarding", "disableTcpStateTracking"} {
		if _, ok := props[k].(bool); !ok {
			props[k] = false
		}
	}

	dns := map[string]any{}

	if d, ok := props["dnsSettings"].(map[string]any); ok {
		maps.Copy(dns, d)
	}

	for _, k := range []string{"dnsServers", "appliedDnsServers"} {
		if _, ok := dns[k]; !ok {
			dns[k] = []any{}
		}
	}

	dns["internalDomainNameSuffix"] = it.Obj.Aux["suffix"]
	props["dnsSettings"] = dns

	nicVMBackRef(props, it.Obj.Aux)

	return p.envelope(id, netNICs, &it.Obj, props)
}

func (p *NetworkProvider) ipConfigBody(nic *netParent, name string, cp map[string]any) map[string]any {
	props := map[string]any{}

	maps.Copy(props, cp)

	props[fieldProvisioningState] = netStatusSucceeded

	if pid := subID(cp, "publicIPAddress"); pid != "" {
		props["publicIPAddress"] = map[string]any{netFieldID: pid}
	}

	return map[string]any{
		netFieldID: nic.armID() + "/" + netIPConfigs + "/" + name, fieldName: name, fieldType: ipConfigTypePath,
		netFieldEtag: nic.Obj.Etag, fieldProperties: props,
	}
}

func (p *NetworkProvider) getIPConfig(nic *netParent, id ResourceID) (map[string]any, error) {
	for _, c := range anyList(nic.Obj.Props[netIPConfigs]) {
		m, _ := c.(map[string]any)
		if strings.EqualFold(strOf(m, fieldName), id.Names[1]) {
			return p.ipConfigBody(nic, strOf(m, fieldName), propsOf(m)), nil
		}
	}

	return nil, ErrResourceNotFound
}

func (p *NetworkProvider) listIPConfigs(nic *netParent) []map[string]any {
	out := make([]map[string]any, 0, len(anyList(nic.Obj.Props[netIPConfigs])))

	for _, c := range anyList(nic.Obj.Props[netIPConfigs]) {
		m, _ := c.(map[string]any)
		out = append(out, p.ipConfigBody(nic, strOf(m, fieldName), propsOf(m)))
	}

	return out
}

// checkIPAvailability implements the vnet CheckIPAddressAvailability action.
func (p *NetworkProvider) checkIPAvailability(vnet *netParent, ip string) (map[string]any, error) {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return nil, netBad("%s is not a valid IP address.", ip)
	}

	var home *netObj

	for _, s := range vnet.Kids[netSubnets] {
		for _, pf := range subnetPrefixes(s) {
			if pf.Contains(addr) {
				home = s
			}
		}
	}

	if home == nil {
		return nil, netErr(http.StatusBadRequest, "PrivateIPAddressNotInAnySubnet",
			"Private IP address %s is not in any subnet of virtual network %s.", ip, vnet.Obj.Name)
	}

	taken := p.usedAddrs("")[strings.ToLower(subnetIDOf(vnet, home.Name))]

	for _, pf := range subnetPrefixes(home) {
		if !pf.Contains(addr) {
			continue
		}

		if !subnetReserved(pf, addr) && taken[addr.String()] == "" {
			return map[string]any{netPropAvailable: true}, nil
		}

		suggest := []string{}
		lo, hi, _ := subnetRange(pf)

		for v := lo; v <= hi && len(suggest) < maxSuggestions; v++ {
			if a := u4(v).String(); taken[a] == "" {
				suggest = append(suggest, a)
			}
		}

		return map[string]any{netPropAvailable: false, "availableIPAddresses": suggest}, nil
	}

	return map[string]any{netPropAvailable: false, "availableIPAddresses": []string{}}, nil
}

// --- deletion rules, reconciliation ---

func (p *NetworkProvider) checkTopDelete(it *netParent, _ ResourceID) error {
	switch it.Type {
	case netVNets:
		for _, s := range it.Kids[netSubnets] {
			if users := p.subnetUsers(subnetIDOf(it, s.Name)); len(users) > 0 {
				return inUseSubnet(s.Name, users[0])
			}
		}
	case netNSGs:
		users := append(p.nsgNICs(it.armID()), p.nsgSubnets(it.armID())...)
		if len(users) > 0 {
			return netErr(http.StatusBadRequest, "InUseNetworkSecurityGroupCannotBeDeleted",
				"Network security group %s cannot be deleted because it is in use by the following resources: %s. "+
					"In order to delete the Network security group, remove the association with the resource(s). "+
					"To learn how to do this, see aka.ms/deletensg.", it.Obj.Name, strings.Join(users, ", "))
		}
	case netNICs:
		if vm := it.Obj.Aux[auxVM]; vm != "" {
			return nicInUse(it.armID(), vm)
		}

		if pe := it.Obj.Aux["pe"]; pe != "" {
			return nicInUse(it.armID(), pe)
		}
	case netRTs:
		if users := p.rtSubnets(it.armID()); len(users) > 0 {
			return netErr(http.StatusBadRequest, "InUseRouteTableCannotBeDeleted",
				"Route table %s is in use by the following subnets: %s and cannot be deleted. Remove the "+
					"association first.", it.Obj.Name, strings.Join(users, ", "))
		}
	case netPIPs:
		if user := p.pipHolder(it.armID()); user != "" {
			return netErr(http.StatusBadRequest, "PublicIPAddressCannotBeDeleted",
				"Public IP address %s can not be deleted since it is still allocated to resource %s.",
				it.armID(), user)
		}
	default:
		return p.checkNewTopDelete(it)
	}

	return nil
}

func (p *NetworkProvider) checkKidDelete(parent *netParent, kt string, id ResourceID) error {
	switch kt {
	case netSubnets:
		if users := p.subnetUsers(subnetIDOf(parent, id.Names[1])); len(users) > 0 {
			return inUseSubnet(id.Names[1], users[0])
		}
	case netDefRules, netIPConfigs, lbNICs, nicLBs:
		return ErrResourceNotFound
	default:
		if parent.Type == netLBs {
			return p.checkLBKidDelete(parent, kt, id)
		}

		if parent.Type == netZones && kt == "SOA" {
			return netErr(http.StatusBadRequest, "BadRequest", "The SOA record set cannot be deleted.")
		}
	}

	return nil
}

func (p *NetworkProvider) afterKidRemoved(parent *netParent, kt string, removed *netObj) {
	if parent.Type == netPEs && kt == peZoneGroups {
		p.removeZoneRecords(parent, removed)
	}
}

func (p *NetworkProvider) afterTopRemoved(it *netParent, _ ResourceID) {
	if it.Type == netPEs {
		p.afterPERemoved(it)
	}
}

// reconcileLocked recomputes derived state after any mutation: peering states and dynamic public IP assignment.
func (p *NetworkProvider) reconcileLocked() {
	p.pruneDanglingLocked()
	p.reconcilePeeringsLocked()

	for _, ip := range p.ofType(netPIPs) {
		assoc := p.pipUser(ip.armID()) != ""
		method := strOf(ip.Obj.Props, "publicIPAllocationMethod")

		switch {
		case method == methodDynamic && !assoc:
			ip.Obj.Aux[auxIP] = ""
		case method == methodDynamic && assoc && ip.Obj.Aux[auxIP] == "":
			if a, err := p.allocPIP(strOf(ip.Obj.Props, "publicIPAddressVersion")); err == nil {
				ip.Obj.Aux[auxIP] = a
			}
		}
	}
}

func (p *NetworkProvider) pruneDanglingLocked() {
	for _, v := range p.ofType(netVNets) {
		for _, s := range v.Kids[netSubnets] {
			p.dropMissingRef(s.Props, propSubnetNSG, netNSGs)
			p.dropMissingRef(s.Props, propSubnetRT, netRTs)
			p.dropMissingRef(s.Props, propNATGateway, netNATs)
		}
	}

	for _, n := range p.ofType(netNICs) {
		p.dropMissingRef(n.Obj.Props, propSubnetNSG, netNSGs)

		for _, c := range anyList(n.Obj.Props[netIPConfigs]) {
			m, _ := c.(map[string]any)
			p.dropMissingRef(propsOf(m), "publicIPAddress", netPIPs)
			p.dropMissingListRefs(propsOf(m))
		}
	}

	p.pruneNewTypesLocked()

	for _, z := range p.ofType(netZones) {
		z.Kids[netLinks] = slices.DeleteFunc(z.Kids[netLinks], func(l *netObj) bool {
			_, ok := p.netRef(subID(l.Props, "virtualNetwork"), netVNets)

			return !ok
		})
	}
}

func (p *NetworkProvider) dropMissingRef(props map[string]any, key, typ string) {
	if id := subID(props, key); id != "" {
		if _, ok := p.netRef(id, typ); !ok {
			delete(props, key)
		}
	}
}

func sameNIC(a, b string) bool {
	cut := func(s string) string {
		if i := strings.LastIndex(strings.ToLower(s), "/"+strings.ToLower(netIPConfigs)+"/"); i >= 0 {
			return strings.ToLower(s[:i])
		}

		return strings.ToLower(s)
	}

	return cut(a) == cut(b)
}

package azurearm

import (
	"maps"
	"net/http"
	"net/netip"
	"slices"
	"strings"
)

const (
	natMinIdle      = 4
	natMaxIdle      = 120
	natMaxIPs       = 16
	natMinPrefixLen = 28
	propNATGateway  = "natGateway"
	propPrefixRef   = "publicIPPrefix"
	auxPrefixRange  = "ipPrefix"
	prefixV4Min     = 28
	prefixV4Max     = 31
	prefixV6Min     = 124
	prefixV6Max     = 127
	prefixV6Base    = "2001:db8:100::"
	ipv6Bits        = 128
)

// --- holders ---

func (p *NetworkProvider) lbFrontendUsers(key, target string) []string {
	var out []string

	for _, lb := range p.ofType(netLBs) {
		for _, f := range lb.Kids[lbFrontends] {
			if strings.EqualFold(subID(f.Props, key), target) {
				out = append(out, lb.armID()+"/"+lbFrontends+"/"+f.Name)
			}
		}
	}

	return out
}

func (p *NetworkProvider) natOfPIP(pipID string) string {
	for _, n := range p.ofType(netNATs) {
		for _, key := range []string{natPropPIPs, natPropPIPsV6} {
			if hasRef(n.Obj.Props, key, pipID) {
				return n.armID()
			}
		}
	}

	return ""
}

// pipHolder names whatever holds the public IP: a NIC IP configuration, a load balancer frontend or a NAT gateway.
func (p *NetworkProvider) pipHolder(pipID string) string {
	if u := p.pipUser(pipID); u != "" {
		return u
	}

	return p.natOfPIP(pipID)
}

func (p *NetworkProvider) prefixHolder(prefID string) string {
	if u := p.lbFrontendUsers(propPrefixRef, prefID); len(u) > 0 {
		return u[0]
	}

	for _, n := range p.ofType(netNATs) {
		for _, key := range []string{natPropPrefixes, natPropPrefixesV6} {
			if hasRef(n.Obj.Props, key, prefID) {
				return n.armID()
			}
		}
	}

	return ""
}

// --- NAT gateways ---

func (p *NetworkProvider) putNAT(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)
	in, _ := body[netFieldSKU].(map[string]any)

	if name := strOf(in, fieldName); name != "" && !strings.EqualFold(name, skuStandard) {
		return nil, false, netBad("NAT gateway SKU %s is not supported; only Standard is.", name)
	}

	idle, err := intIn(props, "idleTimeoutInMinutes", natMinIdle, natMaxIdle, natMinIdle)
	if err != nil {
		return nil, false, err
	}

	old, existed := p.top(netNATs, id.ResourceGroup, id.Names[0])
	loc := normLocation(strOf(body, fieldLocation))

	if loc == "" && existed {
		loc = old.Obj.Location
	}

	self := id.ARMID()
	refs := map[string][]any{}
	total := 0

	for _, key := range []string{natPropPIPs, natPropPIPsV6} {
		list, n, perr := p.natPIPs(self, loc, key, anyList(props[key]))
		if perr != nil {
			return nil, false, perr
		}

		refs[key], total = list, total+n
	}

	for _, key := range []string{natPropPrefixes, natPropPrefixesV6} {
		list, n, perr := p.natPrefixes(self, loc, anyList(props[key]))
		if perr != nil {
			return nil, false, perr
		}

		refs[key], total = list, total+n
	}

	if total > natMaxIPs {
		return nil, false, netErr(
			http.StatusBadRequest,
			"NatGatewayHasTooManyPublicIps",
			"NAT gateway %s would use %d public IP addresses; a NAT gateway supports at most %d.",
			self,
			total,
			natMaxIPs,
		)
	}

	it, obj, created := p.upsertTop(netNATs, id, body)
	obj.Props = takeProps(body, fieldProvisioningState, netFieldResourceGUID, "subnets", "sourceVirtualNetwork")
	obj.Props["idleTimeoutInMinutes"] = idle

	for k, v := range refs {
		if len(v) > 0 {
			obj.Props[k] = v
		} else {
			delete(obj.Props, k)
		}
	}

	obj.Extra = map[string]any{netFieldSKU: map[string]any{fieldName: skuStandard}}

	if z, has := body[netFieldZones]; has {
		obj.Extra[netFieldZones] = z
	}

	return p.bodyNAT(id, it), created, nil
}

func (p *NetworkProvider) natPIPs(self, loc, key string, raw []any) ([]any, int, error) {
	out := make([]any, 0, len(raw))

	for _, e := range raw {
		pid := refIDOf(e)

		pip, ok := p.netRef(pid, netPIPs)
		if !ok || (loc != "" && pip.Obj.Location != loc) {
			return nil, 0, refMissing(pid)
		}

		sk, _ := pip.Obj.Extra[netFieldSKU].(map[string]any)
		version := strOf(pip.Obj.Props, "publicIPAddressVersion")

		switch {
		case strOf(sk, fieldName) != skuStandard:
			return nil, 0, netErr(http.StatusBadRequest, "NatGatewayPublicIpSkuMismatch",
				"Public IP %s must use the Standard SKU to be used by a NAT gateway.", pid)
		case (key == natPropPIPsV6) != (version == versionV6):
			return nil, 0, netBad("Public IP %s has IP version %s, which does not match %s.", pid, version, key)
		}

		if user := p.pipHolder(pip.armID()); user != "" && !strings.EqualFold(user, self) {
			return nil, 0, netErr(http.StatusBadRequest, "PublicIPAddressInUse",
				"Public IP address %s is already allocated to resource %s.", pid, user)
		}

		out = append(out, idRef(pid))
	}

	return out, len(out), nil
}

func (p *NetworkProvider) natPrefixes(self, loc string, raw []any) ([]any, int, error) {
	out := make([]any, 0, len(raw))
	count := 0

	for _, e := range raw {
		pid := refIDOf(e)

		pf, ok := p.netRef(pid, netPrefixes)
		if !ok || (loc != "" && pf.Obj.Location != loc) {
			return nil, 0, refMissing(pid)
		}

		length := intOf(pf.Obj.Props["prefixLength"])
		if length < natMinPrefixLen && length < prefixV6Min {
			return nil, 0, netBad("Public IP prefix %s is too large for a NAT gateway; /%d is the largest allowed.",
				pid, natMinPrefixLen)
		}

		if user := p.prefixHolder(pf.armID()); user != "" && !strings.EqualFold(user, self) {
			return nil, 0, netErr(http.StatusBadRequest, "PublicIPPrefixInUse",
				"Public IP prefix %s is already allocated to resource %s.", pid, user)
		}

		bits := ipv6Bits - length
		if pf.Obj.Props["publicIPAddressVersion"] != versionV6 {
			bits = ipv4Bits - length
		}

		count += 1 << bits
		out = append(out, idRef(pid))
	}

	return out, count, nil
}

func (p *NetworkProvider) natSubnets(natID string) []string {
	var out []string

	for _, v := range p.ofType(netVNets) {
		for _, s := range v.Kids[netSubnets] {
			if strings.EqualFold(subID(s.Props, propNATGateway), natID) {
				out = append(out, subnetIDOf(v, s.Name))
			}
		}
	}

	return out
}

func (p *NetworkProvider) bodyNAT(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	if subs := p.natSubnets(it.armID()); len(subs) > 0 {
		props["subnets"] = idRefs(subs)
	}

	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID

	return p.envelope(id, netNATs, &it.Obj, props)
}

// --- public IP prefixes ---

func (p *NetworkProvider) prefixRange(pf *netParent) (netip.Prefix, bool) {
	r, err := netip.ParsePrefix(pf.Obj.Aux[auxPrefixRange])

	return r, err == nil
}

// prefixAddrsInUse marks every address held by a public IP not created from a prefix, plus every other prefix range.
func (p *NetworkProvider) rangeFree(r netip.Prefix, skip string) bool {
	for _, o := range p.ofType(netPrefixes) {
		if or, ok := p.prefixRange(o); ok && o.armID() != skip && or.Overlaps(r) {
			return false
		}
	}

	for _, ip := range p.ofType(netPIPs) {
		a, err := netip.ParseAddr(ip.Obj.Aux[auxIP])
		if err == nil && r.Contains(a) && subID(ip.Obj.Props, propPrefixRef) == "" {
			return false
		}
	}

	return true
}

func (p *NetworkProvider) allocPrefixRange(length int, v6 bool) (netip.Prefix, error) {
	if v6 {
		base := netip.MustParseAddr(prefixV6Base)
		size := 1 << (ipv6Bits - length)

		for i := range 1 << 8 {
			a := base
			for range i * size {
				a = a.Next()
			}

			if r := netip.PrefixFrom(a, length); p.rangeFree(r, "") {
				return r, nil
			}
		}
	} else {
		size := uint32(1) << uint(ipv4Bits-length)

		for _, blk := range pipRanges {
			base := v4u(blk.Addr())

			for off := uint32(0); off+size <= 1<<(ipv4Bits-blk.Bits()); off += size {
				if r := netip.PrefixFrom(u4(base+off), length); p.rangeFree(r, "") {
					return r, nil
				}
			}
		}
	}

	return netip.Prefix{}, netErr(http.StatusBadRequest, "PublicIPPrefixAddressSpaceExhausted",
		"No public IP address range of size /%d is available in the emulator's documentation ranges.", length)
}

func (p *NetworkProvider) putPrefix(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)
	in, _ := body[netFieldSKU].(map[string]any)

	name, ok := canonEnum(firstNonEmpty(strOf(in, fieldName), skuStandard), []string{skuStandard})
	if !ok {
		return nil, false, netBad("Public IP prefix SKU name must be Standard.")
	}

	tier, ok := canonEnum(firstNonEmpty(strOf(in, netFieldTier), tierRegional), []string{tierRegional, tierGlobal})
	if !ok {
		return nil, false, netBad("Public IP prefix SKU tier must be Regional or Global.")
	}

	version, ok := canonEnum(firstNonEmpty(strOf(props, "publicIPAddressVersion"), versionV4),
		[]string{versionV4, versionV6})
	if !ok {
		return nil, false, netBad("publicIPAddressVersion must be IPv4 or IPv6.")
	}

	lo, hi := prefixV4Min, prefixV4Max
	if version == versionV6 {
		lo, hi = prefixV6Min, prefixV6Max
	}

	length, err := intIn(props, "prefixLength", lo, hi, -1)
	if err != nil {
		return nil, false, netErr(http.StatusBadRequest, "InvalidPublicIpPrefixLength",
			"Public IP prefix length for %s must be between /%d and /%d.", version, lo, hi)
	}

	old, existed := p.top(netPrefixes, id.ResourceGroup, id.Names[0])
	if existed &&
		(intOf(old.Obj.Props["prefixLength"]) != length || old.Obj.Props["publicIPAddressVersion"] != version) {
		return nil, false, netErr(http.StatusBadRequest, "PublicIPPrefixSizeCannotBeChanged",
			"The size and version of public IP prefix %s cannot be modified after creation.", id.Names[0])
	}

	var rng string

	if existed {
		rng = old.Obj.Aux[auxPrefixRange]
	} else {
		r, aerr := p.allocPrefixRange(length, version == versionV6)
		if aerr != nil {
			return nil, false, aerr
		}

		rng = r.String()
	}

	it, obj, created := p.upsertTop(netPrefixes, id, body)
	obj.Props = takeProps(body, "ipPrefix", fieldProvisioningState, netFieldResourceGUID, "publicIPAddresses",
		propNATGateway, "loadBalancerFrontendIPConfiguration")
	obj.Props["prefixLength"], obj.Props["publicIPAddressVersion"] = length, version
	obj.Aux[auxPrefixRange] = rng
	obj.Extra = map[string]any{netFieldSKU: map[string]any{fieldName: name, netFieldTier: tier}}

	if z, has := body[netFieldZones]; has {
		obj.Extra[netFieldZones] = z
	}

	return p.bodyPrefix(id, it), created, nil
}

func (p *NetworkProvider) prefixPIPs(prefID string) []*netParent {
	var out []*netParent

	for _, ip := range p.ofType(netPIPs) {
		if strings.EqualFold(subID(ip.Obj.Props, propPrefixRef), prefID) {
			out = append(out, ip)
		}
	}

	return out
}

func (p *NetworkProvider) bodyPrefix(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	props["ipPrefix"] = it.Obj.Aux[auxPrefixRange]

	if pips := p.prefixPIPs(it.armID()); len(pips) > 0 {
		refs := make([]any, 0, len(pips))
		for _, ip := range pips {
			refs = append(refs, idRef(ip.armID()))
		}

		props["publicIPAddresses"] = refs
	}

	for _, n := range p.ofType(netNATs) {
		for _, key := range []string{natPropPrefixes, natPropPrefixesV6} {
			if hasRef(n.Obj.Props, key, it.armID()) {
				props[propNATGateway] = idRef(n.armID())
			}
		}
	}

	if f := p.lbFrontendUsers(propPrefixRef, it.armID()); len(f) > 0 {
		props["loadBalancerFrontendIPConfiguration"] = idRef(f[0])
	}

	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID

	return p.envelope(id, netPrefixes, &it.Obj, props)
}

// allocFromPrefix picks the address for a public IP created from a prefix: the wanted one, or the lowest free.
func (p *NetworkProvider) allocFromPrefix(pf *netParent, pipID, want string) (string, error) {
	rng, ok := p.prefixRange(pf)
	if !ok {
		return "", netBad("Public IP prefix %s has no address range.", pf.armID())
	}

	taken := map[string]bool{}

	for _, ip := range p.prefixPIPs(pf.armID()) {
		if ip.armID() != pipID {
			taken[ip.Obj.Aux[auxIP]] = true
		}
	}

	if want != "" {
		a, err := netip.ParseAddr(want)
		if err != nil || !rng.Contains(a) {
			return "", netBad("ipAddress %s is not inside public IP prefix %s (%s).", want, pf.armID(), rng)
		}

		if taken[a.String()] {
			return "", netErr(http.StatusBadRequest, "PublicIPAddressInUse",
				"Public IP address %s is already allocated from prefix %s.", want, pf.armID())
		}

		return a.String(), nil
	}

	for a := rng.Addr(); rng.Contains(a); a = a.Next() {
		if !taken[a.String()] {
			return a.String(), nil
		}
	}

	return "", netErr(http.StatusBadRequest, "PublicIPPrefixExhausted",
		"All public IP addresses of prefix %s (%s) are already allocated.", pf.armID(), rng)
}

// bindPIPPrefix validates the publicIPPrefix reference of a public IP and returns the address it gets.
func (p *NetworkProvider) bindPIPPrefix(
	id ResourceID,
	st pipSettings,
	loc, want string,
	props map[string]any,
) (string, error) {
	prefID := subID(props, propPrefixRef)

	pf, ok := p.netRef(prefID, netPrefixes)
	if !ok || (loc != "" && pf.Obj.Location != loc) {
		return "", refMissing(prefID)
	}

	if st.skuName != skuStandard || st.method != methodStatic {
		return "", netBad("A public IP created from a prefix must be Standard SKU with Static allocation.")
	}

	if pf.Obj.Props["publicIPAddressVersion"] != st.version {
		return "", netBad("Public IP address version %s does not match the version of prefix %s.", st.version, prefID)
	}

	pipID := id.ARMID()

	if old, existed := p.top(netPIPs, id.ResourceGroup, id.Names[0]); existed && want == "" &&
		strings.EqualFold(subID(old.Obj.Props, propPrefixRef), prefID) && old.Obj.Aux[auxIP] != "" {
		want = old.Obj.Aux[auxIP]
	}

	return p.allocFromPrefix(pf, pipID, want)
}

// --- application security groups ---

func (p *NetworkProvider) putASG(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	it, obj, created := p.upsertTop(netASGs, id, body)
	obj.Props = map[string]any{}

	return p.bodyASG(id, it), created, nil
}

func (p *NetworkProvider) bodyASG(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{fieldProvisioningState: netStatusSucceeded, netFieldResourceGUID: it.Obj.GUID}

	return p.envelope(id, netASGs, &it.Obj, props)
}

// asgUsers lists the NIC IP configurations, security rules and private endpoints that reference the group.
func (p *NetworkProvider) asgUsers(asgID string) []string {
	var out []string

	for _, c := range p.allIPConfigs() {
		if hasRef(c.props, propNICASGs, asgID) {
			out = append(out, c.id)
		}
	}

	for _, n := range p.ofType(netNSGs) {
		for _, r := range n.Kids[netRules] {
			if hasRef(r.Props, propSrcASGs, asgID) ||
				hasRef(r.Props, propDestASGs, asgID) {
				out = append(out, n.armID()+"/"+netRules+"/"+r.Name)
			}
		}
	}

	for _, pe := range p.ofType(netPEs) {
		if hasRef(pe.Obj.Props, propNICASGs, asgID) {
			out = append(out, pe.armID())
		}
	}

	return out
}

// asgVNet returns the virtual network a group's member NICs live in, ignoring skipNIC; "" when it has no members.
func (p *NetworkProvider) asgVNet(asgID, skipNIC string) string {
	for _, c := range p.allIPConfigs() {
		if strings.EqualFold(c.nic.armID(), skipNIC) || !hasRef(c.props, propNICASGs, asgID) {
			continue
		}

		sid := subID(c.props, netFieldSubnet)
		if i := strings.Index(strings.ToLower(sid), netPrefixSubnetsPath); i >= 0 {
			return strings.ToLower(sid[:i])
		}
	}

	return ""
}

// checkNICASGs validates the application security groups on a NIC's IP configurations.
func (p *NetworkProvider) checkNICASGs(nicID, loc string, cfgs []any) error {
	for _, c := range cfgs {
		m, _ := c.(map[string]any)
		cp := propsOf(m)

		sid := subID(cp, netFieldSubnet)
		vnet := ""

		if i := strings.Index(strings.ToLower(sid), netPrefixSubnetsPath); i >= 0 {
			vnet = strings.ToLower(sid[:i])
		}

		for _, a := range anyList(cp[propNICASGs]) {
			aid := refIDOf(a)

			asg, ok := p.netRef(aid, netASGs)
			if !ok || (loc != "" && asg.Obj.Location != loc) {
				return refMissing(aid)
			}

			if other := p.asgVNet(aid, nicID); other != "" && other != vnet {
				return netErr(http.StatusBadRequest, "ApplicationSecurityGroupVirtualNetworkMismatch",
					"All network interfaces of application security group %s must be in the same virtual network; "+
						"it already has members in %s.", aid, other)
			}
		}
	}

	return nil
}

// checkRuleASGs validates the application security groups a security rule names.
func (p *NetworkProvider) checkRuleASGs(nsgLoc string, props map[string]any) error {
	vnets := map[string]string{}

	for _, key := range []string{propSrcASGs, propDestASGs} {
		for _, a := range anyList(props[key]) {
			aid := refIDOf(a)

			asg, ok := p.netRef(aid, netASGs)
			if !ok || (nsgLoc != "" && asg.Obj.Location != nsgLoc) {
				return refMissing(aid)
			}

			vnets[key] = p.asgVNet(asg.armID(), "")
		}
	}

	s, d := vnets[propSrcASGs], vnets[propDestASGs]
	if s != "" && d != "" && s != d {
		return netErr(http.StatusBadRequest, "ApplicationSecurityGroupVirtualNetworkMismatch",
			"The source and destination application security groups of a rule must have members in the same "+
				"virtual network (%s, %s).", s, d)
	}

	return nil
}

// normRuleASGs rewrites the ASG references of a security rule to bare id objects.
func normRuleASGs(props map[string]any) {
	for _, key := range []string{propSrcASGs, propDestASGs} {
		list := anyList(props[key])
		if len(list) == 0 {
			delete(props, key)

			continue
		}

		ids := make([]string, 0, len(list))
		for _, a := range list {
			ids = append(ids, refIDOf(a))
		}

		props[key] = idRefs(slices.Compact(ids))
	}
}

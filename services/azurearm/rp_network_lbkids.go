package azurearm

import (
	"net/http"
	"net/netip"
	"strings"
)

const (
	probeHTTP        = "Http"
	probeHTTPS       = "Https"
	defaultProbeSecs = 15
	minProbeSecs     = 5
	defaultProbes    = 2
	lbMaxIdle        = 30
	lbMinIdle        = 4
	maxCount         = 1 << 30
	outMaxIdle       = 120
)

// intIn reads an optional integer property within [lo, hi]; def < 0 makes it required.
func intIn(m map[string]any, key string, lo, hi, def int) (int, error) {
	v, set := m[key]
	if !set || v == nil {
		if def < 0 {
			return 0, netBad("%s is required.", key)
		}

		return def, nil
	}

	f, isNum := v.(float64)
	n := intOf(v)

	if i, isInt := v.(int); isInt {
		f, isNum = float64(i), true
	}

	if !isNum || f != float64(n) || n < lo || n > hi {
		return 0, netBad("%s must be an integer between %d and %d.", key, lo, hi)
	}

	return n, nil
}

func boolOr(m map[string]any, key string, def bool) bool {
	if b, ok := m[key].(bool); ok {
		return b
	}

	return def
}

func enumProp(m map[string]any, key string, allowed []string, def string) (string, error) {
	v := strOf(m, key)
	if v == "" {
		if def == "" {
			return "", netBad("%s is required.", key)
		}

		return def, nil
	}

	canon, ok := canonEnum(v, allowed)
	if !ok {
		return "", netBad("%s must be one of %s.", key, strings.Join(allowed, ", "))
	}

	return canon, nil
}

// --- frontends ---

func (p *NetworkProvider) normFrontend(c *lbCtx, name string, m map[string]any, prev *netObj) (*netObj, error) {
	props := propsOf(m)
	pipID, prefID, sid := subID(props, "publicIPAddress"), subID(props, "publicIPPrefix"), subID(props, netFieldSubnet)
	self := c.kidID(lbFrontends, name)

	set := 0

	for _, s := range []string{pipID, prefID, sid} {
		if s != "" {
			set++
		}
	}

	if set != 1 {
		return nil, netBad("Frontend IP configuration %s must reference exactly one of publicIPAddress, "+
			"publicIPPrefix or subnet.", name)
	}

	out := map[string]any{}

	if _, has := m[netFieldZones]; has && c.sku == skuBasic {
		return nil, netBad("Frontend IP configuration %s: availability zones are not supported by the Basic SKU.", name)
	}

	var err error

	switch {
	case pipID != "":
		err = p.bindLBPublicIP(c, self, pipID, out)
	case prefID != "":
		err = p.bindLBPrefix(c, self, prefID, out)
	default:
		err = p.bindLBSubnet(c, self, props, prev, out)
	}

	if err != nil {
		return nil, err
	}

	if gw := subID(props, "gatewayLoadBalancer"); gw != "" {
		out["gatewayLoadBalancer"] = idRef(gw)
	}

	o := lbObj(name, out)
	if z, has := m[netFieldZones]; has {
		o.Extra = map[string]any{netFieldZones: z}
	}

	return o, nil
}

func (p *NetworkProvider) bindLBPublicIP(c *lbCtx, self, pipID string, out map[string]any) error {
	if c.sku == skuGateway {
		return netBad("A Gateway SKU load balancer frontend must use a subnet, not a public IP address.")
	}

	pip, ok := p.netRef(pipID, netPIPs)
	if !ok || (c.loc != "" && pip.Obj.Location != c.loc) {
		return refMissing(pipID)
	}

	want := skuBasic
	if c.sku != skuBasic {
		want = skuStandard
	}

	if sk, _ := pip.Obj.Extra[netFieldSKU].(map[string]any); strOf(sk, fieldName) != want {
		return netErr(
			http.StatusBadRequest,
			"PublicIPAndLBSkuDoNotMatch",
			"Resource %s has sku %s and %s has sku %s; they must be the same.",
			pipID,
			strOf(sk, fieldName),
			c.id,
			c.sku,
		)
	}

	if user := p.pipHolder(pip.armID()); user != "" && !strings.EqualFold(user, self) {
		return netErr(http.StatusBadRequest, "PublicIPAddressInUse",
			"Public IP address %s is already allocated to resource %s.", pipID, user)
	}

	out["publicIPAddress"] = idRef(pipID)

	return nil
}

func (p *NetworkProvider) bindLBPrefix(c *lbCtx, self, prefID string, out map[string]any) error {
	if c.sku != skuStandard {
		return netBad("Public IP prefixes can only be used by Standard SKU load balancers.")
	}

	pf, ok := p.netRef(prefID, netPrefixes)
	if !ok || (c.loc != "" && pf.Obj.Location != c.loc) {
		return refMissing(prefID)
	}

	if user := p.prefixHolder(pf.armID()); user != "" && !strings.EqualFold(user, self) {
		return netErr(http.StatusBadRequest, "PublicIPPrefixInUse",
			"Public IP prefix %s is already allocated to resource %s.", prefID, user)
	}

	out["publicIPPrefix"] = idRef(prefID)

	return nil
}

func (p *NetworkProvider) bindLBSubnet(
	c *lbCtx, self string, props map[string]any, prev *netObj, out map[string]any,
) error {
	sid := subID(props, netFieldSubnet)

	_, sn, found := p.subnetByID(sid)
	if !found {
		return refMissing(sid)
	}

	if loc, ok := p.subnetLoc(sid); ok && c.loc != "" && loc != c.loc {
		return refMissing(sid)
	}

	method, version, err := parseIPConfigMode(self, props)
	if err != nil {
		return err
	}

	key := strings.ToLower(sid)
	if c.taken[key] == nil {
		c.taken[key] = map[string]string{}
	}

	prevAddr := ""
	if prev != nil && strings.EqualFold(subID(prev.Props, netFieldSubnet), sid) {
		prevAddr = strOf(prev.Props, propPrivateIP)
	}

	addr, err := assignIP(self, method, strOf(props, propPrivateIP), subnetPrefixes(sn), c.taken[key], prevAddr)
	if err != nil {
		return err
	}

	c.taken[key][addr] = self
	out[netFieldSubnet] = idRef(sid)
	out[propPrivateIP], out["privateIPAllocationMethod"], out["privateIPAddressVersion"] = addr, method, version

	return nil
}

func (p *NetworkProvider) subnetLoc(subnetID string) (string, bool) {
	v, _, ok := p.subnetByID(subnetID)
	if !ok {
		return "", false
	}

	return v.Obj.Location, true
}

// --- backend pools ---

func (p *NetworkProvider) normPool(c *lbCtx, name string, props map[string]any) (*netObj, error) {
	out := takeProps(map[string]any{fieldProperties: props}, fieldProvisioningState, "backendIPConfigurations",
		lbRules, lbNATRules, "outboundRule", lbOutbound)

	if raw, has := props["loadBalancerBackendAddresses"]; has {
		if c.sku == skuBasic {
			return nil, netBad("Backend pool %s: IP-based backend addresses require the Standard or Gateway SKU.", name)
		}

		list, err := p.poolAddresses(name, anyList(raw))
		if err != nil {
			return nil, err
		}

		out["loadBalancerBackendAddresses"] = list
	}

	if v := subID(props, "virtualNetwork"); v != "" {
		if _, ok := p.netRef(v, netVNets); !ok {
			return nil, refMissing(v)
		}
	}

	return lbObj(name, out), nil
}

func (p *NetworkProvider) poolAddresses(pool string, raw []any) ([]any, error) {
	out := make([]any, 0, len(raw))
	names, ips := map[string]bool{}, map[string]bool{}

	for _, e := range raw {
		m, _ := e.(map[string]any)
		ap := propsOf(m)
		an := strOf(m, fieldName)

		if err := validNetName(an, netMaxName); err != nil {
			return nil, err
		}

		ip, err := netip.ParseAddr(strOf(ap, propIPAddress))
		if err != nil {
			return nil, netBad("Backend address %s of pool %s requires a valid ipAddress.", an, pool)
		}

		if names[strings.ToLower(an)] || ips[ip.String()] {
			return nil, netBad("Backend pool %s lists backend address name or IP %s more than once.", pool, an)
		}

		names[strings.ToLower(an)], ips[ip.String()] = true, true

		np := map[string]any{propIPAddress: ip.String()}

		if err = p.poolAddressNet(an, ip, ap, np); err != nil {
			return nil, err
		}

		if st := strOf(ap, "adminState"); st != "" {
			canon, ok := canonEnum(st, []string{netNone, "Up", "Down"})
			if !ok {
				return nil, netBad("adminState of backend address %s must be None, Up or Down.", an)
			}

			np["adminState"] = canon
		}

		if f := subID(ap, "loadBalancerFrontendIPConfiguration"); f != "" {
			np["loadBalancerFrontendIPConfiguration"] = idRef(f)
		}

		out = append(out, map[string]any{fieldName: an, fieldProperties: np})
	}

	return out, nil
}

// poolAddressNet validates the virtual network or subnet an IP-based backend address lives in.
func (p *NetworkProvider) poolAddressNet(name string, ip netip.Addr, ap, np map[string]any) error {
	vid, sid := subID(ap, "virtualNetwork"), subID(ap, netFieldSubnet)

	switch {
	case sid != "":
		_, sn, ok := p.subnetByID(sid)
		if !ok {
			return refMissing(sid)
		}

		if !prefixInside(netip.PrefixFrom(ip, ip.BitLen()), subnetPrefixes(sn)) {
			return netBad("Backend address %s: %s is not inside subnet %s.", name, ip, sid)
		}

		np[netFieldSubnet] = idRef(sid)
	case vid != "":
		v, ok := p.netRef(vid, netVNets)
		if !ok {
			return refMissing(vid)
		}

		if !prefixInside(netip.PrefixFrom(ip, ip.BitLen()), vnetSpace(&v.Obj)) {
			return netBad("Backend address %s: %s is not inside the address space of %s.", name, ip, vid)
		}

		np["virtualNetwork"] = idRef(vid)
	default:
		return netBad("Backend address %s requires a virtualNetwork or subnet reference.", name)
	}

	return nil
}

// --- probes ---

func normProbe(name string, props map[string]any) (*netObj, error) {
	proto, err := enumProp(props, propProtocol, []string{probeHTTP, probeHTTPS, protoTCP}, "")
	if err != nil {
		return nil, err
	}

	port, err := intIn(props, "port", 1, maxPort, -1)
	if err != nil {
		return nil, err
	}

	interval, err := intIn(props, "intervalInSeconds", minProbeSecs, maxCount, defaultProbeSecs)
	if err != nil {
		return nil, err
	}

	count, err := intIn(props, "numberOfProbes", 1, maxCount, defaultProbes)
	if err != nil {
		return nil, err
	}

	threshold, err := intIn(props, "probeThreshold", 1, maxCount, 1)
	if err != nil {
		return nil, err
	}

	path := strOf(props, "requestPath")

	switch {
	case proto != protoTCP && path == "":
		return nil, netBad("Probe %s with protocol %s requires requestPath.", name, proto)
	case proto == protoTCP && path != "":
		return nil, netBad("Probe %s with protocol Tcp must not set requestPath.", name)
	}

	out := map[string]any{
		propProtocol: proto, "port": port, "intervalInSeconds": interval, "numberOfProbes": count,
		"probeThreshold": threshold,
	}

	if path != "" {
		out["requestPath"] = path
	}

	if nb := strOf(props, "noHealthyBackendsBehavior"); nb != "" {
		canon, ok := canonEnum(nb, []string{"AllProbedDown", "AllProbedUp"})
		if !ok {
			return nil, netBad("noHealthyBackendsBehavior must be AllProbedDown or AllProbedUp.")
		}

		out["noHealthyBackendsBehavior"] = canon
	}

	return lbObj(name, out), nil
}

// --- rules ---

func normLBRule(c *lbCtx, name string, props map[string]any) (*netObj, error) {
	proto, err := enumProp(props, propProtocol, []string{protoTCP, protoUDP, protoAll}, "")
	if err != nil {
		return nil, err
	}

	fp, err := intIn(props, "frontendPort", 0, maxPort-1, -1)
	if err != nil {
		return nil, err
	}

	bp, err := intIn(props, "backendPort", 0, maxPort, fp)
	if err != nil {
		return nil, err
	}

	if proto == protoAll && (fp != 0 || bp != 0) {
		return nil, netBad(
			"Load balancing rule %s with protocol All (HA ports) requires frontendPort and backendPort 0.",
			name,
		)
	}

	if proto != protoAll && fp == 0 {
		return nil, netBad("Load balancing rule %s requires a frontendPort between 1 and %d.", name, maxPort-1)
	}

	if proto == protoAll && c.sku == skuBasic {
		return nil, netBad("Load balancing rule %s: HA ports rules require the Standard or Gateway SKU.", name)
	}

	out := map[string]any{propProtocol: proto, "frontendPort": fp, "backendPort": bp}

	if err = ruleCommon(out, props, lbMinIdle); err != nil {
		return nil, err
	}

	if out["loadDistribution"], err = enumProp(props, "loadDistribution",
		[]string{"Default", "SourceIP", "SourceIPProtocol"}, "Default"); err != nil {
		return nil, err
	}

	out["disableOutboundSnat"] = boolOr(props, "disableOutboundSnat", false)
	out["enableConnectionTracking"] = boolOr(props, "enableConnectionTracking", false)

	if err = ruleRefs(out, props, name, true); err != nil {
		return nil, err
	}

	if pr := subID(props, "probe"); pr != "" {
		out["probe"] = idRef(pr)
	}

	return lbObj(name, out), nil
}

// ruleCommon copies the fields shared by load balancing rules, NAT rules and outbound rules.
func ruleCommon(out, props map[string]any, minIdle int) error {
	idle, err := intIn(props, "idleTimeoutInMinutes", minIdle, lbMaxIdle, minIdle)
	if err != nil {
		return err
	}

	out["idleTimeoutInMinutes"] = idle
	out["enableFloatingIP"] = boolOr(props, "enableFloatingIP", false)
	out["enableTcpReset"] = boolOr(props, "enableTcpReset", false)

	return nil
}

// ruleRefs copies the frontend and backend pool references of a rule.
func ruleRefs(out, props map[string]any, name string, poolRequired bool) error {
	fe := subID(props, propFrontendIP)
	if fe == "" {
		return netBad("Rule %s requires frontendIPConfiguration.id.", name)
	}

	out[propFrontendIP] = idRef(fe)

	var pools []any

	if one := subID(props, propBackendPl); one != "" {
		pools = append(pools, idRef(one))
	}

	for _, v := range anyList(props["backendAddressPools"]) {
		if id := refIDOf(v); id != "" {
			pools = append(pools, idRef(id))
		}
	}

	if len(pools) == 0 && poolRequired {
		return netBad("Rule %s requires a backend address pool.", name)
	}

	if len(pools) > 0 {
		out[propBackendPl] = pools[0]
		out["backendAddressPools"] = pools
	}

	return nil
}

func normNATRule(name string, props map[string]any) (*netObj, error) {
	proto, err := enumProp(props, propProtocol, []string{protoTCP, protoUDP, protoAll}, "")
	if err != nil {
		return nil, err
	}

	out := map[string]any{propProtocol: proto}

	bp, err := intIn(props, "backendPort", 1, maxPort, -1)
	if err != nil {
		return nil, err
	}

	out["backendPort"] = bp

	_, hasPort := props["frontendPort"]
	_, hasStart := props["frontendPortRangeStart"]
	_, hasEnd := props["frontendPortRangeEnd"]

	switch {
	case hasPort && (hasStart || hasEnd):
		return nil, netBad("Inbound NAT rule %s sets both frontendPort and a frontend port range.", name)
	case hasStart != hasEnd:
		return nil, netBad("Inbound NAT rule %s needs both frontendPortRangeStart and frontendPortRangeEnd.", name)
	case !hasPort && !hasStart:
		return nil, netBad("Inbound NAT rule %s requires frontendPort or a frontend port range.", name)
	}

	if err = natPorts(out, props, name, hasPort); err != nil {
		return nil, err
	}

	if err = ruleCommon(out, props, lbMinIdle); err != nil {
		return nil, err
	}

	if err = ruleRefs(out, props, name, hasStart); err != nil {
		return nil, err
	}

	return lbObj(name, out), nil
}

func natPorts(out, props map[string]any, name string, single bool) error {
	if single {
		fp, err := intIn(props, "frontendPort", 1, maxPort-1, -1)
		if err != nil {
			return err
		}

		out["frontendPort"] = fp

		return nil
	}

	start, err := intIn(props, "frontendPortRangeStart", 1, maxPort-1, -1)
	if err != nil {
		return err
	}

	end, err := intIn(props, "frontendPortRangeEnd", 1, maxPort-1, -1)
	if err != nil {
		return err
	}

	if start > end {
		return netBad("Inbound NAT rule %s: frontendPortRangeStart must not exceed frontendPortRangeEnd.", name)
	}

	out["frontendPortRangeStart"], out["frontendPortRangeEnd"] = start, end

	return nil
}

func normNATPool(_ *lbCtx, name string, props map[string]any) (*netObj, error) {
	proto, err := enumProp(props, propProtocol, []string{protoTCP, protoUDP}, "")
	if err != nil {
		return nil, err
	}

	out := map[string]any{propProtocol: proto}

	if err = natPorts(out, props, name, false); err != nil {
		return nil, err
	}

	if out["backendPort"], err = intIn(props, "backendPort", 1, maxPort, -1); err != nil {
		return nil, err
	}

	if err = ruleCommon(out, props, lbMinIdle); err != nil {
		return nil, err
	}

	if err = ruleRefs(out, props, name, false); err != nil {
		return nil, err
	}

	delete(out, propBackendPl)
	delete(out, "backendAddressPools")

	return lbObj(name, out), nil
}

func normOutbound(c *lbCtx, name string, props map[string]any) (*netObj, error) {
	if c.sku != skuStandard {
		return nil, netBad("Outbound rule %s: outbound rules require the Standard SKU load balancer.", name)
	}

	proto, err := enumProp(props, propProtocol, []string{protoTCP, protoUDP, protoAll}, "")
	if err != nil {
		return nil, err
	}

	out := map[string]any{propProtocol: proto}

	pool := subID(props, propBackendPl)
	if pool == "" {
		return nil, netBad("Outbound rule %s requires a backend address pool.", name)
	}

	out[propBackendPl] = idRef(pool)

	var fes []any

	for _, v := range anyList(props["frontendIPConfigurations"]) {
		if id := refIDOf(v); id != "" {
			fes = append(fes, idRef(id))
		}
	}

	if len(fes) == 0 {
		return nil, netBad("Outbound rule %s requires at least one frontend IP configuration.", name)
	}

	out["frontendIPConfigurations"] = fes

	if out["allocatedOutboundPorts"], err = intIn(props, "allocatedOutboundPorts", 0, maxPort, 0); err != nil {
		return nil, err
	}

	if out["idleTimeoutInMinutes"], err = intIn(
		props,
		"idleTimeoutInMinutes",
		lbMinIdle,
		outMaxIdle,
		lbMinIdle,
	); err != nil {
		return nil, err
	}

	out["enableTcpReset"] = boolOr(props, "enableTcpReset", true)

	return lbObj(name, out), nil
}

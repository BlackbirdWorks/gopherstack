package azurearm

import (
	"maps"
	"net/http"
	"net/netip"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	ruleTypePath     = "Microsoft.Network/networkSecurityGroups/securityRules"
	defRuleTypePath  = "Microsoft.Network/networkSecurityGroups/defaultSecurityRules"
	minRulePriority  = 100
	maxRulePriority  = 4096
	ruleInbound      = "Inbound"
	ruleOutbound     = "Outbound"
	ruleAllow        = "Allow"
	ruleDeny         = "Deny"
	ruleWildcard     = "*"
	maxPort          = 65535
	routeTypePath    = "Microsoft.Network/routeTables/routes"
	nextHopAppliance = "VirtualAppliance"
	propNextHopType  = "nextHopType"
	propNextHopIP    = "nextHopIpAddress"
	propSourcePrefix = "sourceAddressPrefix"
	propDestPrefix   = "destinationAddressPrefix"
	propSourcePort   = "sourcePortRange"
	propDestPort     = "destinationPortRange"
)

//nolint:gochecknoglobals // read-only tables
var (
	tagRe       = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9.]*$`)
	ruleProtos  = []string{"Tcp", "Udp", "Icmp", "Esp", "Ah", ruleWildcard}
	ruleAccess  = []string{ruleAllow, ruleDeny}
	ruleDirs    = []string{ruleInbound, ruleOutbound}
	nextHopKind = []string{"VirtualNetworkGateway", "VnetLocal", "Internet", nextHopAppliance, netNone}
)

type defaultRuleDef struct {
	name, desc, dir, access, src, dst string
	priority                          int
}

//nolint:gochecknoglobals // read-only table
var defaultRuleDefs = []defaultRuleDef{
	{"AllowVnetInBound", "Allow inbound traffic from all VMs in VNET", ruleInbound, ruleAllow,
		ruleTagVNet, ruleTagVNet, 65000},
	{"AllowAzureLoadBalancerInBound", "Allow inbound traffic from azure load balancer", ruleInbound, ruleAllow,
		"AzureLoadBalancer", ruleWildcard, 65001},
	{"DenyAllInBound", "Deny all inbound traffic", ruleInbound, ruleDeny, ruleWildcard, ruleWildcard, 65500},
	{"AllowVnetOutBound", "Allow outbound traffic from all VMs to all VMs in VNET", ruleOutbound, ruleAllow,
		ruleTagVNet, ruleTagVNet, 65000},
	{"AllowInternetOutBound", "Allow outbound traffic from all VMs to Internet", ruleOutbound, ruleAllow,
		ruleWildcard, "Internet", 65001},
	{"DenyAllOutBound", "Deny all outbound traffic", ruleOutbound, ruleDeny, ruleWildcard, ruleWildcard, 65500},
}

func defaultRulesFor(nsg *netParent) []map[string]any {
	out := make([]map[string]any, 0, len(defaultRuleDefs))

	for _, d := range defaultRuleDefs {
		out = append(out, defaultRuleBody(nsg, d))
	}

	return out
}

func defaultRuleBody(nsg *netParent, d defaultRuleDef) map[string]any {
	return map[string]any{
		netFieldID: nsg.armID() + "/" + netDefRules + "/" + d.name, fieldName: d.name, fieldType: defRuleTypePath,
		netFieldEtag: `W/"` + uuid.NewSHA1(uuid.NameSpaceOID, []byte(nsg.Obj.GUID+d.name)).String() + `"`,
		fieldProperties: map[string]any{
			"protocol": ruleWildcard, propSourcePort: ruleWildcard, propDestPort: ruleWildcard,
			propSourcePrefix: d.src, propDestPrefix: d.dst, "access": d.access, netPropPriority: d.priority,
			"direction": d.dir, netPropDescription: d.desc, fieldProvisioningState: netStatusSucceeded,
			"sourceAddressPrefixes": []string{}, "destinationAddressPrefixes": []string{},
		},
	}
}

func (p *NetworkProvider) getDefaultRule(nsg *netParent, id ResourceID) (map[string]any, error) {
	for _, d := range defaultRuleDefs {
		if strings.EqualFold(d.name, id.Names[1]) {
			return defaultRuleBody(nsg, d), nil
		}
	}

	return nil, ErrResourceNotFound
}

func canonEnum(v string, allowed []string) (string, bool) {
	for _, a := range allowed {
		if strings.EqualFold(a, v) {
			return a, true
		}
	}

	return "", false
}

func validPortRange(s string) bool {
	if s == ruleWildcard {
		return true
	}

	lo, hi, found := strings.Cut(s, "-")
	a, err := strconv.Atoi(lo)

	if err != nil || a < 0 || a > maxPort {
		return false
	}

	if !found {
		return true
	}

	b, err := strconv.Atoi(hi)

	return err == nil && b >= a && b <= maxPort
}

func validAddressPrefix(s string) bool {
	if s == ruleWildcard {
		return true
	}

	if _, err := netip.ParsePrefix(s); err == nil {
		return true
	}

	if _, err := netip.ParseAddr(s); err == nil {
		return true
	}

	return tagRe.MatchString(s)
}

// normalizeRule validates a security rule definition.
func normalizeRule(name string, props map[string]any, prev *netObj) (*netObj, error) {
	out := takeProps(map[string]any{fieldProperties: props}, fieldProvisioningState)

	for _, c := range []struct {
		key     string
		allowed []string
	}{{"protocol", ruleProtos}, {"access", ruleAccess}, {"direction", ruleDirs}} {
		canon, ok := canonEnum(strOf(out, c.key), c.allowed)
		if !ok {
			return nil, netBad("Security rule %s has missing or invalid %s.", name, c.key)
		}

		out[c.key] = canon
	}

	prio, ok := out[netPropPriority].(float64)
	if !ok || prio < minRulePriority || prio > maxRulePriority || prio != float64(int(prio)) {
		return nil, netErr(http.StatusBadRequest, "SecurityRuleInvalidPriority",
			"Security rule %s has invalid Priority. Value should be between %d and %d.",
			name, minRulePriority, maxRulePriority)
	}

	out[netPropPriority] = int(prio)

	if err := ruleEndpoint(name, out, "source", propSourcePrefix, "sourceAddressPrefixes", propSourcePort,
		"sourcePortRanges"); err != nil {
		return nil, err
	}

	if err := ruleEndpoint(name, out, "destination", propDestPrefix, "destinationAddressPrefixes", propDestPort,
		"destinationPortRanges"); err != nil {
		return nil, err
	}

	normRuleASGs(out)

	o := &netObj{Name: name, Etag: newEtag(), GUID: uuid.NewString(), Props: out}
	if prev != nil {
		o.GUID = prev.GUID
	}

	return o, nil
}

// ruleBuilder normalizes a rule and checks the application security groups it names.
func (p *NetworkProvider) ruleBuilder(loc string) func(string, map[string]any, *netObj) (*netObj, error) {
	return func(name string, props map[string]any, prev *netObj) (*netObj, error) {
		o, err := normalizeRule(name, props, prev)
		if err != nil {
			return nil, err
		}

		if err = p.checkRuleASGs(loc, o.Props); err != nil {
			return nil, err
		}

		return o, nil
	}
}

func ruleEndpoint(name string, props map[string]any, side, prefix, prefixes, port, ports string) error {
	one, many := strOf(props, prefix), strList(props[prefixes])
	hasASG := len(anyList(props[side+"ApplicationSecurityGroups"])) > 0

	switch {
	case one != "" && len(many) > 0:
		return netErr(http.StatusBadRequest, "SecurityRuleParameterContainsConflictingValues",
			"Security rule %s specifies both %s and %s.", name, prefix, prefixes)
	case hasASG && (one != "" || len(many) > 0):
		return netErr(http.StatusBadRequest, "SecurityRuleParameterContainsConflictingValues",
			"Security rule %s specifies both %sApplicationSecurityGroups and an address prefix.", name, side)
	case !hasASG && one == "" && len(many) == 0:
		return netBad("Security rule %s must specify %s or %s.", name, prefix, prefixes)
	}

	for _, a := range append(many, one) {
		if a != "" && !validAddressPrefix(a) {
			return netErr(http.StatusBadRequest, "SecurityRuleInvalidAddressPrefix",
				"Security rule %s has invalid %s address prefix %s.", name, side, a)
		}
	}

	return rulePorts(name, props, side, port, ports)
}

func rulePorts(name string, props map[string]any, side, port, ports string) error {
	p1, pn := strOf(props, port), strList(props[ports])

	switch {
	case p1 != "" && len(pn) > 0:
		return netErr(http.StatusBadRequest, "SecurityRuleParameterContainsConflictingValues",
			"Security rule %s specifies both %s and %s.", name, port, ports)
	case p1 == "" && len(pn) == 0:
		return netBad("Security rule %s must specify %s or %s.", name, port, ports)
	}

	for _, r := range append(pn, p1) {
		if r != "" && !validPortRange(r) {
			return netErr(http.StatusBadRequest, "SecurityRuleInvalidPortRange",
				"Security rule %s has invalid %s port range %s.", name, side, r)
		}
	}

	return nil
}

func ruleConflict(list []*netObj) error {
	seen := map[string]string{}

	for _, r := range list {
		key := strOf(r.Props, "direction") + "/" + strconv.Itoa(intOf(r.Props[netPropPriority]))
		if other, dup := seen[key]; dup {
			return netErr(http.StatusBadRequest, "SecurityRuleConflict",
				"Rules %s and %s have the same Priority %d and Direction %s.", other, r.Name,
				intOf(r.Props[netPropPriority]), strOf(r.Props, "direction"))
		}

		seen[key] = r.Name
	}

	return nil
}

func intOf(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case float64:
		return int(t)
	}

	return 0
}

// inlineKids normalizes an inline child array of a top-level definition; nil means the key was absent.
func (p *NetworkProvider) inlineKids(
	body map[string]any, key, kt string, old *netParent,
	build func(name string, props map[string]any, prev *netObj) (*netObj, error),
) ([]*netObj, error) {
	raw, has := propsOf(body)[key]
	if !has {
		return nil, nil
	}

	arr, ok := raw.([]any)
	if !ok {
		return nil, netBad("%s must be an array.", key)
	}

	out := make([]*netObj, 0, len(arr))

	for _, e := range arr {
		m, _ := e.(map[string]any)
		name := strOf(m, fieldName)

		if err := validNetName(name, netMaxName); err != nil {
			return nil, err
		}

		if hasObj(out, name) {
			return nil, netBad("%s name %s is used more than once.", key, name)
		}

		var prev *netObj
		if old != nil {
			prev, _ = old.kid(kt, name)
		}

		o, err := build(name, propsOf(m), prev)
		if err != nil {
			return nil, err
		}

		out = append(out, o)
	}

	return out, nil
}

func (p *NetworkProvider) putNSG(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	old, _ := p.top(netNSGs, id.ResourceGroup, id.Names[0])

	loc := normLocation(strOf(body, fieldLocation))
	if loc == "" && old != nil {
		loc = old.Obj.Location
	}

	rules, err := p.inlineKids(body, netRules, netRules, old, p.ruleBuilder(loc))
	if err != nil {
		return nil, false, err
	}

	if err = ruleConflict(rules); err != nil {
		return nil, false, err
	}

	it, _, created := p.upsertTop(netNSGs, id, body)
	it.Obj.Props = takeProps(body, netRules, netDefRules, "networkInterfaces", "subnets", fieldProvisioningState,
		netFieldResourceGUID)

	if rules != nil {
		it.Kids[netRules] = rules
	}

	return p.bodyNSG(id, it), created, nil
}

func (p *NetworkProvider) bodyNSG(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	rules := make([]any, 0, len(it.Kids[netRules]))
	for _, r := range it.Kids[netRules] {
		rules = append(rules, p.bodyRule(kidID(id, netRules, r.Name), r))
	}

	defs := make([]any, 0, len(defaultRuleDefs))
	for _, d := range defaultRulesFor(it) {
		defs = append(defs, d)
	}

	props[netRules] = rules
	props[netDefRules] = defs
	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID

	if nics := p.nsgNICs(it.armID()); len(nics) > 0 {
		props["networkInterfaces"] = idRefs(nics)
	}

	if subs := p.nsgSubnets(it.armID()); len(subs) > 0 {
		props["subnets"] = idRefs(subs)
	}

	return p.envelope(id, netNSGs, &it.Obj, props)
}

func idRefs(ids []string) []any {
	out := make([]any, 0, len(ids))
	for _, i := range ids {
		out = append(out, map[string]any{netFieldID: i})
	}

	return out
}

func (p *NetworkProvider) bodyRule(id ResourceID, r *netObj) map[string]any {
	props := map[string]any{}

	maps.Copy(props, r.Props)

	props[fieldProvisioningState] = netStatusSucceeded

	return map[string]any{
		netFieldID: id.ARMID(), fieldName: r.Name, fieldType: ruleTypePath, netFieldEtag: r.Etag,
		fieldProperties: props,
	}
}

func (p *NetworkProvider) writeRule(nsg *netParent, name string, body map[string]any) (bool, error) {
	prev, idx := nsg.kid(netRules, name)

	r, err := p.ruleBuilder(nsg.Obj.Location)(name, propsOf(body), prev)
	if err != nil {
		return false, err
	}

	list := append([]*netObj{}, nsg.Kids[netRules]...)

	if idx >= 0 {
		list[idx] = r
	} else {
		list = append(list, r)
	}

	if err = ruleConflict(list); err != nil {
		return false, err
	}

	nsg.Kids[netRules] = list
	p.touch(nsg)

	return idx < 0, nil
}

func (p *NetworkProvider) nsgSubnets(nsgID string) []string {
	var out []string

	for _, v := range p.ofType(netVNets) {
		for _, s := range v.Kids[netSubnets] {
			if strings.EqualFold(subID(s.Props, propSubnetNSG), nsgID) {
				out = append(out, subnetIDOf(v, s.Name))
			}
		}
	}

	return out
}

func (p *NetworkProvider) nsgNICs(nsgID string) []string {
	var out []string

	for _, n := range p.ofType(netNICs) {
		if strings.EqualFold(subID(n.Obj.Props, propSubnetNSG), nsgID) {
			out = append(out, n.armID())
		}
	}

	return out
}

// --- route tables ---

func normalizeRoute(name string, props map[string]any, prev *netObj) (*netObj, error) {
	out := takeProps(map[string]any{fieldProperties: props}, fieldProvisioningState)

	if _, err := netParsePrefix(strOf(out, netFieldAddrPrefix)); err != nil {
		return nil, err
	}

	kind, ok := canonEnum(strOf(out, propNextHopType), nextHopKind)
	if !ok {
		return nil, netBad("Route %s has missing or invalid nextHopType.", name)
	}

	out[propNextHopType] = kind

	ip := strOf(out, propNextHopIP)

	switch {
	case kind == nextHopAppliance && ip == "":
		return nil, netBad("Route %s of next hop type VirtualAppliance requires nextHopIpAddress.", name)
	case kind != nextHopAppliance && ip != "":
		return nil, netBad("Route %s: nextHopIpAddress is only allowed with next hop type VirtualAppliance.", name)
	case ip != "":
		if _, err := netip.ParseAddr(ip); err != nil {
			return nil, netBad("Route %s has invalid nextHopIpAddress %s.", name, ip)
		}
	}

	if _, set := out["hasBgpOverride"].(bool); !set {
		out["hasBgpOverride"] = false
	}

	o := &netObj{Name: name, Etag: newEtag(), GUID: uuid.NewString(), Props: out}
	if prev != nil {
		o.GUID = prev.GUID
	}

	return o, nil
}

func (p *NetworkProvider) putRT(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	old, _ := p.top(netRTs, id.ResourceGroup, id.Names[0])

	routes, err := p.inlineKids(body, netRoutes, netRoutes, old, normalizeRoute)
	if err != nil {
		return nil, false, err
	}

	it, _, created := p.upsertTop(netRTs, id, body)
	it.Obj.Props = takeProps(body, netRoutes, "subnets", fieldProvisioningState, netFieldResourceGUID)

	if routes != nil {
		it.Kids[netRoutes] = routes
	}

	return p.bodyRT(id, it), created, nil
}

func (p *NetworkProvider) bodyRT(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	if _, ok := props["disableBgpRoutePropagation"].(bool); !ok {
		props["disableBgpRoutePropagation"] = false
	}

	routes := make([]any, 0, len(it.Kids[netRoutes]))
	for _, r := range it.Kids[netRoutes] {
		routes = append(routes, p.bodyRoute(kidID(id, netRoutes, r.Name), r))
	}

	props[netRoutes] = routes
	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID

	if subs := p.rtSubnets(it.armID()); len(subs) > 0 {
		props["subnets"] = idRefs(subs)
	}

	return p.envelope(id, netRTs, &it.Obj, props)
}

func (p *NetworkProvider) bodyRoute(id ResourceID, r *netObj) map[string]any {
	props := map[string]any{}

	maps.Copy(props, r.Props)

	props[fieldProvisioningState] = netStatusSucceeded

	return map[string]any{
		netFieldID: id.ARMID(), fieldName: r.Name, fieldType: routeTypePath, netFieldEtag: r.Etag,
		fieldProperties: props,
	}
}

func (p *NetworkProvider) writeRoute(rt *netParent, name string, body map[string]any) (bool, error) {
	prev, idx := rt.kid(netRoutes, name)

	r, err := normalizeRoute(name, propsOf(body), prev)
	if err != nil {
		return false, err
	}

	if idx >= 0 {
		rt.Kids[netRoutes][idx] = r
	} else {
		rt.Kids[netRoutes] = append(rt.Kids[netRoutes], r)
	}

	p.touch(rt)

	return idx < 0, nil
}

func (p *NetworkProvider) rtSubnets(rtID string) []string {
	var out []string

	for _, v := range p.ofType(netVNets) {
		for _, s := range v.Kids[netSubnets] {
			if strings.EqualFold(subID(s.Props, propSubnetRT), rtID) {
				out = append(out, subnetIDOf(v, s.Name))
			}
		}
	}

	return out
}

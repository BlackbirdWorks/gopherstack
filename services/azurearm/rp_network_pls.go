package azurearm

import (
	"maps"
	"strings"

	"github.com/google/uuid"
)

const (
	netPLS           = "privateLinkServices"
	plsIPConfigPath  = "Microsoft.Network/privateLinkServices/ipConfigurations"
	plsPropIPCfgs    = "ipConfigurations"
	plsPropFrontend  = "loadBalancerFrontendIpConfigurations"
	plsPropVis       = "visibility"
	plsPropAuto      = "autoApproval"
	plsPropSubs      = "subscriptions"
	plsPropAccess    = "accessMode"
	plsMaxIPConfigs  = 8
	plsPolicyKey     = "privateLinkServiceNetworkPolicies"
	auxPLSAlias      = "alias"
	netPropIPVersion = "privateIPAddressVersion"
	netPropIPMethod  = "privateIPAllocationMethod"
	peLinkIDKey      = "privateLinkServiceId"
)

func plsSubscriptions(props map[string]any, key string) []string {
	return strList(propsMap(props[key])[plsPropSubs])
}

func containsSub(list []string, sub string) bool {
	for _, s := range list {
		if strings.EqualFold(s, sub) {
			return true
		}
	}

	return false
}

func checkSubscriptionList(props map[string]any, key string) error {
	for _, s := range plsSubscriptions(props, key) {
		if _, err := uuid.Parse(s); err != nil {
			return netBad("%s.subscriptions entry '%s' is not a valid subscription id.", key, s)
		}
	}

	return nil
}

func (p *NetworkProvider) putPLS(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)
	old, existed := p.top(netPLS, id.ResourceGroup, id.Names[0])

	loc := normLocation(strOf(body, fieldLocation))
	if loc == "" && existed {
		loc = old.Obj.Location
	}

	if mode := strOf(props, plsPropAccess); mode != "" {
		if _, ok := canonEnum(mode, []string{"Default", "Restricted"}); !ok {
			return nil, false, netBad("accessMode must be Default or Restricted.")
		}
	}

	for _, key := range []string{plsPropVis, plsPropAuto} {
		if err := checkSubscriptionList(props, key); err != nil {
			return nil, false, err
		}
	}

	frontends, err := p.plsFrontends(id, loc, props)
	if err != nil {
		return nil, false, err
	}

	cfgs, nicName, err := p.planPLSNIC(id, old, props)
	if err != nil {
		return nil, false, err
	}

	it, obj, created := p.upsertTop(netPLS, id, body)
	obj.Props = takeProps(body, plsPropIPCfgs, plsPropFrontend, "networkInterfaces", "privateEndpointConnections",
		"alias", fieldProvisioningState)
	obj.Props[plsPropFrontend] = frontends
	obj.Props[plsPropIPCfgs] = cfgs
	obj.Aux[auxPLSAlias] = id.Names[0] + "." + obj.GUID + "." + loc + ".azure.privatelinkservice"

	p.installPLSNIC(it, id, loc, nicName, cfgs)

	return p.bodyPLS(id, it), created, nil
}

func (p *NetworkProvider) plsFrontends(id ResourceID, loc string, props map[string]any) ([]any, error) {
	raw := anyList(props[plsPropFrontend])
	if len(raw) == 0 {
		return nil, netBad("Private link service %s requires at least one load balancer frontend IP configuration.",
			id.Names[0])
	}

	out := make([]any, 0, len(raw))

	for _, v := range raw {
		fid := refIDOf(v)

		f, ok := p.lbKidByID(fid, lbFrontends)
		if !ok {
			return nil, refMissing(fid)
		}

		rid, _ := ParseGenericResourcePath(fid)

		lb, _ := p.top(netLBs, rid.ResourceGroup, rid.Names[0])
		if lb.Obj.Location != loc {
			return nil, refMissing(fid)
		}

		sku, _ := lb.Obj.Extra[netFieldSKU].(map[string]any)
		if !strings.EqualFold(strOf(sku, fieldName), skuStandard) || subID(f.Props, netFieldSubnet) == "" {
			return nil, netBad("Load balancer frontend %s must belong to a Standard internal load balancer to be "+
				"exposed by a private link service.", fid)
		}

		out = append(out, idRef(fid))
	}

	return out, nil
}

// planPLSNIC allocates the NAT addresses of the service without touching state; the first return value holds the
// stored ipConfigurations, the second the managed interface name.
func (p *NetworkProvider) planPLSNIC(id ResourceID, old *netParent, props map[string]any) ([]any, string, error) {
	raw := anyList(props[plsPropIPCfgs])
	if len(raw) == 0 || len(raw) > plsMaxIPConfigs {
		return nil, "", netBad("Private link service %s requires between 1 and %d IP configurations.",
			id.Names[0], plsMaxIPConfigs)
	}

	nicName := id.Names[0] + ".nic." + uuid.NewString()
	if old != nil && old.Obj.Aux[auxPENIC] != "" {
		nicName = old.Obj.Aux[auxPENIC]
	}

	nicID := ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: namespaceMicrosoftNetwork,
		Types: []string{netNICs}, Names: []string{nicName},
	}.ARMID()

	prev := p.plsPrevAddrs(old)
	taken := map[string]map[string]string{}
	base := p.usedAddrsSkip(nicID, "")
	out := make([]any, 0, len(raw))
	seen := map[string]bool{}
	primaries := 0

	for _, e := range raw {
		m, _ := e.(map[string]any)

		cfg, perr := p.planPLSConfig(nicID, m, base, taken, prev, seen)
		if perr != nil {
			return nil, "", perr
		}

		if b, _ := propsOf(cfg)["primary"].(bool); b {
			primaries++
		}

		out = append(out, cfg)
	}

	if primaries > 1 {
		return nil, "", netBad("Private link service %s has more than one primary IP configuration.", id.Names[0])
	}

	if primaries == 0 {
		first, _ := out[0].(map[string]any)
		propsOf(first)["primary"] = true
	}

	return out, nicName, nil
}

func (p *NetworkProvider) plsPrevAddrs(old *netParent) map[string]string {
	prev := map[string]string{}
	if old == nil {
		return prev
	}

	for _, c := range anyList(old.Obj.Props[plsPropIPCfgs]) {
		m, _ := c.(map[string]any)
		prev[strings.ToLower(strOf(m, fieldName))] = strOf(propsOf(m), propPrivateIP)
	}

	return prev
}

func (p *NetworkProvider) planPLSConfig(
	nicID string, m map[string]any, base, taken map[string]map[string]string,
	prev map[string]string, seen map[string]bool,
) (map[string]any, error) {
	name := strOf(m, fieldName)
	if err := validNetName(name, netMaxName); err != nil {
		return nil, err
	}

	if seen[strings.ToLower(name)] {
		return nil, netBad("IP configuration name %s is used more than once.", name)
	}

	seen[strings.ToLower(name)] = true

	cp := propsOf(m)
	sid := subID(cp, netFieldSubnet)

	_, sn, ok := p.subnetByID(sid)
	if !ok {
		return nil, refMissing(sid)
	}

	if sn.Props[plsPolicyKey] != netDisabled {
		return nil, netBad("Subnet %s must have privateLinkServiceNetworkPolicies set to Disabled to host a private "+
			"link service IP configuration.", sid)
	}

	method, ok := canonEnum(firstNonEmpty(strOf(cp, netPropIPMethod), methodDynamic),
		[]string{methodDynamic, methodStatic})
	if !ok {
		return nil, netBad("IP configuration %s has an invalid privateIPAllocationMethod.", name)
	}

	if v := strOf(cp, netPropIPVersion); v != "" && !strings.EqualFold(v, versionV4) {
		return nil, netBad("Private link service IP configurations support only IPv4.")
	}

	key := strings.ToLower(sid)
	if taken[key] == nil {
		taken[key] = maps.Clone(base[key])
		if taken[key] == nil {
			taken[key] = map[string]string{}
		}
	}

	cfgID := nicID + "/" + netIPConfigs + "/" + name

	addr, err := assignIP(cfgID, method, strOf(cp, propPrivateIP), subnetPrefixes(sn), taken[key],
		prev[strings.ToLower(name)])
	if err != nil {
		return nil, err
	}

	taken[key][addr] = cfgID
	primary, _ := cp["primary"].(bool)

	return map[string]any{
		fieldName: name,
		fieldProperties: map[string]any{
			netFieldSubnet: idRef(sid), propPrivateIP: addr, netPropIPMethod: method,
			netPropIPVersion: versionV4, cmpFieldPrimary: primary,
		},
	}, nil
}

func (p *NetworkProvider) installPLSNIC(it *netParent, id ResourceID, loc, nicName string, cfgs []any) {
	if oldName := it.Obj.Aux[auxPENIC]; oldName != "" && !strings.EqualFold(oldName, nicName) {
		delete(p.items, netKey(netNICs, id.ResourceGroup, oldName))
	}

	list := make([]any, 0, len(cfgs))

	for _, c := range cfgs {
		m, _ := c.(map[string]any)
		cp := maps.Clone(propsOf(m))

		list = append(list, map[string]any{fieldName: strOf(m, fieldName), fieldProperties: cp})
	}

	key := netKey(netNICs, id.ResourceGroup, nicName)
	guid := uuid.NewString()

	if ex, ok := p.items[key]; ok {
		guid = ex.Obj.GUID
	}

	p.items[key] = &netParent{
		Type: netNICs, RG: id.ResourceGroup, Sub: id.SubscriptionID, Kids: map[string][]*netObj{},
		Obj: netObj{
			Name: nicName, Location: loc, Etag: newEtag(), GUID: guid,
			Props: map[string]any{netIPConfigs: list, "privateLinkService": idRef(it.armID())},
			Aux: map[string]string{
				"mac": macFor(guid), "suffix": strings.ReplaceAll(guid, "-", "")[:8] + ".bx.internal.cloudapp.net",
				"pe": it.armID(),
			},
		},
	}
	it.Obj.Aux[auxPENIC] = nicName
}

func (p *NetworkProvider) bodyPLS(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	self := it.armID()
	cfgs := make([]any, 0, len(anyList(it.Obj.Props[plsPropIPCfgs])))

	for _, c := range anyList(it.Obj.Props[plsPropIPCfgs]) {
		m, _ := c.(map[string]any)
		cp := maps.Clone(propsOf(m))
		cp[fieldProvisioningState] = netStatusSucceeded
		cfgs = append(cfgs, map[string]any{
			netFieldID: self + "/" + plsPropIPCfgs + "/" + strOf(m, fieldName), fieldName: strOf(m, fieldName),
			fieldType: plsIPConfigPath, netFieldEtag: it.Obj.Etag, fieldProperties: cp,
		})
	}

	props[plsPropIPCfgs] = cfgs

	nics := []any{}
	if nic := p.peNIC(it); nic != nil {
		nics = append(nics, idRef(nic.armID()))
	}

	connList := p.connsOf(self)
	conns := make([]any, 0, len(connList))

	for _, c := range connList {
		conns = append(conns, p.targetConnBody(c, self))
	}

	props["networkInterfaces"] = nics
	props["privateEndpointConnections"] = conns
	props["alias"] = it.Obj.Aux[auxPLSAlias]
	props[fieldProvisioningState] = netStatusSucceeded

	if _, ok := props["enableProxyProtocol"].(bool); !ok {
		props["enableProxyProtocol"] = false
	}

	return p.envelope(id, netPLS, &it.Obj, props)
}

// plsUsingFrontend lists the private link services that expose the load balancer frontend.
func (p *NetworkProvider) plsUsingFrontend(frontendID string) []string {
	var out []string

	for _, s := range p.ofType(netPLS) {
		if hasRef(s.Obj.Props, plsPropFrontend, frontendID) {
			out = append(out, s.armID())
		}
	}

	return out
}

func (p *NetworkProvider) plsUsingLB(lbID string) []string {
	var out []string

	for _, s := range p.ofType(netPLS) {
		for _, v := range anyList(s.Obj.Props[plsPropFrontend]) {
			if strings.HasPrefix(strings.ToLower(refIDOf(v)), strings.ToLower(lbID)+"/") {
				out = append(out, s.armID())

				break
			}
		}
	}

	return out
}

func (p *NetworkProvider) afterPLSRemoved(it *netParent) {
	for _, c := range p.connsOf(it.armID()) {
		c.pe.Obj.Aux["gone:"+c.name] = "1"
		p.touch(c.pe)
	}

	delete(p.items, netKey(netNICs, it.RG, it.Obj.Aux[auxPENIC]))
}

// plsAutoApproves reports whether the private link service approves connections of the subscription unasked.
func (p *NetworkProvider) plsAutoApproves(rid ResourceID, sub string) bool {
	s, ok := p.top(netPLS, rid.ResourceGroup, rid.Names[0])
	if !ok {
		return false
	}

	return containsSub(plsSubscriptions(s.Obj.Props, plsPropAuto), sub)
}

// plsVisible reports whether the subscription can see the private link service.
func plsVisible(s *netParent, sub string) bool {
	return strings.EqualFold(s.Sub, sub) || containsSub(plsSubscriptions(s.Obj.Props, plsPropVis), sub) ||
		containsSub(plsSubscriptions(s.Obj.Props, plsPropAuto), sub)
}

// CheckPLSVisibility answers the checkPrivateLinkServiceVisibility actions; rg is empty for the subscription form.
func (p *NetworkProvider) CheckPLSVisibility(sub, rg, loc, alias string) map[string]any {
	p.mu.RLock("CheckPLSVisibility")
	defer p.mu.RUnlock()

	for _, s := range p.ofType(netPLS) {
		if !strings.EqualFold(s.Obj.Aux[auxPLSAlias], alias) || s.Obj.Location != normLocation(loc) {
			continue
		}

		if rg != "" && !strings.EqualFold(s.RG, rg) {
			continue
		}

		return map[string]any{"visible": plsVisible(s, sub)}
	}

	return map[string]any{"visible": false}
}

// AutoApprovedPLS lists the services in loc that approve connections of the subscription without review.
func (p *NetworkProvider) AutoApprovedPLS(sub, rg, loc string) []map[string]any {
	p.mu.RLock("AutoApprovedPLS")
	defer p.mu.RUnlock()

	out := []map[string]any{}

	for _, s := range p.ofType(netPLS) {
		if s.Obj.Location != normLocation(loc) || !containsSub(plsSubscriptions(s.Obj.Props, plsPropAuto), sub) {
			continue
		}

		if rg != "" && !strings.EqualFold(s.RG, rg) {
			continue
		}

		out = append(out, map[string]any{"privateLinkService": s.armID()})
	}

	return out
}

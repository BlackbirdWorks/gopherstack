package azurearm

import (
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const (
	netLBs         = "loadBalancers"
	netNATs        = "natGateways"
	netPrefixes    = "publicIPPrefixes"
	netASGs        = "applicationSecurityGroups"
	netPEs         = "privateEndpoints"
	lbFrontends    = "frontendIPConfigurations"
	lbPools        = "backendAddressPools"
	lbProbes       = "probes"
	lbRules        = "loadBalancingRules"
	lbNATRules     = "inboundNatRules"
	lbNATPools     = "inboundNatPools"
	lbOutbound     = "outboundRules"
	lbNICs         = "networkInterfaces"
	nicLBs         = "loadBalancers"
	peZoneGroups   = "privateDnsZoneGroups"
	skuGateway     = "Gateway"
	tierRegional   = "Regional"
	tierGlobal     = "Global"
	protoAll       = "All"
	protoTCP       = "Tcp"
	protoUDP       = "Udp"
	propNICPools   = "loadBalancerBackendAddressPools"
	propNICNATs    = "loadBalancerInboundNatRules"
	propNICASGs    = "applicationSecurityGroups"
	propFrontendIP = "frontendIPConfiguration"
	propBackendPl  = "backendAddressPool"
	lbTypePrefix   = "Microsoft.Network/loadBalancers/"

	propFQDN            = "fqdn"
	propTTL             = "ttl"
	propIPAddress       = "ipAddress"
	propProtocol        = "protocol"
	propGroupIDs        = "groupIds"
	propActionsRequired = "actionsRequired"
	propDestASGs        = "destinationApplicationSecurityGroups"
	propSrcASGs         = "sourceApplicationSecurityGroups"
	propPrivateIP       = "privateIPAddress"
	natPropPIPs         = "publicIpAddresses"
	natPropPIPsV6       = "publicIpAddressesV6"
	natPropPrefixes     = "publicIpPrefixes"
	natPropPrefixesV6   = "publicIpPrefixesV6"
)

// lbKidTypes are the load balancer child collections in body order.
//
//nolint:gochecknoglobals // read-only table
var lbKidTypes = []string{lbFrontends, lbPools, lbProbes, lbRules, lbNATRules, lbNATPools, lbOutbound}

// lbCtx carries what child validation needs from the load balancer being written.
type lbCtx struct {
	taken      map[string]map[string]string
	id, sku    string
	loc        string
	isInternal bool
}

func idRef(id string) map[string]any { return map[string]any{netFieldID: id} }

// childName extracts the name of a same-load-balancer child reference, or "" when ref points elsewhere.
func (c *lbCtx) childName(ref, kt string) string {
	prefix := strings.ToLower(c.id + "/" + kt + "/")
	if !strings.HasPrefix(strings.ToLower(ref), prefix) {
		return ""
	}

	return ref[len(prefix):]
}

func (c *lbCtx) kidID(kt, name string) string { return c.id + "/" + kt + "/" + name }

func parseLBSKU(body map[string]any) (string, string, error) {
	in, _ := body[netFieldSKU].(map[string]any)

	name, ok := canonEnum(firstNonEmpty(strOf(in, fieldName), skuBasic), []string{skuBasic, skuStandard, skuGateway})
	if !ok {
		return "", "", netBad("Load balancer SKU name must be Basic, Standard or Gateway.")
	}

	tier, ok := canonEnum(firstNonEmpty(strOf(in, netFieldTier), tierRegional), []string{tierRegional, tierGlobal})
	if !ok || (tier == tierGlobal && name != skuStandard) {
		return "", "", netBad("Load balancer SKU tier must be Regional, or Global with the Standard SKU.")
	}

	return name, tier, nil
}

func (p *NetworkProvider) putLB(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	sku, tier, err := parseLBSKU(body)
	if err != nil {
		return nil, false, err
	}

	old, existed := p.top(netLBs, id.ResourceGroup, id.Names[0])
	loc := normLocation(strOf(body, fieldLocation))

	if loc == "" && existed {
		loc = old.Obj.Location
	}

	c := &lbCtx{
		id: id.ARMID(), sku: sku, loc: loc, taken: p.usedAddrsSkip("", id.ARMID()),
	}

	kids := map[string][]*netObj{}

	if old != nil {
		for k, v := range old.Kids {
			kids[k] = slices.Clone(v)
		}
	}

	props := propsOf(body)

	for _, kt := range lbKidTypes {
		raw, has := props[kt]
		if !has {
			continue
		}

		list, berr := p.buildLBKids(c, kt, raw, kids[kt])
		if berr != nil {
			return nil, false, berr
		}

		kids[kt] = list
	}

	if err = p.validateLB(c, kids); err != nil {
		return nil, false, err
	}

	it, obj, created := p.upsertTop(netLBs, id, body)
	obj.Props = takeProps(
		body,
		append([]string{fieldProvisioningState, netFieldResourceGUID, "scope"}, lbKidTypes...)...)
	obj.Extra = map[string]any{netFieldSKU: map[string]any{fieldName: sku, netFieldTier: tier}}
	it.Kids = kids

	return p.bodyLB(id, it), created, nil
}

func (p *NetworkProvider) buildLBKids(c *lbCtx, kt string, raw any, prevList []*netObj) ([]*netObj, error) {
	arr, ok := raw.([]any)
	if !ok {
		return nil, netBad("%s must be an array.", kt)
	}

	out := make([]*netObj, 0, len(arr))

	for _, e := range arr {
		m, _ := e.(map[string]any)
		name := strOf(m, fieldName)

		if err := validNetName(name, netMaxName); err != nil {
			return nil, err
		}

		if hasObj(out, name) {
			return nil, netBad("%s name %s is used more than once.", kt, name)
		}

		var prev *netObj

		for _, o := range prevList {
			if strings.EqualFold(o.Name, name) {
				prev = o
			}
		}

		o, err := p.buildLBKid(c, kt, name, m, prev)
		if err != nil {
			return nil, err
		}

		out = append(out, o)
	}

	return out, nil
}

func (p *NetworkProvider) buildLBKid(c *lbCtx, kt, name string, m map[string]any, prev *netObj) (*netObj, error) {
	var (
		o   *netObj
		err error
	)

	props := propsOf(m)

	switch kt {
	case lbFrontends:
		o, err = p.normFrontend(c, name, m, prev)
	case lbPools:
		o, err = p.normPool(c, name, props)
	case lbProbes:
		o, err = normProbe(name, props)
	case lbRules:
		o, err = normLBRule(c, name, props)
	case lbNATRules:
		o, err = normNATRule(name, props)
	case lbNATPools:
		o, err = normNATPool(c, name, props)
	default:
		o, err = normOutbound(c, name, props)
	}

	if err != nil {
		return nil, err
	}

	if prev != nil {
		o.GUID = prev.GUID
	}

	return o, nil
}

func lbObj(name string, props map[string]any) *netObj {
	return &netObj{Name: name, Etag: newEtag(), GUID: uuid.NewString(), Props: props}
}

// validateLB checks the references between the children of one load balancer.
func (p *NetworkProvider) validateLB(c *lbCtx, kids map[string][]*netObj) error {
	if err := p.checkFrontendMix(c, kids[lbFrontends]); err != nil {
		return err
	}

	if err := checkLBRefs(c, kids); err != nil {
		return err
	}

	if err := ruleFrontendConflicts(kids[lbRules], kids[lbNATRules]); err != nil {
		return err
	}

	for _, o := range kids[lbOutbound] {
		for _, f := range anyList(o.Props["frontendIPConfigurations"]) {
			fo := findObj(kids[lbFrontends], c.childName(refIDOf(f), lbFrontends))
			if fo == nil || isPrivateFrontend(fo) {
				return netBad("Outbound rule %s must reference public frontend IP configurations.", o.Name)
			}
		}
	}

	return nil
}

// checkLBRefs requires every reference of a rule-like child to name an existing child of the same load balancer.
func checkLBRefs(c *lbCtx, kids map[string][]*netObj) error {
	for _, kt := range []string{lbRules, lbNATRules, lbNATPools, lbOutbound} {
		for _, o := range kids[kt] {
			for _, ref := range lbRefs(o) {
				n := c.childName(ref.id, ref.kt)
				if n != "" && findObj(kids[ref.kt], n) != nil {
					continue
				}

				return netErr(http.StatusBadRequest, netCodeRef,
					"Resource %s referenced by %s of load balancer %s was not found. A load balancer can only "+
						"reference its own %s.", ref.id, o.Name, c.id, ref.kt)
			}
		}
	}

	return nil
}

type lbRef struct{ id, kt string }

// lbRefs lists every same-load-balancer reference a rule-like child holds.
func lbRefs(o *netObj) []lbRef {
	var out []lbRef

	add := func(v any, kt string) {
		if id := refIDOf(v); id != "" {
			out = append(out, lbRef{id, kt})
		}
	}

	add(o.Props[propFrontendIP], lbFrontends)
	add(o.Props[propBackendPl], lbPools)
	add(o.Props["probe"], lbProbes)

	for _, v := range anyList(o.Props["backendAddressPools"]) {
		add(v, lbPools)
	}

	for _, v := range anyList(o.Props["frontendIPConfigurations"]) {
		add(v, lbFrontends)
	}

	return out
}

func refIDOf(v any) string {
	m, _ := v.(map[string]any)

	return strOf(m, netFieldID)
}

func findObj(list []*netObj, name string) *netObj {
	for _, o := range list {
		if strings.EqualFold(o.Name, name) {
			return o
		}
	}

	return nil
}

func isPrivateFrontend(f *netObj) bool { return subID(f.Props, netFieldSubnet) != "" }

func (p *NetworkProvider) checkFrontendMix(c *lbCtx, fronts []*netObj) error {
	priv, pub := 0, 0

	for _, f := range fronts {
		if isPrivateFrontend(f) {
			priv++
		} else {
			pub++
		}
	}

	if priv > 0 && pub > 0 {
		return netBad("Load balancer %s cannot have both public and private frontend IP configurations.", c.id)
	}

	c.isInternal = priv > 0

	return nil
}

func ruleFrontendConflicts(rules, nats []*netObj) error {
	seen := map[string]string{}

	for _, r := range rules {
		proto := strOf(r.Props, propProtocol)
		if proto == protoAll {
			continue
		}

		key := strings.ToLower(refIDOf(r.Props[propFrontendIP])) + "/" + proto + "/" + numStr(r.Props["frontendPort"])
		if other, dup := seen[key]; dup {
			return netErr(http.StatusBadRequest, "LoadBalancingRuleFrontendPortConflict",
				"Load balancing rules %s and %s use the same frontend IP configuration, protocol and frontend port.",
				other, r.Name)
		}

		seen[key] = r.Name
	}

	seenNAT := map[string]string{}

	for _, r := range nats {
		if _, v2 := r.Props["frontendPortRangeStart"]; v2 {
			continue
		}

		key := strings.ToLower(refIDOf(r.Props[propFrontendIP])) + "/" + strOf(r.Props, propProtocol) + "/" +
			numStr(r.Props["frontendPort"])
		if other, dup := seenNAT[key]; dup {
			return netErr(http.StatusBadRequest, "InboundNatRuleFrontendPortConflict",
				"Inbound NAT rules %s and %s use the same frontend IP configuration, protocol and frontend port.",
				other, r.Name)
		}

		seenNAT[key] = r.Name
	}

	return nil
}

func numStr(v any) string { return strconv.Itoa(intOf(v)) }

// --- body ---

func (p *NetworkProvider) bodyLB(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	for _, kt := range lbKidTypes {
		list := make([]any, 0, len(it.Kids[kt]))
		for _, k := range it.Kids[kt] {
			list = append(list, p.bodyLBKid(it, kt, kidID(id, kt, k.Name), k))
		}

		props[kt] = list
	}

	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID

	return p.envelope(id, netLBs, &it.Obj, props)
}

// refsTo lists the ids of children of kt whose property key references target.
func refsTo(list []*netObj, kt, lbID, key, target string) []string {
	var out []string

	for _, o := range list {
		hit := strings.EqualFold(refIDOf(o.Props[key]), target)

		for _, v := range anyList(o.Props[key+"s"]) {
			hit = hit || strings.EqualFold(refIDOf(v), target)
		}

		if hit {
			out = append(out, lbID+"/"+kt+"/"+o.Name)
		}
	}

	return out
}

func (p *NetworkProvider) bodyLBKid(lb *netParent, kt string, id ResourceID, k *netObj) map[string]any {
	props := map[string]any{}

	maps.Copy(props, k.Props)

	lbID := lb.armID()
	self := id.ARMID()

	switch kt {
	case lbFrontends:
		props[lbRules] = idRefs(refsTo(lb.Kids[lbRules], lbRules, lbID, propFrontendIP, self))
		props[lbNATRules] = idRefs(refsTo(lb.Kids[lbNATRules], lbNATRules, lbID, propFrontendIP, self))
		props[lbNATPools] = idRefs(refsTo(lb.Kids[lbNATPools], lbNATPools, lbID, propFrontendIP, self))
		props[lbOutbound] = idRefs(refsTo(lb.Kids[lbOutbound], lbOutbound, lbID, "frontendIPConfiguration", self))
	case lbPools:
		props["backendIPConfigurations"] = idRefs(p.poolMembers(self))
		props[lbRules] = idRefs(refsTo(lb.Kids[lbRules], lbRules, lbID, propBackendPl, self))
		props[lbNATRules] = idRefs(refsTo(lb.Kids[lbNATRules], lbNATRules, lbID, propBackendPl, self))
		props[lbOutbound] = idRefs(refsTo(lb.Kids[lbOutbound], lbOutbound, lbID, propBackendPl, self))

		if _, ok := props["loadBalancerBackendAddresses"]; !ok {
			props["loadBalancerBackendAddresses"] = []any{}
		}
	case lbProbes:
		props[lbRules] = idRefs(refsTo(lb.Kids[lbRules], lbRules, lbID, "probe", self))
	case lbNATRules:
		if m := p.natMembers(self); len(m) > 0 {
			props["backendIPConfiguration"] = idRef(m[0])
		}
	}

	props[fieldProvisioningState] = netStatusSucceeded

	out := map[string]any{
		netFieldID: self, fieldName: k.Name, fieldType: lbTypePrefix + kt, netFieldEtag: k.Etag,
		fieldProperties: props,
	}

	if z, ok := k.Extra["zones"]; ok {
		out[netFieldZones] = z
	}

	return out
}

// --- NIC membership ---

type ipCfgRef struct {
	props map[string]any
	nic   *netParent
	id    string
}

func (p *NetworkProvider) allIPConfigs() []ipCfgRef {
	var out []ipCfgRef

	for _, n := range p.ofType(netNICs) {
		for _, c := range anyList(n.Obj.Props[netIPConfigs]) {
			m, _ := c.(map[string]any)
			out = append(out, ipCfgRef{
				id: n.armID() + "/" + netIPConfigs + "/" + strOf(m, fieldName), props: propsOf(m), nic: n,
			})
		}
	}

	return out
}

func hasRef(props map[string]any, key, target string) bool {
	for _, v := range anyList(props[key]) {
		if strings.EqualFold(refIDOf(v), target) {
			return true
		}
	}

	return false
}

func (p *NetworkProvider) poolMembers(poolID string) []string {
	var out []string

	for _, c := range p.allIPConfigs() {
		if hasRef(c.props, propNICPools, poolID) {
			out = append(out, c.id)
		}
	}

	return out
}

func (p *NetworkProvider) natMembers(ruleID string) []string {
	var out []string

	for _, c := range p.allIPConfigs() {
		if hasRef(c.props, propNICNATs, ruleID) {
			out = append(out, c.id)
		}
	}

	return out
}

// lbKidByID resolves a reference to a load balancer child.
func (p *NetworkProvider) lbKidByID(idStr, kt string) (*netObj, bool) {
	rid, err := ParseGenericResourcePath(idStr)
	if err != nil || len(rid.Types) != 2 || !strings.EqualFold(rid.Types[0], netLBs) ||
		!strings.EqualFold(rid.Types[1], kt) || !strings.EqualFold(rid.Namespace, namespaceMicrosoftNetwork) {
		return nil, false
	}

	lb, ok := p.top(netLBs, rid.ResourceGroup, rid.Names[0])
	if !ok {
		return nil, false
	}

	k, idx := lb.kid(kt, rid.Names[1])

	return k, idx >= 0
}

// lbsOfNIC lists the load balancers a network interface is a backend member of.
func (p *NetworkProvider) lbsOfNIC(nic *netParent) []*netParent {
	seen := map[string]bool{}

	var out []*netParent

	for _, c := range anyList(nic.Obj.Props[netIPConfigs]) {
		m, _ := c.(map[string]any)

		for _, key := range []string{propNICPools, propNICNATs} {
			for _, v := range anyList(propsOf(m)[key]) {
				rid, err := ParseGenericResourcePath(refIDOf(v))
				if err != nil || len(rid.Names) == 0 {
					continue
				}

				if lb, ok := p.top(netLBs, rid.ResourceGroup, rid.Names[0]); ok && !seen[lb.armID()] {
					seen[lb.armID()] = true
					out = append(out, lb)
				}
			}
		}
	}

	return out
}

func (p *NetworkProvider) nicsOfLB(lb *netParent) []*netParent {
	var out []*netParent

	for _, n := range p.ofType(netNICs) {
		if slices.ContainsFunc(p.lbsOfNIC(n), func(l *netParent) bool { return l == lb }) {
			out = append(out, n)
		}
	}

	return out
}

// --- child PUT / DELETE ---

func (p *NetworkProvider) writeLBKid(lb *netParent, kt, name string, body map[string]any) (bool, error) {
	c := &lbCtx{id: lb.armID(), loc: lb.Obj.Location, taken: p.usedAddrsSkip("", lb.armID())}

	sku, _ := lb.Obj.Extra[netFieldSKU].(map[string]any)
	c.sku = strOf(sku, fieldName)

	prev, idx := lb.kid(kt, name)

	o, err := p.buildLBKid(c, kt, name, body, prev)
	if err != nil {
		return false, err
	}

	kids := map[string][]*netObj{}
	for k, v := range lb.Kids {
		kids[k] = slices.Clone(v)
	}

	if idx >= 0 {
		kids[kt][idx] = o
	} else {
		kids[kt] = append(kids[kt], o)
	}

	if err = p.validateLB(c, kids); err != nil {
		return false, err
	}

	lb.Kids = kids
	p.touch(lb)

	return idx < 0, nil
}

func (p *NetworkProvider) checkLBKidDelete(lb *netParent, kt string, id ResourceID) error {
	self := lb.armID() + "/" + kt + "/" + id.Names[1]

	var users []string

	switch kt {
	case lbPools:
		users = append(refsTo(lb.Kids[lbRules], lbRules, lb.armID(), propBackendPl, self),
			refsTo(lb.Kids[lbOutbound], lbOutbound, lb.armID(), propBackendPl, self)...)
	case lbFrontends:
		for _, k := range []string{lbRules, lbNATRules, lbNATPools, lbOutbound} {
			users = append(users, refsTo(lb.Kids[k], k, lb.armID(), propFrontendIP, self)...)
		}
	case lbProbes:
		users = refsTo(lb.Kids[lbRules], lbRules, lb.armID(), "probe", self)
	}

	if len(users) > 0 {
		return netErr(http.StatusBadRequest, "InUseLoadBalancerChildCannotBeDeleted",
			"%s is in use by %s and cannot be deleted. Remove the reference first.", self, users[0])
	}

	return nil
}

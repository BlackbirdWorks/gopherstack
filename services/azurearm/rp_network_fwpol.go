package azurearm

import (
	"maps"
	"net/http"
	"slices"
	"strings"
)

const (
	netFWPols        = "firewallPolicies"
	fpRCGs           = "ruleCollectionGroups"
	fpTypePrefix     = "Microsoft.Network/firewallPolicies/"
	fpFilterRC       = "FirewallPolicyFilterRuleCollection"
	fpNatRC          = "FirewallPolicyNatRuleCollection"
	fpRuleNet        = "NetworkRule"
	fpRuleApp        = "ApplicationRule"
	fpRuleNat        = "NatRule"
	fpPropBase       = "basePolicy"
	fpPropRCs        = "ruleCollections"
	fpPropRules      = "rules"
	fpPropSrc        = "sourceAddresses"
	fpPropDst        = "destinationAddresses"
	fpPropDstPorts   = "destinationPorts"
	fpPropProtocols  = "protocols"
	fpPropIPProtos   = "ipProtocols"
	fpPropSrcGroups  = "sourceIpGroups"
	fpPropDstGroups  = "destinationIpGroups"
	fpPropTranslated = "translatedAddress"
)

// --- classic collections on a firewall ---

func (p *NetworkProvider) buildFWClassic(props map[string]any, kids map[string][]*netObj) error {
	for _, kt := range []string{fwAppRCs, fwNatRCs, fwNetRCs} {
		prios := map[int]bool{}

		for _, e := range anyList(props[kt]) {
			m, _ := e.(map[string]any)
			name := strOf(m, fieldName)

			if err := validNetName(name, netMaxName); err != nil {
				return err
			}

			if hasObj(kids[kt], name) {
				return netBad("Rule collection name %s is used more than once.", name)
			}

			cp := propsOf(m)

			pr, err := intIn(cp, netPropPriority, fwMinPrio, fwMaxPrio, -1)
			if err != nil {
				return err
			}

			if prios[pr] {
				return netBad("Rule collection priority %d is used more than once.", pr)
			}

			prios[pr] = true

			out, err := classicCollection(kt, name, cp)
			if err != nil {
				return err
			}

			kids[kt] = append(kids[kt], &netObj{Name: name, Etag: newEtag(), Props: out})
		}
	}

	return nil
}

func classicCollection(kt, name string, cp map[string]any) (map[string]any, error) {
	out := takeProps(map[string]any{fieldProperties: cp}, fieldProvisioningState)

	act, _ := out["action"].(map[string]any)
	allowed, kind := []string{ruleAllow, ruleDeny}, fpRuleNet

	switch kt {
	case fwNatRCs:
		allowed, kind = []string{"Dnat", "Snat"}, fpRuleNat
	case fwAppRCs:
		kind = fpRuleApp
	}

	typ, ok := canonEnum(strOf(act, fieldType), allowed)
	if !ok {
		return nil, netBad("Rule collection %s: action.type must be one of %s.", name, strings.Join(allowed, ", "))
	}

	out["action"] = map[string]any{fieldType: typ}

	rules := anyList(out[fpPropRules])
	seen := map[string]bool{}

	for _, r := range rules {
		rm, _ := r.(map[string]any)

		if n := strOf(rm, fieldName); n == "" || seen[strings.ToLower(n)] {
			return nil, netBad("Rule collection %s requires unique, non-empty rule names.", name)
		}

		seen[strings.ToLower(strOf(rm, fieldName))] = true

		if err := checkFWRule(kind, fpPropProtocols, rm, fwTierPrem); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// --- rule validation shared by classic collections and policy rule collection groups ---

func nonEmptyStrings(m map[string]any, key string) bool { return len(strList(m[key])) > 0 }

func checkFWRule(kind, protoKey string, r map[string]any, tier string) error {
	name := strOf(r, fieldName)

	if !nonEmptyStrings(r, fpPropSrc) && !nonEmptyStrings(r, fpPropSrcGroups) {
		return netBad("Rule %s requires sourceAddresses or sourceIpGroups.", name)
	}

	for _, key := range []string{fpPropSrc, fpPropDst} {
		for _, a := range strList(r[key]) {
			if !validFWAddress(a) {
				return netBad("Rule %s: %s is not a valid address in %s.", name, a, key)
			}
		}
	}

	switch kind {
	case fpRuleNet:
		return checkFWNetworkRule(name, protoKey, r)
	case fpRuleApp:
		return checkFWAppRule(name, r, tier)
	default:
		return checkFWNatRule(name, protoKey, r)
	}
}

func validFWAddress(a string) bool {
	return validAddressPrefix(a) || validIPGroupAddress(a)
}

func checkFWProtocols(name, key string, r map[string]any, allowed []string) error {
	list := strList(r[key])
	if len(list) == 0 {
		return netBad("Rule %s requires %s.", name, key)
	}

	for _, pr := range list {
		if _, ok := canonEnum(pr, allowed); !ok {
			return netBad("Rule %s: protocol %s must be one of %s.", name, pr, strings.Join(allowed, ", "))
		}
	}

	return nil
}

func checkFWPorts(name string, r map[string]any, exactly int) error {
	ports := strList(r[fpPropDstPorts])
	if len(ports) == 0 || (exactly > 0 && len(ports) != exactly) {
		return netBad("Rule %s requires destinationPorts.", name)
	}

	for _, pt := range ports {
		if !validPortRange(pt) {
			return netBad("Rule %s: %s is not a valid destination port or port range.", name, pt)
		}
	}

	return nil
}

func checkFWNetworkRule(name, protoKey string, r map[string]any) error {
	if err := checkFWProtocols(name, protoKey, r, fwNetProtos); err != nil {
		return err
	}

	if !nonEmptyStrings(r, fpPropDst) && !nonEmptyStrings(r, "destinationFqdns") &&
		!nonEmptyStrings(r, fpPropDstGroups) {
		return netBad("Rule %s requires destinationAddresses, destinationFqdns or destinationIpGroups.", name)
	}

	return checkFWPorts(name, r, 0)
}

func checkFWAppRule(name string, r map[string]any, tier string) error {
	protos := anyList(r[fpPropProtocols])
	if len(protos) == 0 {
		return netBad("Rule %s requires protocols.", name)
	}

	for _, e := range protos {
		m, _ := e.(map[string]any)
		if _, ok := canonEnum(strOf(m, "protocolType"), fwAppProtos); !ok {
			return netBad("Rule %s: protocolType must be Http, Https or Mssql.", name)
		}

		if _, err := intIn(m, "port", 0, fwMaxPort, -1); err != nil {
			return err
		}
	}

	if !nonEmptyStrings(r, "targetFqdns") && !nonEmptyStrings(r, "fqdnTags") && !nonEmptyStrings(r, "targetUrls") &&
		!nonEmptyStrings(r, "webCategories") && !nonEmptyStrings(r, fpPropDst) {
		return netBad(
			"Rule %s requires targetFqdns, fqdnTags, targetUrls, webCategories or destinationAddresses.",
			name,
		)
	}

	if tier != fwTierPrem && (nonEmptyStrings(r, "targetUrls") || r["terminateTLS"] == true) {
		return netBad("Rule %s: URL filtering and TLS termination require a Premium firewall policy.", name)
	}

	if tier == fwTierBasic && nonEmptyStrings(r, "webCategories") {
		return netBad("Rule %s: web categories require a Standard or Premium firewall policy.", name)
	}

	return nil
}

func checkFWNatRule(name, protoKey string, r map[string]any) error {
	if err := checkFWProtocols(name, protoKey, r, fwNatProtos); err != nil {
		return err
	}

	if len(strList(r[fpPropDst])) != 1 {
		return netBad("NAT rule %s requires exactly one destinationAddresses entry.", name)
	}

	if err := checkFWPorts(name, r, 1); err != nil {
		return err
	}

	if (strOf(r, fpPropTranslated) == "") == (strOf(r, "translatedFqdn") == "") {
		return netBad("NAT rule %s requires exactly one of translatedAddress or translatedFqdn.", name)
	}

	if tp := strOf(r, "translatedPort"); tp == "" || tp == ruleWildcard || !validPortRange(tp) ||
		strings.Contains(tp, "-") {
		return netBad("NAT rule %s requires a single translatedPort.", name)
	}

	return nil
}

// --- firewall policies ---

var fwTIOrder = []string{fwTIOff, "Alert", ruleDeny} //nolint:gochecknoglobals // read-only table, least to most strict

func (p *NetworkProvider) putFWPol(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)
	self := id.ARMID()

	sku, _ := props[agwPropSKU].(map[string]any)

	tier, ok := canonEnum(firstNonEmpty(strOf(sku, netFieldTier), fwTierStd), fwTiers)
	if !ok {
		return nil, false, netBad("Firewall policy SKU tier must be Standard, Premium or Basic.")
	}

	old, existed := p.top(netFWPols, id.ResourceGroup, id.Names[0])
	loc := normLocation(strOf(body, fieldLocation))

	if loc == "" && existed {
		loc = old.Obj.Location
	}

	if tier != fwTierPrem && (props["intrusionDetection"] != nil || props["transportSecurity"] != nil) {
		return nil, false, netBad("IDPS and TLS inspection require a Premium firewall policy.")
	}

	if existed && tier != policyTier(old) {
		for _, k := range old.Kids[fpRCGs] {
			if err := recheckRCG(tier, k); err != nil {
				return nil, false, err
			}
		}
	}

	ti, err := p.checkPolicyHierarchy(self, loc, props)
	if err != nil {
		return nil, false, err
	}

	it, obj, created := p.upsertTop(netFWPols, id, body)
	obj.Props = takeProps(
		body,
		fieldProvisioningState,
		"childPolicies",
		"firewalls",
		fpRCGs,
		"size",
		fpPropBase,
		agwPropSKU,
		fwPropTI,
	)
	obj.Props[agwPropSKU] = map[string]any{netFieldTier: tier}

	if ti != "" {
		obj.Props[fwPropTI] = ti
	}

	if b := subID(props, fpPropBase); b != "" {
		obj.Props[fpPropBase] = idRef(b)
	}

	return p.bodyFWPol(id, it), created, nil
}

func recheckRCG(tier string, k *netObj) error {
	for _, c := range anyList(k.Props[fpPropRCs]) {
		cm, _ := c.(map[string]any)

		for _, r := range anyList(cm[fpPropRules]) {
			rm, _ := r.(map[string]any)

			if strOf(rm, "ruleType") == fpRuleApp {
				if err := checkFWAppRule(strOf(rm, fieldName), rm, tier); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// checkPolicyHierarchy validates basePolicy (exists, same region, acyclic) and threat intel strictness.
func (p *NetworkProvider) checkPolicyHierarchy(self, loc string, props map[string]any) (string, error) {
	ti := strOf(props, fwPropTI)
	if ti != "" {
		canon, ok := canonEnum(ti, fwTIModes)
		if !ok {
			return "", netBad("threatIntelMode must be Alert, Deny or Off.")
		}

		ti = canon
	}

	base := subID(props, fpPropBase)
	if base == "" {
		return ti, nil
	}

	if strings.EqualFold(base, self) {
		return "", netBad("A firewall policy cannot be its own base policy.")
	}

	bp, ok := p.netRef(base, netFWPols)
	if !ok {
		return "", refMissing(base)
	}

	if loc != "" && bp.Obj.Location != loc {
		return "", netErr(http.StatusBadRequest, "FirewallPolicyBasePolicyRegionMismatch",
			"Base policy %s and the child policy must reside in the same region.", base)
	}

	for cur, hops := bp, 0; cur != nil && hops <= maxPolicyDepth; hops++ {
		if strings.EqualFold(cur.armID(), self) {
			return "", netBad("Base policy %s would create a cycle in the policy hierarchy.", base)
		}

		next := subID(cur.Obj.Props, fpPropBase)
		if next == "" {
			break
		}

		cur, _ = p.netRef(next, netFWPols)
	}

	parent := strOf(bp.Obj.Props, fwPropTI)
	if ti != "" && parent != "" && slices.Index(fwTIOrder, ti) < slices.Index(fwTIOrder, parent) {
		return "", netBad("threatIntelMode %s is less strict than the base policy mode %s.", ti, parent)
	}

	return ti, nil
}

const maxPolicyDepth = 64

func (p *NetworkProvider) bodyFWPol(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	self := it.armID()

	var kids, fws []string

	for _, o := range p.ofType(netFWPols) {
		if strings.EqualFold(subID(o.Obj.Props, fpPropBase), self) {
			kids = append(kids, o.armID())
		}
	}

	for _, fw := range p.ofType(netFWs) {
		if strings.EqualFold(subID(fw.Obj.Props, fwPropPolicy), self) {
			fws = append(fws, fw.armID())
		}
	}

	groups := make([]string, 0, len(it.Kids[fpRCGs]))
	for _, g := range it.Kids[fpRCGs] {
		groups = append(groups, self+"/"+fpRCGs+"/"+g.Name)
	}

	props["childPolicies"], props["firewalls"], props[fpRCGs] = idRefs(kids), idRefs(fws), idRefs(groups)
	props[fieldProvisioningState] = netStatusSucceeded

	return p.envelope(id, netFWPols, &it.Obj, props)
}

func (p *NetworkProvider) checkFWPolDelete(it *netParent) error {
	self := it.armID()

	for _, fw := range p.ofType(netFWs) {
		if strings.EqualFold(subID(fw.Obj.Props, fwPropPolicy), self) {
			return netErr(http.StatusBadRequest, "FirewallPolicyInUse",
				"Firewall policy %s is associated with Azure Firewall %s and cannot be deleted.", self, fw.armID())
		}
	}

	for _, o := range p.ofType(netFWPols) {
		if strings.EqualFold(subID(o.Obj.Props, fpPropBase), self) {
			return netErr(http.StatusBadRequest, "FirewallPolicyInUse",
				"Firewall policy %s is the base policy of %s and cannot be deleted.", self, o.armID())
		}
	}

	return nil
}

// --- rule collection groups ---

func (p *NetworkProvider) writeRCG(pol *netParent, name string, body map[string]any) (bool, error) {
	props := propsOf(body)

	prio, err := intIn(props, netPropPriority, fwMinPrio, fwMaxPrio, -1)
	if err != nil {
		return false, err
	}

	for _, g := range pol.Kids[fpRCGs] {
		if !strings.EqualFold(g.Name, name) && intOf(g.Props[netPropPriority]) == prio {
			return false, netBad("Rule collection group priority %d is already used by %s.", prio, g.Name)
		}
	}

	cols, err := normRuleCollections(policyTier(pol), anyList(props[fpPropRCs]))
	if err != nil {
		return false, err
	}

	if err = p.checkIPGroupRefs(cols); err != nil {
		return false, err
	}

	prev, idx := pol.kid(fpRCGs, name)
	o := &netObj{Name: name, Etag: newEtag(), Props: map[string]any{netPropPriority: prio, fpPropRCs: cols}}

	if prev != nil {
		o.GUID = prev.GUID
	}

	if idx >= 0 {
		pol.Kids[fpRCGs][idx] = o
	} else {
		pol.Kids[fpRCGs] = append(pol.Kids[fpRCGs], o)
	}

	p.touch(pol)

	return idx < 0, nil
}

func normRuleCollections(tier string, raw []any) ([]any, error) {
	out := make([]any, 0, len(raw))
	names, prios := map[string]bool{}, map[int]bool{}

	for _, e := range raw {
		m, _ := e.(map[string]any)
		name := strOf(m, fieldName)

		if err := validNetName(name, netMaxName); err != nil {
			return nil, err
		}

		if names[strings.ToLower(name)] {
			return nil, netBad("Rule collection name %s is used more than once.", name)
		}

		names[strings.ToLower(name)] = true

		pr, err := intIn(m, netPropPriority, fwMinPrio, fwMaxPrio, -1)
		if err != nil {
			return nil, err
		}

		if prios[pr] {
			return nil, netBad("Rule collection priority %d is used more than once.", pr)
		}

		prios[pr] = true

		col, err := normRuleCollection(tier, name, m)
		if err != nil {
			return nil, err
		}

		out = append(out, col)
	}

	return out, nil
}

func normRuleCollection(tier, name string, m map[string]any) (map[string]any, error) {
	out := map[string]any{}

	maps.Copy(out, m)

	act, _ := m["action"].(map[string]any)

	var kinds []string

	switch strOf(m, "ruleCollectionType") {
	case fpFilterRC:
		if typ, ok := canonEnum(strOf(act, fieldType), []string{ruleAllow, ruleDeny}); ok {
			out["action"] = map[string]any{fieldType: typ}
		} else {
			return nil, netBad("Rule collection %s: action.type must be Allow or Deny.", name)
		}

		kinds = []string{fpRuleNet, fpRuleApp}
	case fpNatRC:
		if typ, ok := canonEnum(strOf(act, fieldType), []string{"DNAT"}); ok {
			out["action"] = map[string]any{fieldType: typ}
		} else {
			return nil, netBad("Rule collection %s: action.type must be DNAT.", name)
		}

		kinds = []string{fpRuleNat}
	default:
		return nil, netBad("Rule collection %s: ruleCollectionType must be %s or %s.", name, fpFilterRC, fpNatRC)
	}

	rules := anyList(m[fpPropRules])
	if len(rules) == 0 {
		return nil, netBad("Rule collection %s requires at least one rule.", name)
	}

	first, seen := "", map[string]bool{}

	for _, r := range rules {
		rm, _ := r.(map[string]any)
		kind := strOf(rm, "ruleType")

		if !containsFold(kinds, kind) {
			return nil, netBad(
				"Rule collection %s: ruleType %s is not valid in a %s.",
				name,
				kind,
				strOf(m, "ruleCollectionType"),
			)
		}

		if first == "" {
			first = kind
		} else if kind != first {
			return nil, netBad("Rule collection %s: all rules in a rule collection must be of the same type.", name)
		}

		rn := strOf(rm, fieldName)
		if rn == "" || seen[strings.ToLower(rn)] {
			return nil, netBad("Rule collection %s requires unique, non-empty rule names.", name)
		}

		seen[strings.ToLower(rn)] = true

		if err := checkFWRule(kind, fpPropIPProtos, rm, tier); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (p *NetworkProvider) bodyRCG(pol *netParent, g *netObj) map[string]any {
	props := map[string]any{}

	maps.Copy(props, g.Props)

	props[fieldProvisioningState] = netStatusSucceeded

	return map[string]any{
		netFieldID: pol.armID() + "/" + fpRCGs + "/" + g.Name, fieldName: g.Name,
		fieldType: fpTypePrefix + fpRCGs, netFieldEtag: g.Etag, fieldProperties: props,
	}
}

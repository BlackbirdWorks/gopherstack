package azurearm

import (
	"maps"
	"regexp"

	"github.com/google/uuid"
)

const (
	netRFs       = "routeFilters"
	rfRules      = "routeFilterRules"
	rfRuleType   = "Microsoft.Network/routeFilters/routeFilterRules"
	rfPropRules  = "rules"
	rfPropAccess = "access"
	rfPropType   = "routeFilterRuleType"
	rfPropComms  = "communities"
	rfRuleKind   = "Community"
)

var rfCommunityRe = regexp.MustCompile(`^\d{1,5}:\d{1,5}$`)

func normalizeRFRule(name string, props map[string]any, prev *netObj) (*netObj, error) {
	out := takeProps(map[string]any{fieldProperties: props}, fieldProvisioningState, "location")

	access, ok := canonEnum(strOf(out, rfPropAccess), []string{ruleAllow, "Deny"})
	if !ok {
		return nil, netBad("Route filter rule %s requires access to be Allow or Deny.", name)
	}

	out[rfPropAccess] = access

	kind, ok := canonEnum(strOf(out, rfPropType), []string{rfRuleKind})
	if !ok {
		return nil, netBad("Route filter rule %s requires routeFilterRuleType Community.", name)
	}

	out[rfPropType] = kind

	comms := strList(out[rfPropComms])
	if len(comms) == 0 {
		return nil, netBad("Route filter rule %s requires at least one BGP community.", name)
	}

	for _, c := range comms {
		if !rfCommunityRe.MatchString(c) {
			return nil, netBad("Route filter rule %s: '%s' is not a BGP community of the form ASN:value.", name, c)
		}
	}

	o := &netObj{Name: name, Etag: newEtag(), GUID: uuid.NewString(), Props: out}
	if prev != nil {
		o.GUID = prev.GUID
	}

	return o, nil
}

func (p *NetworkProvider) putRF(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	old, _ := p.top(netRFs, id.ResourceGroup, id.Names[0])

	rules, err := p.inlineKids(body, rfPropRules, rfRules, old, normalizeRFRule)
	if err != nil {
		return nil, false, err
	}

	it, _, created := p.upsertTop(netRFs, id, body)
	it.Obj.Props = takeProps(body, rfPropRules, "peerings", "ipv6Peerings", fieldProvisioningState)

	if rules != nil {
		it.Kids[rfRules] = rules
	}

	return p.bodyRF(id, it), created, nil
}

func (p *NetworkProvider) bodyRF(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	rules := make([]any, 0, len(it.Kids[rfRules]))
	for _, r := range it.Kids[rfRules] {
		rules = append(rules, p.bodyRFRule(it, kidID(id, rfRules, r.Name), r))
	}

	props[rfPropRules] = rules
	props[fieldProvisioningState] = netStatusSucceeded

	return p.envelope(id, netRFs, &it.Obj, props)
}

func (p *NetworkProvider) bodyRFRule(parent *netParent, id ResourceID, r *netObj) map[string]any {
	props := maps.Clone(r.Props)
	props[fieldProvisioningState] = netStatusSucceeded

	return map[string]any{
		netFieldID: id.ARMID(), fieldName: r.Name, fieldType: rfRuleType, netFieldEtag: r.Etag,
		fieldLocation: parent.Obj.Location, fieldProperties: props,
	}
}

func (p *NetworkProvider) writeRFRule(rf *netParent, name string, body map[string]any) (bool, error) {
	prev, idx := rf.kid(rfRules, name)

	r, err := normalizeRFRule(name, propsOf(body), prev)
	if err != nil {
		return false, err
	}

	if idx >= 0 {
		rf.Kids[rfRules][idx] = r
	} else {
		rf.Kids[rfRules] = append(rf.Kids[rfRules], r)
	}

	p.touch(rf)

	return idx < 0, nil
}

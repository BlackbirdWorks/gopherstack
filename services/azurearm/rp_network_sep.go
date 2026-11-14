package azurearm

import (
	"maps"
	"strings"

	"github.com/google/uuid"
)

const (
	netSEPs      = "serviceEndpointPolicies"
	sepDefs      = "serviceEndpointPolicyDefinitions"
	sepDefType   = "Microsoft.Network/serviceEndpointPolicies/serviceEndpointPolicyDefinitions"
	sepPropSvc   = "service"
	sepPropRes   = "serviceResources"
	propSubnetSE = "serviceEndpointPolicies"
)

func normalizeSEPDef(name string, props map[string]any, prev *netObj) (*netObj, error) {
	out := takeProps(map[string]any{fieldProperties: props}, fieldProvisioningState)

	svc := strOf(out, sepPropSvc)
	if svc == "" {
		return nil, netBad("Service endpoint policy definition %s requires a service.", name)
	}

	res := strList(out[sepPropRes])
	for _, r := range res {
		if !strings.HasPrefix(strings.ToLower(r), "/subscriptions/") {
			return nil, netBad("Service endpoint policy definition %s: service resource '%s' must be a subscription, "+
				"resource group or resource id.", name, r)
		}
	}

	o := &netObj{Name: name, Etag: newEtag(), GUID: uuid.NewString(), Props: out}
	if prev != nil {
		o.GUID = prev.GUID
	}

	return o, nil
}

func (p *NetworkProvider) putSEP(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	old, _ := p.top(netSEPs, id.ResourceGroup, id.Names[0])

	defs, err := p.inlineKids(body, sepDefs, sepDefs, old, normalizeSEPDef)
	if err != nil {
		return nil, false, err
	}

	it, _, created := p.upsertTop(netSEPs, id, body)
	it.Obj.Props = takeProps(body, sepDefs, "subnets", fieldProvisioningState, netFieldResourceGUID)

	if defs != nil {
		it.Kids[sepDefs] = defs
	}

	return p.bodySEP(id, it), created, nil
}

func (p *NetworkProvider) sepSubnets(sepID string) []string {
	var out []string

	for _, v := range p.ofType(netVNets) {
		for _, s := range v.Kids[netSubnets] {
			if hasRef(s.Props, propSubnetSE, sepID) {
				out = append(out, subnetIDOf(v, s.Name))
			}
		}
	}

	return out
}

func (p *NetworkProvider) bodySEP(id ResourceID, it *netParent) map[string]any {
	props := map[string]any{}

	maps.Copy(props, it.Obj.Props)

	defs := make([]any, 0, len(it.Kids[sepDefs]))
	for _, d := range it.Kids[sepDefs] {
		defs = append(defs, p.bodySEPDef(kidID(id, sepDefs, d.Name), d))
	}

	props[sepDefs] = defs
	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID

	if subs := p.sepSubnets(it.armID()); len(subs) > 0 {
		props["subnets"] = idRefs(subs)
	}

	return p.envelope(id, netSEPs, &it.Obj, props)
}

func (p *NetworkProvider) bodySEPDef(id ResourceID, d *netObj) map[string]any {
	props := maps.Clone(d.Props)
	props[fieldProvisioningState] = netStatusSucceeded

	return map[string]any{
		netFieldID: id.ARMID(), fieldName: d.Name, fieldType: sepDefType, netFieldEtag: d.Etag,
		fieldProperties: props,
	}
}

func (p *NetworkProvider) writeSEPDef(sep *netParent, name string, body map[string]any) (bool, error) {
	prev, idx := sep.kid(sepDefs, name)

	d, err := normalizeSEPDef(name, propsOf(body), prev)
	if err != nil {
		return false, err
	}

	if idx >= 0 {
		sep.Kids[sepDefs][idx] = d
	} else {
		sep.Kids[sepDefs] = append(sep.Kids[sepDefs], d)
	}

	p.touch(sep)

	return idx < 0, nil
}

// checkSubnetSEPs validates the policies a subnet names and normalises them to id references.
func (p *NetworkProvider) checkSubnetSEPs(props map[string]any) error {
	raw, has := props[propSubnetSE]
	if !has {
		return nil
	}

	out := []any{}

	for _, v := range anyList(raw) {
		sid := refIDOf(v)
		if _, ok := p.netRef(sid, netSEPs); !ok {
			return refMissing(sid)
		}

		out = append(out, idRef(sid))
	}

	props[propSubnetSE] = out

	return nil
}

package azurearm

import (
	"maps"
	"strings"

	"github.com/google/uuid"
)

const (
	netTaps        = "virtualNetworkTaps"
	nicTapCfgs     = "tapConfigurations"
	tapCfgType     = "Microsoft.Network/networkInterfaces/tapConfigurations"
	tapPropNIC     = "destinationNetworkInterfaceIPConfiguration"
	tapPropLB      = "destinationLoadBalancerFrontEndIPConfiguration"
	tapPropPort    = "destinationPort"
	tapPropTapRef  = "virtualNetworkTap"
	tapMaxPort     = 65535
	tapPropBackref = "networkInterfaceTapConfigurations"
)

func (p *NetworkProvider) putTap(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	props := propsOf(body)
	nicRef := subID(props, tapPropNIC)
	lbRef := subID(props, tapPropLB)

	if (nicRef == "") == (lbRef == "") {
		return nil, false, netBad("Virtual network tap %s requires exactly one of %s and %s.", id.Names[0],
			tapPropNIC, tapPropLB)
	}

	if nicRef != "" && !p.ipConfigExists(nicRef) || lbRef != "" && !p.lbFrontendExists(lbRef) {
		return nil, false, refMissing(firstNonEmpty(nicRef, lbRef))
	}

	if port, set := props[tapPropPort].(float64); set && (port < 1 || port > tapMaxPort || port != float64(int(port))) {
		return nil, false, netBad("destinationPort must be between 1 and %d.", tapMaxPort)
	}

	it, _, created := p.upsertTop(netTaps, id, body)
	it.Obj.Props = takeProps(body, tapPropBackref, fieldProvisioningState, netFieldResourceGUID)

	for _, k := range []string{tapPropNIC, tapPropLB} {
		if ref := subID(props, k); ref != "" {
			it.Obj.Props[k] = idRef(ref)
		}
	}

	return p.bodyTap(id, it), created, nil
}

func (p *NetworkProvider) ipConfigExists(cfgID string) bool {
	for _, c := range p.allIPConfigs() {
		if strings.EqualFold(c.id, cfgID) {
			return true
		}
	}

	return false
}

func (p *NetworkProvider) lbFrontendExists(fid string) bool {
	_, ok := p.lbKidByID(fid, lbFrontends)

	return ok
}

// tapConfigsOf lists the NIC tap configurations that point at the tap.
func (p *NetworkProvider) tapConfigsOf(tapID string) []string {
	var out []string

	for _, n := range p.ofType(netNICs) {
		for _, k := range n.Kids[nicTapCfgs] {
			if strings.EqualFold(subID(k.Props, tapPropTapRef), tapID) {
				out = append(out, n.armID()+"/"+nicTapCfgs+"/"+k.Name)
			}
		}
	}

	return out
}

func (p *NetworkProvider) bodyTap(id ResourceID, it *netParent) map[string]any {
	props := maps.Clone(it.Obj.Props)
	props[fieldProvisioningState] = netStatusSucceeded
	props[netFieldResourceGUID] = it.Obj.GUID
	props[tapPropBackref] = idRefs(p.tapConfigsOf(it.armID()))

	return p.envelope(id, netTaps, &it.Obj, props)
}

func (p *NetworkProvider) writeTapCfg(nic *netParent, name string, body map[string]any) (bool, error) {
	tid := subID(propsOf(body), tapPropTapRef)
	if tid == "" {
		return false, netBad("Tap configuration %s requires virtualNetworkTap.", name)
	}

	if _, ok := p.netRef(tid, netTaps); !ok {
		return false, refMissing(tid)
	}

	prev, idx := nic.kid(nicTapCfgs, name)
	o := &netObj{
		Name: name, Etag: newEtag(), GUID: uuid.NewString(), Props: map[string]any{tapPropTapRef: idRef(tid)},
	}

	if prev != nil {
		o.GUID = prev.GUID
		nic.Kids[nicTapCfgs][idx] = o
	} else {
		nic.Kids[nicTapCfgs] = append(nic.Kids[nicTapCfgs], o)
	}

	p.touch(nic)

	return prev == nil, nil
}

func (p *NetworkProvider) bodyTapCfg(id ResourceID, k *netObj) map[string]any {
	return map[string]any{
		netFieldID: id.ARMID(), fieldName: k.Name, fieldType: tapCfgType, netFieldEtag: k.Etag,
		fieldProperties: map[string]any{
			tapPropTapRef: maps.Clone(propsMap(k.Props[tapPropTapRef])), fieldProvisioningState: netStatusSucceeded,
		},
	}
}

// nicTapList is the tapConfigurations array of a network interface body.
func (p *NetworkProvider) nicTapList(it *netParent) []any {
	out := make([]any, 0, len(it.Kids[nicTapCfgs]))

	for _, k := range it.Kids[nicTapCfgs] {
		out = append(out, p.bodyTapCfg(kidID(it.id(it.Sub), nicTapCfgs, k.Name), k))
	}

	return out
}

package azurearm

import (
	"net/http"
	"strings"
)

const (
	netDDoS         = "ddosProtectionPlans"
	propDDoSPlan    = "ddosProtectionPlan"
	propDDoSEnabled = "enableDdosProtection"
	propDDoSVNets   = "virtualNetworks"
	propDDoSPIPs    = "publicIPAddresses"
	propDDoSSetting = "ddosSettings"
)

func (p *NetworkProvider) putDDoS(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	if _, existed := p.top(netDDoS, id.ResourceGroup, id.Names[0]); !existed &&
		normLocation(strOf(body, fieldLocation)) == "" {
		return nil, false, netErr(http.StatusBadRequest, netCodeLocation,
			"The location property is required for this definition.")
	}

	it, obj, created := p.upsertTop(netDDoS, id, body)
	obj.Props = map[string]any{}

	return p.bodyDDoS(id, it), created, nil
}

func (p *NetworkProvider) bodyDDoS(id ResourceID, it *netParent) map[string]any {
	self := it.armID()
	vnets := []any{}

	for _, v := range p.ofType(netVNets) {
		if strings.EqualFold(subID(v.Obj.Props, propDDoSPlan), self) {
			vnets = append(vnets, map[string]any{netFieldID: v.armID()})
		}
	}

	pips := []any{}

	for _, ip := range p.ofType(netPIPs) {
		if strings.EqualFold(subID(mapOf(ip.Obj.Props[propDDoSSetting]), propDDoSPlan), self) {
			pips = append(pips, map[string]any{netFieldID: ip.armID()})
		}
	}

	props := map[string]any{
		fieldProvisioningState: netStatusSucceeded, netFieldResourceGUID: it.Obj.GUID,
		propDDoSVNets: vnets, propDDoSPIPs: pips,
	}

	return p.envelope(id, netDDoS, &it.Obj, props)
}

// checkDDoSRef verifies the plan a virtual network or public IP address names exists.
func (p *NetworkProvider) checkDDoSRef(planID string) error {
	if planID == "" {
		return nil
	}

	if _, ok := p.netRef(planID, netDDoS); !ok {
		return refMissing(planID)
	}

	return nil
}

func (p *NetworkProvider) checkDDoSZoneDelete(it *netParent) error {
	if it.Type == netZones {
		if n := len(it.Kids[netLinks]); n > 0 {
			return netErr(http.StatusConflict, dnsCodeConflict,
				"The private DNS zone %s cannot be deleted because it still has %d virtual network link(s); remove "+
					"them first.", it.Obj.Name, n)
		}

		return nil
	}

	self := it.armID()

	for _, v := range p.ofType(netVNets) {
		if strings.EqualFold(subID(v.Obj.Props, propDDoSPlan), self) {
			return netErr(http.StatusBadRequest, "InUseDdosProtectionPlanCannotBeDeleted",
				"DDoS protection plan %s is associated with virtual network %s and cannot be deleted. Remove the "+
					"association first.", self, v.armID())
		}
	}

	for _, ip := range p.ofType(netPIPs) {
		if strings.EqualFold(subID(mapOf(ip.Obj.Props[propDDoSSetting]), propDDoSPlan), self) {
			return netErr(http.StatusBadRequest, "InUseDdosProtectionPlanCannotBeDeleted",
				"DDoS protection plan %s is associated with public IP address %s and cannot be deleted. Remove the "+
					"association first.", self, ip.armID())
		}
	}

	return nil
}

// checkPIPDDoS validates a public IP address's ddosSettings: the mode and, only in Enabled mode, a plan.
func (p *NetworkProvider) checkPIPDDoS(settings map[string]any) error {
	if settings == nil {
		return nil
	}

	mode := strOf(settings, "protectionMode")
	if mode != "" {
		if _, ok := canonEnum(mode, []string{"VirtualNetworkInherited", flexEnabled, apimDisabled}); !ok {
			return netBad("The ddosSettings.protectionMode '%s' is not valid; allowed values are "+
				"VirtualNetworkInherited, Enabled and Disabled.", mode)
		}
	}

	planID := subID(settings, propDDoSPlan)
	if planID != "" && !strings.EqualFold(mode, flexEnabled) {
		return netBad("A ddosProtectionPlan can only be set on a public IP address whose protectionMode is Enabled.")
	}

	return p.checkDDoSRef(planID)
}

func (p *NetworkProvider) putPIPChecked(id ResourceID, body map[string]any) (map[string]any, bool, error) {
	if err := p.checkPIPDDoS(mapOf(propsOf(body)[propDDoSSetting])); err != nil {
		return nil, false, err
	}

	return p.putPIP(id, body)
}

func (p *NetworkProvider) putPlainTop(top string, id ResourceID, body map[string]any) (map[string]any, bool, error) {
	if top == netDDoS {
		return p.putDDoS(id, body)
	}

	return p.putASG(id, body)
}

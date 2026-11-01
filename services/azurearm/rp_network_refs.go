package azurearm

import (
	"net/http"
	"strings"
)

// checkNewTopDelete applies the in-use rules of the load balancer, NAT, prefix and security group types.
func (p *NetworkProvider) checkNewTopDelete(it *netParent) error {
	switch it.Type {
	case netNATs:
		if subs := p.natSubnets(it.armID()); len(subs) > 0 {
			return netErr(http.StatusBadRequest, "InUseNatGatewayCannotBeDeleted",
				"NAT gateway %s is in use by subnet %s and cannot be deleted. Remove the association first.",
				it.armID(), subs[0])
		}
	case netPrefixes:
		if pips := p.prefixPIPs(it.armID()); len(pips) > 0 {
			return netErr(http.StatusBadRequest, "PublicIPPrefixInUse",
				"Public IP prefix %s cannot be deleted because public IP address %s was created from it.",
				it.armID(), pips[0].armID())
		}

		if user := p.prefixHolder(it.armID()); user != "" {
			return netErr(http.StatusBadRequest, "PublicIPPrefixInUse",
				"Public IP prefix %s cannot be deleted because it is in use by %s.", it.armID(), user)
		}
	case netASGs:
		if users := p.asgUsers(it.armID()); len(users) > 0 {
			return netErr(http.StatusBadRequest, "InUseApplicationSecurityGroupCannotBeDeleted",
				"Application security group %s cannot be deleted because it is in use by %s.", it.armID(), users[0])
		}
	}

	return nil
}

// checkNICRefs validates the load balancer pools, inbound NAT rules and application security groups of a NIC.
func (p *NetworkProvider) checkNICRefs(nicID, loc string, cfgs []any) error {
	for _, c := range cfgs {
		m, _ := c.(map[string]any)
		cfgID := nicID + "/" + netIPConfigs + "/" + strOf(m, fieldName)
		cp := propsOf(m)

		for _, v := range anyList(cp[propNICPools]) {
			if _, ok := p.lbKidByID(refIDOf(v), lbPools); !ok {
				return refMissing(refIDOf(v))
			}
		}

		for _, v := range anyList(cp[propNICNATs]) {
			if err := p.checkNICNATRule(cfgID, refIDOf(v)); err != nil {
				return err
			}
		}
	}

	return p.checkNICASGs(nicID, loc, cfgs)
}

func (p *NetworkProvider) checkNICNATRule(cfgID, rid string) error {
	rule, ok := p.lbKidByID(rid, lbNATRules)
	if !ok {
		return refMissing(rid)
	}

	if _, ranged := rule.Props["frontendPortRangeStart"]; ranged {
		return netBad("Inbound NAT rule %s uses a frontend port range and attaches to a backend pool, "+
			"not to a network interface.", rid)
	}

	for _, other := range p.natMembers(rid) {
		if !strings.EqualFold(other, cfgID) {
			return netErr(http.StatusBadRequest, "InboundNatRuleInUse",
				"Inbound NAT rule %s is already assigned to IP configuration %s.", rid, other)
		}
	}

	return nil
}

// dropMissingListRefs removes pool, NAT rule and security group references whose target no longer exists.
func (p *NetworkProvider) dropMissingListRefs(cp map[string]any) {
	keep := func(key string, exists func(string) bool) {
		var out []any

		for _, v := range anyList(cp[key]) {
			if exists(refIDOf(v)) {
				out = append(out, v)
			}
		}

		if len(out) == 0 {
			delete(cp, key)

			return
		}

		cp[key] = out
	}

	keep(propNICPools, func(id string) bool {
		_, ok := p.lbKidByID(id, lbPools)

		return ok
	})
	keep(propNICNATs, func(id string) bool {
		_, ok := p.lbKidByID(id, lbNATRules)

		return ok
	})
	keep(propNICASGs, func(id string) bool {
		_, ok := p.netRef(id, netASGs)

		return ok
	})
}

// pruneNewTypesLocked drops references to resources that no longer exist (after a resource group delete).
func (p *NetworkProvider) pruneNewTypesLocked() {
	for _, lb := range p.ofType(netLBs) {
		for _, f := range lb.Kids[lbFrontends] {
			p.dropMissingRef(f.Props, "publicIPAddress", netPIPs)
			p.dropMissingRef(f.Props, propPrefixRef, netPrefixes)
		}
	}

	for _, n := range p.ofType(netNATs) {
		for _, key := range []string{natPropPIPs, natPropPIPsV6} {
			p.keepRefs(n.Obj.Props, key, netPIPs)
		}

		for _, key := range []string{natPropPrefixes, natPropPrefixesV6} {
			p.keepRefs(n.Obj.Props, key, netPrefixes)
		}
	}

	for _, nsg := range p.ofType(netNSGs) {
		for _, r := range nsg.Kids[netRules] {
			for _, key := range []string{propSrcASGs, propDestASGs} {
				p.keepRefs(r.Props, key, netASGs)
			}
		}
	}
}

func (p *NetworkProvider) keepRefs(props map[string]any, key, typ string) {
	var out []any

	for _, v := range anyList(props[key]) {
		if _, ok := p.netRef(refIDOf(v), typ); ok {
			out = append(out, v)
		}
	}

	if len(out) == 0 {
		delete(props, key)

		return
	}

	props[key] = out
}

func (p *NetworkProvider) listLBNICs(lb *netParent, id ResourceID) []map[string]any {
	nics := p.nicsOfLB(lb)
	out := make([]map[string]any, 0, len(nics))

	for _, n := range nics {
		out = append(out, p.bodyNIC(n.id(id.SubscriptionID), n))
	}

	return out
}

func (p *NetworkProvider) listNICLBs(nic *netParent, id ResourceID) []map[string]any {
	lbs := p.lbsOfNIC(nic)
	out := make([]map[string]any, 0, len(lbs))

	for _, lb := range lbs {
		out = append(out, p.bodyLB(lb.id(id.SubscriptionID), lb))
	}

	return out
}

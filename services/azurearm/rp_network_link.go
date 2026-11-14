package azurearm

import "net/http"

func (p *NetworkProvider) putLinkTop(top string, id ResourceID, body map[string]any) (map[string]any, bool, error) {
	switch top {
	case netPLS:
		return p.putPLS(id, body)
	case netRFs:
		return p.putRF(id, body)
	case netSEPs:
		return p.putSEP(id, body)
	case netWatchers:
		return p.putWatcher(id, body)
	default:
		return p.putTap(id, body)
	}
}

func (p *NetworkProvider) bodyLinkTop(id ResourceID, it *netParent) map[string]any {
	switch it.Type {
	case netPLS:
		return p.bodyPLS(id, it)
	case netRFs:
		return p.bodyRF(id, it)
	case netSEPs:
		return p.bodySEP(id, it)
	case netWatchers:
		return p.bodyWatcher(id, it)
	default:
		return p.bodyTap(id, it)
	}
}

// checkLinkTopDelete applies the in-use rules of the types that other resources reference.
func (p *NetworkProvider) checkLinkTopDelete(it *netParent) error {
	switch it.Type {
	case netSEPs:
		if subs := p.sepSubnets(it.armID()); len(subs) > 0 {
			return netErr(http.StatusBadRequest, "InUseServiceEndpointPolicyCannotBeDeleted",
				"Service endpoint policy %s is associated with subnet %s and cannot be deleted. Remove the "+
					"association first.", it.armID(), subs[0])
		}
	case netTaps:
		if cfgs := p.tapConfigsOf(it.armID()); len(cfgs) > 0 {
			return netErr(http.StatusBadRequest, "InUseVirtualNetworkTapCannotBeDeleted",
				"Virtual network tap %s is used by network interface tap configuration %s and cannot be deleted.",
				it.armID(), cfgs[0])
		}
	case netLBs:
		if users := p.plsUsingLB(it.armID()); len(users) > 0 {
			return netErr(http.StatusBadRequest, "InUseLoadBalancerCannotBeDeleted",
				"Load balancer %s is exposed by private link service %s and cannot be deleted.", it.armID(), users[0])
		}
	}

	return nil
}

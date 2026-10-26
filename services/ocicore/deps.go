package ocicore

import "slices"

const maxVcns = 50

func (b *InMemoryBackend) liveRefs(match func(resource) bool) []resource {
	var out []resource

	for _, r := range b.res {
		if live(r) && match(r) {
			out = append(out, r)
		}
	}

	return out
}

func (b *InMemoryBackend) isDefault(vcnID, id string) bool {
	v, ok := b.res[vcnID].(*Vcn)

	return ok && (v.DefaultRouteTableID == id || v.DefaultSecurityList == id || v.DefaultDhcpOptionsID == id)
}

func usedBy(what, id string, refs []resource) *APIError {
	if len(refs) == 0 {
		return nil
	}

	return errConflict("The " + what + " " + id + " cannot be deleted because it is in use by " +
		refs[0].base().ID)
}

func routeTarget(id string) func(resource) bool {
	return func(r resource) bool {
		rt, ok := r.(*RouteTable)

		return ok && slices.ContainsFunc(rt.RouteRules, func(x RouteRule) bool { return x.NetworkEntityID == id })
	}
}

// deleteCheck enforces the documented delete-in-use conflicts; callers hold the write lock.
func (b *InMemoryBackend) deleteCheck(r resource) *APIError {
	id := r.base().ID

	switch v := r.(type) {
	case *Vcn:
		refs := b.liveRefs(func(x resource) bool { return x.vcnRef() == id && !b.isDefault(id, x.base().ID) })
		if len(refs) > 0 {
			return errConflict("The Vcn " + id + " cannot be deleted because it still contains " +
				ocidKind(refs[0].base().ID) + " " + refs[0].base().ID)
		}
	case *RouteTable:
		if b.isDefault(v.VcnID, id) {
			return errConflict("The default route table of a VCN cannot be deleted")
		}

		return usedBy("route table", id, b.liveRefs(func(x resource) bool { return routeTableRef(x) == id }))
	case *SecurityList:
		if b.isDefault(v.VcnID, id) {
			return errConflict("The default security list of a VCN cannot be deleted")
		}

		return usedBy("security list", id, b.liveRefs(func(x resource) bool {
			s, ok := x.(*Subnet)

			return ok && slices.Contains(s.SecurityListIDs, id)
		}))
	case *DhcpOptions:
		if b.isDefault(v.VcnID, id) {
			return errConflict("The default DHCP options of a VCN cannot be deleted")
		}

		return usedBy("DHCP options", id, b.liveRefs(func(x resource) bool {
			s, ok := x.(*Subnet)

			return ok && s.DhcpOptionsID == id
		}))
	case *InternetGateway, *NatGateway, *ServiceGateway, *LocalPeeringGateway:
		return usedBy("gateway", id, b.liveRefs(routeTarget(id)))
	case *NetworkSecurityGroup:
		for _, rule := range b.nsgRules {
			if rule.NsgID != id && (rule.Source == id || rule.Destination == id) && live(b.res[rule.NsgID]) {
				return errConflict("The network security group " + id + " is referenced by a rule of " + rule.NsgID)
			}
		}
	case *PublicIP:
		if v.AssignedEntityType == entityNAT {
			return errConflict("The public IP " + id + " is assigned to " + v.AssignedEntityID)
		}
	}

	return nil
}

func routeTableRef(r resource) string {
	switch v := r.(type) {
	case *Subnet:
		return v.RouteTableID
	case *InternetGateway:
		return v.RouteTableID
	case *NatGateway:
		return v.RouteTableID
	case *ServiceGateway:
		return v.RouteTableID
	case *LocalPeeringGateway:
		return v.RouteTableID
	}

	return ""
}

// terminate starts deletion and applies the side effects on related resources.
func (b *InMemoryBackend) terminate(r resource) {
	switch v := r.(type) {
	case *Vcn:
		for _, id := range []string{v.DefaultRouteTableID, v.DefaultSecurityList, v.DefaultDhcpOptionsID} {
			if d, ok := b.res[id]; ok && live(d) {
				b.markTerminating(d)
			}
		}
	case *NatGateway:
		b.releaseNatIP(v)
	case *LocalPeeringGateway:
		if peer, ok := b.res[v.PeerID].(*LocalPeeringGateway); ok {
			peer.PeeringStatus, peer.PeeringStatusDetails = peerRevoked, "Peer gateway was deleted"
			b.bump(peer)
		}
	}

	b.markTerminating(r)
}

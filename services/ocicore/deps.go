package ocicore

import "slices"

const maxVcns = 50

func (b *InMemoryBackend) liveRefs(match func(Resource) bool) []Resource {
	var out []Resource

	for _, r := range b.res {
		if live(r) && match(r) {
			out = append(out, r)
		}
	}

	return out
}

// liveOf returns the live resources of type T accepted by match.
func liveOf[T Resource](b *InMemoryBackend, match func(T) bool) []T {
	var out []T

	for _, r := range b.res {
		if t, ok := r.(T); ok && live(r) && match(t) {
			out = append(out, t)
		}
	}

	return out
}

func inVcn[T Resource](vcnID string) func(T) bool {
	return func(t T) bool { return t.vcnRef() == vcnID }
}

func (b *InMemoryBackend) isDefault(vcnID, id string) bool {
	v, ok := b.res[vcnID].(*Vcn)

	return ok && (v.DefaultRouteTableID == id || v.DefaultSecurityList == id || v.DefaultDhcpOptionsID == id)
}

func usedBy(what, id, user string) *APIError {
	if user == "" {
		return nil
	}

	return errConflict("The " + what + " " + id + " cannot be deleted because it is in use by " + user)
}

func firstID[T Resource](items []T) string {
	if len(items) == 0 {
		return ""
	}

	return items[0].base().ID
}

func routesTo(id string) func(*RouteTable) bool {
	return func(rt *RouteTable) bool {
		return slices.ContainsFunc(rt.RouteRules, func(x RouteRule) bool { return x.NetworkEntityID == id })
	}
}

func defaultOf(what string) *APIError {
	return errConflict("The default " + what + " of a VCN cannot be deleted")
}

func (b *InMemoryBackend) deleteVcnCheck(id string) *APIError {
	refs := b.liveRefs(func(x Resource) bool { return x.vcnRef() == id && !b.isDefault(id, x.base().ID) })
	if len(refs) == 0 {
		return nil
	}

	return errConflict("The Vcn " + id + " cannot be deleted because it still contains " +
		ocidKind(refs[0].base().ID) + " " + refs[0].base().ID)
}

func (b *InMemoryBackend) deleteNsgCheck(id string) *APIError {
	if v := firstID(liveOf(b, func(x *Vnic) bool { return slices.Contains(x.NsgIDs, id) })); v != "" {
		return errConflict("The network security group " + id + " is used by VNIC " + v)
	}

	for _, rule := range b.nsgRules {
		if rule.NsgID != id && (rule.Source == id || rule.Destination == id) && live(b.res[rule.NsgID]) {
			return errConflict("The network security group " + id + " is referenced by a rule of " + rule.NsgID)
		}
	}

	return nil
}

// deleteCheck enforces the documented delete-in-use conflicts; callers hold the write lock.
func (b *InMemoryBackend) deleteCheck(r Resource) *APIError {
	for _, check := range []func(Resource) *APIError{b.deleteNetworkCheck, b.deleteDrgKindCheck, b.deleteStorageCheck} {
		if aerr := check(r); aerr != nil {
			return aerr
		}
	}

	return nil
}

func (b *InMemoryBackend) deleteDrgKindCheck(r Resource) *APIError {
	switch v := r.(type) {
	case *Drg:
		return b.deleteDrgCheck(v.ID)
	case *DrgRouteTable:
		return b.deleteDrgTableCheck(v)
	case *DrgRouteDistribution:
		return b.deleteDrgDistCheck(v)
	}

	return nil
}

func (b *InMemoryBackend) deleteNetworkCheck(r Resource) *APIError {
	id := r.base().ID

	switch v := r.(type) {
	case *Vcn:
		return b.deleteVcnCheck(id)
	case *RouteTable:
		if b.isDefault(v.VcnID, id) {
			return defaultOf("route table")
		}

		return usedBy("route table", id, firstUser(b, func(x Resource) bool { return routeTableRef(x) == id }))
	case *SecurityList:
		if b.isDefault(v.VcnID, id) {
			return defaultOf("security list")
		}

		return usedBy("security list", id, firstID(liveOf(b, func(s *Subnet) bool {
			return slices.Contains(s.SecurityListIDs, id)
		})))
	case *DhcpOptions:
		if b.isDefault(v.VcnID, id) {
			return defaultOf("DHCP options")
		}

		return usedBy("DHCP options", id, firstID(liveOf(b, func(s *Subnet) bool { return s.DhcpOptionsID == id })))
	case *InternetGateway, *NatGateway, *ServiceGateway, *LocalPeeringGateway:
		return usedBy("gateway", id, firstID(liveOf(b, routesTo(id))))
	case *NetworkSecurityGroup:
		return b.deleteNsgCheck(id)
	case *PublicIP:
		if v.AssignedEntityType == entityNAT {
			return errConflict("The public IP " + id + " is assigned to " + v.AssignedEntityID)
		}
	case *Subnet:
		return b.deleteSubnetCheck(id)
	}

	return nil
}

func (b *InMemoryBackend) deleteStorageCheck(r Resource) *APIError {
	id := r.base().ID

	switch v := r.(type) {
	case *Volume:
		if aerr := b.groupMemberBlock("volume", id, v.VolumeGroupID); aerr != nil {
			return aerr
		}

		return usedBy("volume", id, firstID(liveOf(b, func(a *VolumeAttachment) bool { return a.VolumeID == id })))
	case *BootVolume:
		if aerr := b.groupMemberBlock("boot volume", id, v.VolumeGroupID); aerr != nil {
			return aerr
		}

		return usedBy(
			"boot volume",
			id,
			firstID(liveOf(b, func(a *BootVolumeAttachment) bool { return a.BootVolumeID == id })),
		)
	case *VolumeBackup:
		if aerr := b.memberBackupBlock(id, v.VolumeGroupBackupID); aerr != nil {
			return aerr
		}

		return b.retentionBlock("volume backup", id, &v.Retention)
	case *BootVolumeBackup:
		if aerr := b.memberBackupBlock(id, v.VolumeGroupBackupID); aerr != nil {
			return aerr
		}

		return b.retentionBlock("boot volume backup", id, &v.Retention)
	case *VolumeGroupBackup:
		return b.deleteGroupBackupCheck(v)
	}

	return nil
}

func (b *InMemoryBackend) groupMemberBlock(what, id, groupID string) *APIError {
	if g, ok := b.res[groupID].(*VolumeGroup); ok && live(g) {
		return errConflict(
			"The " + what + " " + id + " belongs to volume group " + groupID + ": remove it from the group first",
		)
	}

	return nil
}

func (b *InMemoryBackend) deleteSubnetCheck(id string) *APIError {
	if v := firstID(liveOf(b, func(x *Vnic) bool { return x.SubnetID == id })); v != "" {
		return errConflict("The subnet " + id + " cannot be deleted because it still contains VNIC " + v)
	}

	if p := firstID(liveOf(b, func(x *PrivateIP) bool { return x.SubnetID == id })); p != "" {
		return errConflict("The subnet " + id + " cannot be deleted because it still contains private IP " + p)
	}

	return nil
}

func firstUser(b *InMemoryBackend, match func(Resource) bool) string {
	return firstID(b.liveRefs(match))
}

func routeTableRef(r Resource) string {
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
func (b *InMemoryBackend) terminate(r Resource) {
	switch v := r.(type) {
	case *Vcn:
		for _, id := range []string{v.DefaultRouteTableID, v.DefaultSecurityList, v.DefaultDhcpOptionsID} {
			if d, ok := b.res[id]; ok && live(d) {
				b.markTerminating(d)
			}
		}
	case *Drg:
		b.terminateDrg(v.ID)
	case *Volume:
		b.dropAssignments(v.ID)
		b.endReplicas(v)
	case *BootVolume:
		b.dropAssignments(v.ID)
	case *VolumeGroup:
		b.leaveGroup(v)
		b.dropAssignments(v.ID)
	case *VolumeGroupBackup:
		b.terminateGroupBackup(v)
	case *NatGateway:
		b.releaseNatIP(v)
	case *LocalPeeringGateway:
		if peer, ok := b.res[v.PeerID].(*LocalPeeringGateway); ok {
			peer.PeeringStatus, peer.PeeringStatusDetails = peerRevoked, "Peer gateway was deleted"
			b.bump(peer)
		}
	}

	vnicID := ""

	switch ip := r.(type) {
	case *PublicIP:
		vnicID = b.vnicOfPublic(ip)
	case *Ipv6:
		vnicID = ip.VnicID
	}

	b.markTerminating(r)
	b.refreshVnic(vnicID)
}

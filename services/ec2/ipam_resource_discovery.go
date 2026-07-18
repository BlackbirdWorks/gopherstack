package ec2

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// ---- IPAM Resource Discoveries (user-created) ----

// CreateIpamResourceDiscovery creates a standalone (non-default) IPAM resource discovery that
// can later be associated with one or more IPAMs via AssociateIpamResourceDiscovery.
func (b *InMemoryBackend) CreateIpamResourceDiscovery(
	description string, operatingRegions []string,
) (*IpamResourceDiscovery, error) {
	b.mu.Lock("CreateIpamResourceDiscovery")
	defer b.mu.Unlock()

	id := "ipam-res-disco-" + uuid.New().String()[:8]
	d := &IpamResourceDiscovery{
		IpamResourceDiscoveryID: id,
		IpamResourceDiscoveryARN: "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
			":ipam-resource-discovery/" + id,
		OwnerID:          b.AccountID,
		Region:           b.Region,
		State:            ipamStateCreateComplete,
		Description:      description,
		OperatingRegions: append([]string(nil), operatingRegions...),
	}
	b.ipamResourceDiscoveries.Put(d)

	cp := *d
	cp.OperatingRegions = append([]string(nil), d.OperatingRegions...)

	return &cp, nil
}

// DeleteIpamResourceDiscovery removes a non-default IPAM resource discovery. Default resource
// discoveries (auto-created by CreateIpam) and discoveries still associated with an IPAM
// cannot be deleted.
func (b *InMemoryBackend) DeleteIpamResourceDiscovery(id string) (*IpamResourceDiscovery, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamResourceDiscoveryId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIpamResourceDiscovery")
	defer b.mu.Unlock()

	d, ok := b.ipamResourceDiscoveries.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamResourceDiscoveryNotFound, id)
	}

	if d.IsDefault {
		return nil, fmt.Errorf(
			"%w: the default resource discovery of an IPAM cannot be deleted", ErrIpamResourceDiscoveryInUse,
		)
	}

	for _, a := range b.ipamResourceDiscoveryAssocs.All() {
		if a.IpamResourceDiscoveryID == id {
			return nil, fmt.Errorf(
				"%w: resource discovery %s has existing IPAM associations", ErrIpamResourceDiscoveryInUse, id,
			)
		}
	}
	b.ipamResourceDiscoveries.Delete(id)

	cp := *d
	cp.OperatingRegions = append([]string(nil), d.OperatingRegions...)
	cp.State = ipamStateDeleteComplete

	return &cp, nil
}

// AssociateIpamResourceDiscovery associates a standalone resource discovery with an IPAM.
func (b *InMemoryBackend) AssociateIpamResourceDiscovery(
	ipamID, discoveryID string,
) (*IpamResourceDiscoveryAssociation, error) {
	if ipamID == "" || discoveryID == "" {
		return nil, fmt.Errorf("%w: IpamId and IpamResourceDiscoveryId are required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateIpamResourceDiscovery")
	defer b.mu.Unlock()

	ipam, ok := b.ipams.Get(ipamID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamNotFound, ipamID)
	}

	if _, discoveryOK := b.ipamResourceDiscoveries.Get(discoveryID); !discoveryOK {
		return nil, fmt.Errorf("%w: %s", ErrIpamResourceDiscoveryNotFound, discoveryID)
	}

	assocID := "ipam-res-disco-assoc-" + uuid.New().String()[:8]
	assoc := &IpamResourceDiscoveryAssociation{
		IpamResourceDiscoveryAssociationID: assocID,
		IpamID:                             ipamID,
		IpamARN:                            ipam.IpamARN,
		IpamRegion:                         b.Region,
		IpamResourceDiscoveryID:            discoveryID,
		OwnerID:                            b.AccountID,
		IsDefault:                          false,
		ResourceDiscoveryStatus:            ipamResourceDiscoveryAssocStatus,
		State:                              ipamAssocStateAssociateComplete,
	}
	assoc.IpamResourceDiscoveryAssociationARN = "arn:aws:ec2:" + b.Region + ":" + b.AccountID +
		":ipam-resource-discovery-association/" + assocID
	b.ipamResourceDiscoveryAssocs.Put(assoc)
	ipam.ResourceDiscoveryAssociationCount++

	cp := *assoc

	return &cp, nil
}

// DisassociateIpamResourceDiscovery removes a (non-default) resource discovery association
// from an IPAM.
func (b *InMemoryBackend) DisassociateIpamResourceDiscovery(
	assocID string,
) (*IpamResourceDiscoveryAssociation, error) {
	if assocID == "" {
		return nil, fmt.Errorf("%w: IpamResourceDiscoveryAssociationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateIpamResourceDiscovery")
	defer b.mu.Unlock()

	assoc, ok := b.ipamResourceDiscoveryAssocs.Get(assocID)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamResourceDiscoveryAssociationNotFound, assocID)
	}

	if assoc.IsDefault {
		return nil, fmt.Errorf(
			"%w: the default resource discovery association of an IPAM cannot be disassociated",
			ErrIpamResourceDiscoveryInUse,
		)
	}

	if ipam, ipamOK := b.ipams.Get(assoc.IpamID); ipamOK && ipam.ResourceDiscoveryAssociationCount > 0 {
		ipam.ResourceDiscoveryAssociationCount--
	}
	b.ipamResourceDiscoveryAssocs.Delete(assocID)

	cp := *assoc
	cp.State = ipamAssocStateDisassociateComplete

	return &cp, nil
}

// ModifyIpamResourceDiscovery updates a resource discovery's description and operating regions.
func (b *InMemoryBackend) ModifyIpamResourceDiscovery(
	id, description string, addOperatingRegions, removeOperatingRegions []string,
) (*IpamResourceDiscovery, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: IpamResourceDiscoveryId is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIpamResourceDiscovery")
	defer b.mu.Unlock()

	d, ok := b.ipamResourceDiscoveries.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamResourceDiscoveryNotFound, id)
	}

	if description != "" {
		d.Description = description
	}

	d.OperatingRegions = applyOperatingRegionDelta(d.OperatingRegions, addOperatingRegions, removeOperatingRegions)
	d.State = ipamStateModifyComplete

	cp := *d
	cp.OperatingRegions = append([]string(nil), d.OperatingRegions...)

	return &cp, nil
}

// applyOperatingRegionDelta returns current with removeRegions removed and addRegions added
// (deduplicated), mirroring the Add.../Remove... semantics of ModifyIpamResourceDiscovery.
func applyOperatingRegionDelta(current, addRegions, removeRegions []string) []string {
	remove := make(map[string]bool, len(removeRegions))
	for _, r := range removeRegions {
		remove[r] = true
	}

	kept := make(map[string]bool, len(current))
	out := make([]string, 0, len(current)+len(addRegions))

	for _, r := range current {
		if remove[r] || kept[r] {
			continue
		}

		kept[r] = true
		out = append(out, r)
	}

	for _, r := range addRegions {
		if kept[r] {
			continue
		}

		kept[r] = true
		out = append(out, r)
	}

	return out
}

// ---- IPAM Resource CIDRs ----

// ipamResourceCidrKey builds the internal map key for a monitored resource CIDR.
func ipamResourceCidrKey(resourceID, resourceCidr string) string {
	return resourceID + "|" + resourceCidr
}

// recordIpamResourceCidrLocked creates or refreshes the monitored resource CIDR entry backing
// a pool allocation. Must be called with b.mu held.
func (b *InMemoryBackend) recordIpamResourceCidrLocked(pool *IpamPool, alloc *IpamPoolAllocation) {
	if alloc.ResourceID == "" {
		return
	}

	b.ipamResourceCidrs.Put(&IpamResourceCidr{
		IpamID:          pool.IpamID,
		IpamPoolID:      pool.IpamPoolID,
		IpamScopeID:     pool.IpamScopeID,
		ResourceID:      alloc.ResourceID,
		ResourceCidr:    alloc.Cidr,
		ResourceRegion:  b.Region,
		ResourceType:    alloc.ResourceType,
		ResourceOwnerID: alloc.ResourceOwner,
		ManagementState: "managed",
		Monitored:       true,
	})
}

// forgetIpamResourceCidrLocked removes the monitored resource CIDR entry backing a released
// pool allocation. Must be called with b.mu held.
func (b *InMemoryBackend) forgetIpamResourceCidrLocked(alloc *IpamPoolAllocation) {
	b.ipamResourceCidrs.Delete(ipamResourceCidrKey(alloc.ResourceID, alloc.Cidr))
}

// GetIpamResourceCidrs returns resource CIDRs monitored by IPAM in the given scope, optionally
// filtered by pool, resource ID, resource owner, or resource type.
func (b *InMemoryBackend) GetIpamResourceCidrs(
	scopeID, poolID, resourceID, resourceOwner, resourceType string,
) ([]*IpamResourceCidr, error) {
	if scopeID == "" {
		return nil, fmt.Errorf("%w: IpamScopeId is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetIpamResourceCidrs")
	defer b.mu.RUnlock()

	if _, ok := b.ipamScopes.Get(scopeID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrIpamScopeNotFound, scopeID)
	}

	out := make([]*IpamResourceCidr, 0, b.ipamResourceCidrs.Len())

	for _, c := range b.ipamResourceCidrs.All() {
		if c.IpamScopeID != scopeID {
			continue
		}

		if poolID != "" && c.IpamPoolID != poolID {
			continue
		}

		if resourceID != "" && c.ResourceID != resourceID {
			continue
		}

		if resourceOwner != "" && c.ResourceOwnerID != resourceOwner {
			continue
		}

		if resourceType != "" && c.ResourceType != resourceType {
			continue
		}

		cp := *c
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ResourceID < out[j].ResourceID
	})

	return out, nil
}

// ModifyIpamResourceCidr moves a monitored resource CIDR between scopes and/or updates its
// monitored flag.
func (b *InMemoryBackend) ModifyIpamResourceCidr(
	currentScopeID, resourceCidr, resourceID, resourceRegion string, monitored bool, destScopeID string,
) (*IpamResourceCidr, error) {
	if currentScopeID == "" || resourceCidr == "" || resourceID == "" {
		return nil, fmt.Errorf(
			"%w: CurrentIpamScopeId, ResourceCidr, and ResourceId are required", ErrInvalidParameter,
		)
	}

	b.mu.Lock("ModifyIpamResourceCidr")
	defer b.mu.Unlock()

	key := ipamResourceCidrKey(resourceID, resourceCidr)

	c, ok := b.ipamResourceCidrs.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: %s %s", ErrIpamResourceCidrNotFound, resourceID, resourceCidr)
	}

	if c.IpamScopeID != currentScopeID {
		return nil, fmt.Errorf(
			"%w: %s is not currently in scope %s", ErrIpamResourceCidrNotFound, resourceID, currentScopeID,
		)
	}

	if resourceRegion != "" {
		c.ResourceRegion = resourceRegion
	}

	c.Monitored = monitored
	if !monitored {
		c.ManagementState = "unmanaged"
	} else {
		c.ManagementState = "managed"
	}

	if destScopeID != "" {
		c.IpamScopeID = destScopeID
	}

	cp := *c

	return &cp, nil
}

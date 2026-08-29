package route53

import (
	"fmt"
	"sort"
)

// AssociateVPCWithHostedZone associates a VPC with a private hosted zone.
// Returns ErrPublicZoneVPCAssociation when called on a public zone.
//
// Re-associating a VPC that is already associated with this same zone is a
// no-op success, not an error: AWS's documented AssociateVPCWithHostedZone
// error list has no "duplicate association" error, and its one
// association-conflict error (ConflictingDomainExists) is explicitly scoped
// to "the VPC is already associated with *another* hosted zone that has the
// same name" — which rules it out for the same-VPC-same-zone case handled
// here.
func (b *InMemoryBackend) AssociateVPCWithHostedZone(zoneID, vpcID, vpcRegion string) error {
	if vpcID == "" {
		return fmt.Errorf("%w: VPCId is required", ErrInvalidInput)
	}

	b.mu.Lock("AssociateVPCWithHostedZone")
	defer b.mu.Unlock()

	zd, ok := b.zones.Get(zoneID)
	if !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	if !zd.zone.PrivateZone {
		return fmt.Errorf(
			"%w: cannot associate a VPC with public hosted zone %s",
			ErrPublicZoneVPCAssociation,
			zoneID,
		)
	}

	for _, existing := range b.vpcAssociations[zoneID] {
		if existing.VPCID == vpcID {
			return nil
		}
	}

	b.vpcAssociations[zoneID] = append(b.vpcAssociations[zoneID], vpcAssociation{
		VPCID:     vpcID,
		VPCRegion: vpcRegion,
	})

	return nil
}

// DisassociateVPCFromHostedZone removes a VPC association from a private hosted zone.
func (b *InMemoryBackend) DisassociateVPCFromHostedZone(zoneID, vpcID string) error {
	b.mu.Lock("DisassociateVPCFromHostedZone")
	defer b.mu.Unlock()

	zd, ok := b.zones.Get(zoneID)
	if !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	if !zd.zone.PrivateZone {
		return fmt.Errorf("%w: hosted zone %s is not private", ErrInvalidInput, zoneID)
	}

	assocs := b.vpcAssociations[zoneID]
	newAssocs := assocs[:0:0]

	for _, a := range assocs {
		if a.VPCID != vpcID {
			newAssocs = append(newAssocs, a)
		}
	}

	if len(newAssocs) == len(assocs) {
		return fmt.Errorf(
			"%w: VPC %s is not associated with hosted zone %s",
			ErrVPCAssociationNotFound,
			vpcID,
			zoneID,
		)
	}

	// AWS rejects removal of the last VPC from a private hosted zone.
	if len(newAssocs) == 0 {
		return fmt.Errorf(
			"%w: cannot disassociate the last VPC from private hosted zone %s",
			ErrLastVPCAssociation,
			zoneID,
		)
	}

	b.vpcAssociations[zoneID] = newAssocs

	return nil
}

// ListVPCAssociations returns all VPCs associated with a hosted zone.
func (b *InMemoryBackend) ListVPCAssociations(zoneID string) ([]vpcAssociation, error) {
	b.mu.RLock("ListVPCAssociations")
	defer b.mu.RUnlock()

	if _, ok := b.zones.Get(zoneID); !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	result := make([]vpcAssociation, len(b.vpcAssociations[zoneID]))
	copy(result, b.vpcAssociations[zoneID])

	return result, nil
}

// CreateVPCAssociationAuthorization authorizes a VPC to be associated with a hosted zone
// from another AWS account.
func (b *InMemoryBackend) CreateVPCAssociationAuthorization(
	zoneID, vpcID, vpcRegion string,
) (*VPCAssociationAuthorization, error) {
	b.mu.Lock("CreateVPCAssociationAuthorization")
	defer b.mu.Unlock()

	if _, ok := b.zones.Get(zoneID); !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	for _, auth := range b.vpcAssocAuthorizations[zoneID] {
		if auth.VPCID == vpcID {
			return nil, fmt.Errorf(
				"%w: VPC %s authorization already exists for zone %s",
				ErrInvalidInput,
				vpcID,
				zoneID,
			)
		}
	}

	auth := VPCAssociationAuthorization{VPCID: vpcID, VPCRegion: vpcRegion}
	b.vpcAssocAuthorizations[zoneID] = append(b.vpcAssocAuthorizations[zoneID], auth)

	cp := auth

	return &cp, nil
}

// DeleteVPCAssociationAuthorization removes a VPC association authorization.
func (b *InMemoryBackend) DeleteVPCAssociationAuthorization(zoneID, vpcID string) error {
	b.mu.Lock("DeleteVPCAssociationAuthorization")
	defer b.mu.Unlock()

	if _, ok := b.zones.Get(zoneID); !ok {
		return fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	auths := b.vpcAssocAuthorizations[zoneID]
	newAuths := auths[:0:0]

	for _, a := range auths {
		if a.VPCID != vpcID {
			newAuths = append(newAuths, a)
		}
	}

	if len(newAuths) == len(auths) {
		return fmt.Errorf(
			"%w: authorization for VPC %s not found in zone %s",
			ErrVPCAssociationAuthorizationNF,
			vpcID,
			zoneID,
		)
	}

	if len(newAuths) == 0 {
		delete(b.vpcAssocAuthorizations, zoneID)
	} else {
		b.vpcAssocAuthorizations[zoneID] = newAuths
	}

	return nil
}

// ListVPCAssociationAuthorizations returns all VPC association authorizations for a hosted zone.
func (b *InMemoryBackend) ListVPCAssociationAuthorizations(
	zoneID string,
) ([]VPCAssociationAuthorization, error) {
	b.mu.RLock("ListVPCAssociationAuthorizations")
	defer b.mu.RUnlock()

	if _, ok := b.zones.Get(zoneID); !ok {
		return nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	result := make([]VPCAssociationAuthorization, len(b.vpcAssocAuthorizations[zoneID]))
	copy(result, b.vpcAssocAuthorizations[zoneID])

	return result, nil
}

// ListHostedZonesByVPC returns all private hosted zones that have a VPC
// association with the given VPC, truncated to maxItems
// (route53@v1.65.6 api_op_ListHostedZonesByVPC.go).
func (b *InMemoryBackend) ListHostedZonesByVPC(vpcID, vpcRegion string, maxItems int) ([]HostedZone, error) {
	b.mu.RLock("ListHostedZonesByVPC")
	defer b.mu.RUnlock()

	var result []HostedZone

	for zoneID, assocs := range b.vpcAssociations {
		for _, a := range assocs {
			if a.VPCID == vpcID && (vpcRegion == "" || a.VPCRegion == vpcRegion) {
				if zd, ok := b.zones.Get(zoneID); ok {
					cp := zd.zone
					cp.ResourceRecordSetCount = len(zd.records)
					result = append(result, cp)
				}

				break
			}
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	if maxItems > 0 && len(result) > maxItems {
		result = result[:maxItems]
	}

	return result, nil
}

// CountAssociatedVPCs returns the number of VPCs associated with the given
// hosted zone. It returns ErrHostedZoneNotFound if the zone does not exist.
func (b *InMemoryBackend) CountAssociatedVPCs(zoneID string) (int, error) {
	b.mu.RLock("CountAssociatedVPCs")
	defer b.mu.RUnlock()

	if _, ok := b.zones.Get(zoneID); !ok {
		return 0, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	return len(b.vpcAssociations[zoneID]), nil
}

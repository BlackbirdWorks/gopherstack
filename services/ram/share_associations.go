package ram

import (
	"fmt"
	"time"
)

// AssociateResourceShare associates principals or resource ARNs with a resource share.
// Entities that are already associated are silently skipped (idempotent), matching AWS behavior.
// Returns deep copies of the new associations so callers cannot mutate backend state.
func (b *InMemoryBackend) AssociateResourceShare(
	shareARN string,
	principals, resourceARNs []string,
) ([]*ResourceShareAssociation, error) {
	b.mu.Lock("AssociateResourceShare")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares.Get(shareARN)
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	// Build a set of already-associated entities for O(1) deduplication.
	// We don't know how many belong to this share without scanning first,
	// so we start with no hint and let the map grow naturally.
	existing := make(map[string]struct{})
	for _, a := range b.associations {
		if a.ResourceShareARN == shareARN {
			existing[a.AssociatedEntity] = struct{}{}
		}
	}

	// Validate every non-duplicate principal against AllowExternalPrincipals
	// before mutating any state. Checking this inside the mutation loop below
	// would leave associations (and invitations) already appended for
	// earlier principals committed to the backend even though the overall
	// call returns an error to the caller.
	for _, p := range principals {
		if _, dup := existing[p]; dup {
			continue
		}

		if b.isExternalPrincipal(p) && !rs.AllowExternalPrincipals {
			return nil, fmt.Errorf(
				"%w: external principals not allowed for resource share %s",
				ErrValidation,
				shareARN,
			)
		}
	}

	now := time.Now()
	added := make([]*ResourceShareAssociation, 0, len(principals)+len(resourceARNs))

	for _, p := range principals {
		if _, dup := existing[p]; dup {
			continue
		}

		external := b.isExternalPrincipal(p)

		assoc := &ResourceShareAssociation{
			ResourceShareARN:  shareARN,
			ResourceShareName: rs.Name,
			AssociatedEntity:  p,
			AssociationType:   associationTypePrincipal,
			Status:            associationStatusAssociated,
			External:          external,
			CreationTime:      now,
			LastUpdatedTime:   now,
		}
		b.associations = append(b.associations, assoc)
		added = append(added, cloneAssociation(assoc))

		if external {
			receiverID := principalReceiverAccountID(p)
			b.createInvitationLocked(shareARN, rs.Name, receiverID)
		}
	}

	for _, r := range resourceARNs {
		if _, dup := existing[r]; dup {
			continue
		}

		assoc := &ResourceShareAssociation{
			ResourceShareARN:  shareARN,
			ResourceShareName: rs.Name,
			AssociatedEntity:  r,
			AssociationType:   associationTypeResource,
			Status:            associationStatusAssociated,
			External:          false,
			CreationTime:      now,
			LastUpdatedTime:   now,
		}
		b.associations = append(b.associations, assoc)
		added = append(added, cloneAssociation(assoc))
	}

	return added, nil
}

// DisassociateResourceShare removes principals or resource ARNs from a resource share.
func (b *InMemoryBackend) DisassociateResourceShare(
	shareARN string,
	principals, resourceARNs []string,
) ([]*ResourceShareAssociation, error) {
	b.mu.Lock("DisassociateResourceShare")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares.Get(shareARN)
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	toRemove := make(map[string]struct{}, len(principals)+len(resourceARNs))

	for _, p := range principals {
		toRemove[p] = struct{}{}
	}

	for _, r := range resourceARNs {
		toRemove[r] = struct{}{}
	}

	var updated []*ResourceShareAssociation

	kept := b.associations[:0]

	for _, a := range b.associations {
		if a.ResourceShareARN == shareARN {
			if _, found := toRemove[a.AssociatedEntity]; found {
				cp := cloneAssociation(a)
				cp.Status = associationStatusDisassociated
				cp.LastUpdatedTime = time.Now()
				updated = append(updated, cp)

				continue
			}
		}

		kept = append(kept, a)
	}

	// Nil the truncated tail slots so the GC can collect the removed associations.
	for i := len(kept); i < len(b.associations); i++ {
		b.associations[i] = nil
	}

	b.associations = kept

	_ = rs // share lookup above ensures we return NotFound for deleted shares

	return updated, nil
}

// GetResourceShareAssociations returns associations for the given resource share ARNs and type.
func (b *InMemoryBackend) GetResourceShareAssociations(
	associationType string,
	shareARNs []string,
) []*ResourceShareAssociation {
	b.mu.RLock("GetResourceShareAssociations")
	defer b.mu.RUnlock()

	arnSet := make(map[string]struct{}, len(shareARNs))

	for _, a := range shareARNs {
		arnSet[a] = struct{}{}
	}

	result := make([]*ResourceShareAssociation, 0, len(b.associations))

	for _, a := range b.associations {
		if associationType != "" && a.AssociationType != associationType {
			continue
		}

		if len(arnSet) > 0 {
			if _, ok := arnSet[a.ResourceShareARN]; !ok {
				continue
			}
		}

		result = append(result, cloneAssociation(a))
	}

	return result
}

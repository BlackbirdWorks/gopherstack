package ram

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

// ShareFilter narrows the resource shares a list op considers; a zero field imposes no constraint.
type ShareFilter struct {
	Principal    string
	ResourceType string
	ResourceARNs []string
}

// SourceAssociation is one source constraint on a resource share.
type SourceAssociation struct {
	CreationTime     time.Time
	LastUpdatedTime  time.Time
	ResourceShareARN string
	SourceID         string
	Status           string
}

// ResourceShareARNsFor returns the active shares that satisfy every set field of f, or nil for an empty filter.
func (b *InMemoryBackend) ResourceShareARNsFor(f ShareFilter) map[string]struct{} {
	if f.Principal == "" && f.ResourceType == "" && len(f.ResourceARNs) == 0 {
		return nil
	}

	b.mu.RLock("ResourceShareARNsFor")
	defer b.mu.RUnlock()

	result := make(map[string]struct{})

	for shareARN, ok := range b.shareMatchesLocked(f) {
		if ok {
			result[shareARN] = struct{}{}
		}
	}

	return result
}

func (b *InMemoryBackend) shareMatchesLocked(f ShareFilter) map[string]bool {
	principalOK := map[string]bool{}
	resourceOK := map[string]bool{}
	seen := map[string]bool{}

	for _, a := range b.associations {
		if a.Status == associationStatusDisassociated {
			continue
		}

		seen[a.ResourceShareARN] = true

		switch a.AssociationType {
		case associationTypePrincipal:
			if a.AssociatedEntity == f.Principal {
				principalOK[a.ResourceShareARN] = true
			}
		case associationTypeResource:
			arnOK := len(f.ResourceARNs) == 0 || slices.Contains(f.ResourceARNs, a.AssociatedEntity)
			typeOK := f.ResourceType == "" || resourceTypeFromARN(a.AssociatedEntity) == f.ResourceType

			if arnOK && typeOK {
				resourceOK[a.ResourceShareARN] = true
			}
		}
	}

	out := make(map[string]bool, len(seen))
	needResource := f.ResourceType != "" || len(f.ResourceARNs) > 0

	for shareARN := range seen {
		out[shareARN] = (f.Principal == "" || principalOK[shareARN]) && (!needResource || resourceOK[shareARN])
	}

	return out
}

// ConfigureResourceShare stores the CreateResourceShare-only members: the retain flag and source constraints.
func (b *InMemoryBackend) ConfigureResourceShare(shareARN string, retain *bool, sources []string) error {
	b.mu.Lock("ConfigureResourceShare")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares.Get(shareARN)
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	rs.RetainSharingOnAccountLeaveOrganization = retain
	associateSourcesLocked(rs, sources, time.Now())

	return nil
}

// AssociateResourceShareSources adds source constraints to a share, reactivating disassociated ones.
func (b *InMemoryBackend) AssociateResourceShareSources(shareARN string, sources []string) error {
	b.mu.Lock("AssociateResourceShareSources")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares.Get(shareARN)
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	associateSourcesLocked(rs, sources, time.Now())

	return nil
}

// DisassociateResourceShareSources marks the named source constraints DISASSOCIATED.
func (b *InMemoryBackend) DisassociateResourceShareSources(shareARN string, sources []string) error {
	b.mu.Lock("DisassociateResourceShareSources")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares.Get(shareARN)
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	now := time.Now()

	for i := range rs.Sources {
		if slices.Contains(sources, rs.Sources[i].ID) && rs.Sources[i].Status == associationStatusAssociated {
			rs.Sources[i].Status = associationStatusDisassociated
			rs.Sources[i].LastUpdatedTime = now
		}
	}

	return nil
}

func associateSourcesLocked(rs *ResourceShare, sources []string, now time.Time) {
	for _, id := range sources {
		idx := slices.IndexFunc(rs.Sources, func(s ShareSource) bool { return s.ID == id })
		if idx < 0 {
			rs.Sources = append(rs.Sources, ShareSource{
				ID: id, Status: associationStatusAssociated, CreationTime: now, LastUpdatedTime: now,
			})

			continue
		}

		if rs.Sources[idx].Status != associationStatusAssociated {
			rs.Sources[idx].Status = associationStatusAssociated
			rs.Sources[idx].LastUpdatedTime = now
		}
	}
}

// ListSourceAssociations returns source constraints, hiding DISASSOCIATED ones unless status asks for them.
func (b *InMemoryBackend) ListSourceAssociations(shareARNs []string, sourceID, status string) []SourceAssociation {
	b.mu.RLock("ListSourceAssociations")
	defer b.mu.RUnlock()

	var result []SourceAssociation

	for _, rs := range b.resourceShares.All() {
		if rs.Status == statusDeleted || (len(shareARNs) > 0 && !slices.Contains(shareARNs, rs.ARN)) {
			continue
		}

		for _, s := range rs.Sources {
			if (sourceID != "" && s.ID != sourceID) ||
				(status == "" && s.Status == associationStatusDisassociated) ||
				(status != "" && s.Status != status) {
				continue
			}

			result = append(result, SourceAssociation{
				ResourceShareARN: rs.ARN, SourceID: s.ID, Status: s.Status,
				CreationTime: s.CreationTime, LastUpdatedTime: s.LastUpdatedTime,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].ResourceShareARN != result[j].ResourceShareARN {
			return result[i].ResourceShareARN < result[j].ResourceShareARN
		}

		return result[i].SourceID < result[j].SourceID
	})

	return result
}

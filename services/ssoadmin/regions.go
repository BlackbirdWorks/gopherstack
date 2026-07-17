package ssoadmin

import "time"

// AddRegion adds a region to an SSO instance, simulating the asynchronous
// replication workflow AWS documents (status starts ADDING, transitions to
// ACTIVE once observed via ListRegions/DescribeRegion). Returns the region's
// current status, matching AddRegionOutput.Status.
func (b *InMemoryBackend) AddRegion(instanceArn, regionName string) (string, error) {
	b.mu.Lock("AddRegion")
	defer b.mu.Unlock()

	if !b.instances.Has(instanceArn) {
		return "", ErrInstanceNotFound
	}
	for _, r := range b.instanceRegions[instanceArn] {
		if r.RegionName == regionName {
			return r.Status, nil
		}
	}
	b.instanceRegions[instanceArn] = append(b.instanceRegions[instanceArn], RegionMetadata{
		RegionName: regionName,
		Status:     regionStatusAdding,
		AddedDate:  time.Now().UTC(),
	})

	return regionStatusAdding, nil
}

// RemoveRegion initiates removal of a region from an SSO instance. The entry
// is retained with REMOVING status and pruned on the next ListRegions or
// DescribeRegion call, mirroring the DeleteInstance/cascadeDeleteInstance
// lazy-prune pattern. Returns the region's status (REMOVING), matching
// RemoveRegionOutput.Status.
func (b *InMemoryBackend) RemoveRegion(instanceArn, regionName string) (string, error) {
	b.mu.Lock("RemoveRegion")
	defer b.mu.Unlock()

	if !b.instances.Has(instanceArn) {
		return "", ErrInstanceNotFound
	}
	regions := b.instanceRegions[instanceArn]
	for i := range regions {
		if regions[i].RegionName == regionName {
			regions[i].Status = regionStatusRemoving

			return regionStatusRemoving, nil
		}
	}

	return "", ErrRequestNotFound
}

// ListRegions returns the regions associated with an SSO instance. Entries in
// ADDING status lazily transition to ACTIVE and entries in REMOVING status
// are pruned, matching the lazy-transition pattern used elsewhere in this
// backend (e.g. ListInstances, DescribeAccountAssignmentCreationStatus).
func (b *InMemoryBackend) ListRegions(instanceArn string) ([]RegionMetadata, error) {
	b.mu.Lock("ListRegions")
	defer b.mu.Unlock()

	if !b.instances.Has(instanceArn) {
		return nil, ErrInstanceNotFound
	}

	regions := b.instanceRegions[instanceArn]
	kept := regions[:0]

	for i := range regions {
		if regions[i].Status == regionStatusRemoving {
			continue
		}
		if regions[i].Status == regionStatusAdding {
			regions[i].Status = regionStatusActive
		}
		kept = append(kept, regions[i])
	}
	b.instanceRegions[instanceArn] = kept

	result := make([]RegionMetadata, len(kept))
	copy(result, kept)

	return result, nil
}

// DescribeRegion returns metadata for a specific region associated with an
// SSO instance, lazily transitioning ADDING → ACTIVE on first describe (see
// ListRegions doc comment). Returns ResourceNotFoundException if the region
// was never added, or has finished being removed, on this instance.
func (b *InMemoryBackend) DescribeRegion(instanceArn, regionName string) (*RegionMetadata, error) {
	b.mu.Lock("DescribeRegion")
	defer b.mu.Unlock()

	if !b.instances.Has(instanceArn) {
		return nil, ErrInstanceNotFound
	}

	regions := b.instanceRegions[instanceArn]
	for i := range regions {
		if regions[i].RegionName != regionName || regions[i].Status == regionStatusRemoving {
			continue
		}
		if regions[i].Status == regionStatusAdding {
			regions[i].Status = regionStatusActive
		}
		cp := regions[i]

		return &cp, nil
	}

	return nil, ErrRequestNotFound
}

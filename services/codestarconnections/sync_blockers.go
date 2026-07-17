package codestarconnections

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// GetSyncBlockerSummary returns the sync blocker summary for a resource.
func (b *InMemoryBackend) GetSyncBlockerSummary(
	ctx context.Context,
	resourceName, syncType string,
) (*SyncBlockerSummary, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetSyncBlockerSummary")
	defer b.mu.RUnlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	summary := &SyncBlockerSummary{
		ResourceName:   resourceName,
		LatestBlockers: []SyncBlocker{},
	}

	group := b.syncBlockersByResource.Get(key)
	blockers := make([]SyncBlocker, 0, len(group))

	for _, blocker := range group {
		blockers = append(blockers, *blocker)
	}

	// Sort by CreatedAt descending.
	sort.Slice(blockers, func(i, j int) bool {
		return blockers[i].CreatedAt.After(blockers[j].CreatedAt)
	})

	summary.LatestBlockers = blockers

	return summary, nil
}

// CreateSyncBlocker creates a new sync blocker for a resource (test helper + internal use).
func (b *InMemoryBackend) CreateSyncBlocker(
	ctx context.Context,
	resourceName, syncType, blockerType, createdReason string,
) (*SyncBlocker, error) {
	if !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateSyncBlocker")
	defer b.mu.Unlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	id := uuid.NewString()
	blocker := &SyncBlocker{
		ID:            id,
		Type:          blockerType,
		Status:        SyncBlockerStatusActive,
		CreatedAt:     time.Now().UTC(),
		CreatedReason: createdReason,
		ResourceName:  resourceName,
		SyncType:      syncType,
		region:        region,
	}

	b.syncBlockers.Put(blocker)

	cp := *blocker

	return &cp, nil
}

// UpdateSyncBlocker resolves a sync blocker by ID. If the blocker ID is not found
// (or was created in a different region than the caller's context, matching the
// original map-based lookup's region scoping), returns ErrSyncBlockerNotFound --
// the real UpdateSyncBlocker operation documents SyncBlockerDoesNotExistException
// for exactly this case, it does not resolve unknown IDs gracefully.
func (b *InMemoryBackend) UpdateSyncBlocker(
	ctx context.Context,
	id, resolvedReason string,
) (*SyncBlockerSummary, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateSyncBlocker")
	defer b.mu.Unlock()

	blocker, ok := b.syncBlockers.Get(id)
	if !ok || blocker.region != region {
		return nil, fmt.Errorf("%w: sync blocker not found: %s", ErrSyncBlockerNotFound, id)
	}

	now := time.Now().UTC()
	// Status/ResolvedReason/ResolvedAt are not part of any index key
	// (syncBlockers is keyed by ID; byResource derives from
	// region/ResourceName/SyncType, none of which change here), so mutating
	// the stored *SyncBlocker in place is safe -- no Delete+Put needed.
	blocker.Status = SyncBlockerStatusResolved
	blocker.ResolvedReason = resolvedReason
	blocker.ResolvedAt = &now

	// Return summary for the resource that owns this blocker.
	key := regionKey(region, syncConfigKey(blocker.ResourceName, blocker.SyncType))
	summary := &SyncBlockerSummary{
		ResourceName:   blocker.ResourceName,
		LatestBlockers: []SyncBlocker{},
	}

	group := b.syncBlockersByResource.Get(key)
	for _, b2 := range group {
		summary.LatestBlockers = append(summary.LatestBlockers, *b2)
	}

	sort.Slice(summary.LatestBlockers, func(i, j int) bool {
		return summary.LatestBlockers[i].CreatedAt.After(summary.LatestBlockers[j].CreatedAt)
	})

	return summary, nil
}

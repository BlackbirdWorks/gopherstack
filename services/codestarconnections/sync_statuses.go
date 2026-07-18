package codestarconnections

import (
	"context"
	"fmt"
	"time"
)

// GetRepositorySyncStatus returns the latest sync status for a repository link and branch.
func (b *InMemoryBackend) GetRepositorySyncStatus(
	ctx context.Context,
	repositoryLinkID, branch, syncType string,
) (*RepositorySyncStatus, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetRepositorySyncStatus")
	defer b.mu.RUnlock()

	if !b.repositoryLinks.Has(regionKey(region, repositoryLinkID)) {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	key := regionKey(region, repositorySyncStatusKey(repositoryLinkID, branch, syncType))
	if s, ok := b.repositorySyncStatuses.Get(key); ok {
		cp := *s
		cp.Events = append([]SyncEvent(nil), s.Events...)

		return &cp, nil
	}

	return &RepositorySyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    SyncStatusSucceeded,
		Events:    []SyncEvent{},
	}, nil
}

// SetRepositorySyncStatus stores a sync status for a repository link/branch/syncType (test helper).
func (b *InMemoryBackend) SetRepositorySyncStatus(
	ctx context.Context,
	repositoryLinkID, branch, syncType, status string,
	events []SyncEvent,
) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("SetRepositorySyncStatus")
	defer b.mu.Unlock()

	b.repositorySyncStatuses.Put(&RepositorySyncStatus{
		StartedAt:        time.Now().UTC(),
		Status:           status,
		Events:           events,
		region:           region,
		repositoryLinkID: repositoryLinkID,
		branch:           branch,
		syncType:         syncType,
	})
}

// GetResourceSyncStatus returns the latest sync status for a resource.
func (b *InMemoryBackend) GetResourceSyncStatus(
	ctx context.Context,
	resourceName, syncType string,
) (*ResourceSyncStatus, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetResourceSyncStatus")
	defer b.mu.RUnlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	if s, ok := b.resourceSyncStatuses.Get(key); ok {
		cp := *s
		cp.Events = append([]SyncEvent(nil), s.Events...)

		return &cp, nil
	}

	return &ResourceSyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    SyncStatusSucceeded,
		Events:    []SyncEvent{},
	}, nil
}

// SetResourceSyncStatus stores a sync status for a resource (test helper).
func (b *InMemoryBackend) SetResourceSyncStatus(
	ctx context.Context,
	resourceName, syncType, status string,
	events []SyncEvent,
) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("SetResourceSyncStatus")
	defer b.mu.Unlock()

	b.resourceSyncStatuses.Put(&ResourceSyncStatus{
		StartedAt:    time.Now().UTC(),
		Status:       status,
		Events:       events,
		region:       region,
		resourceName: resourceName,
		syncType:     syncType,
	})
}

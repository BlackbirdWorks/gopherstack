package codeconnections

import (
	"context"
	"sort"
	"time"
)

// GetRepositorySyncStatus returns a stub latest sync status for a repository link and branch.
func (b *InMemoryBackend) GetRepositorySyncStatus(
	ctx context.Context,
	repositoryLinkID, _ /*branch*/, _ /*syncType*/ string,
) (*RepositorySyncStatus, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetRepositorySyncStatus")
	defer b.mu.RUnlock()

	if !b.repositoryLinks.Has(regionKey(region, repositoryLinkID)) {
		return nil, ErrNotFound
	}

	return &RepositorySyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    "SUCCEEDED",
		Events:    []SyncEvent{},
	}, nil
}

// GetResourceSyncStatus returns a stub latest sync status for a resource.
func (b *InMemoryBackend) GetResourceSyncStatus(
	ctx context.Context,
	resourceName, syncType string,
) (*ResourceSyncStatus, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetResourceSyncStatus")
	defer b.mu.RUnlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return nil, ErrNotFound
	}

	return &ResourceSyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    "SUCCEEDED",
		Events:    []SyncEvent{},
	}, nil
}

// ListRepositorySyncDefinitions returns the sync definitions derived from the
// repository link's sync configurations. Real AWS docs state that "for
// CFN_STACK_SYNC the parent and target resource are the same".
func (b *InMemoryBackend) ListRepositorySyncDefinitions(
	ctx context.Context,
	repositoryLinkID, syncType string,
) ([]RepositorySyncDefinition, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListRepositorySyncDefinitions")
	defer b.mu.RUnlock()

	if !b.repositoryLinks.Has(regionKey(region, repositoryLinkID)) {
		return nil, ErrNotFound
	}

	cfgs := b.syncConfigurationsByRegion.Get(region)
	result := make([]RepositorySyncDefinition, 0, len(cfgs))

	for _, cfg := range cfgs {
		if cfg.RepositoryLinkID != repositoryLinkID {
			continue
		}

		if syncType != "" && cfg.SyncType != syncType {
			continue
		}

		result = append(result, RepositorySyncDefinition{
			Branch:    cfg.Branch,
			Directory: cfg.ConfigFile,
			Parent:    cfg.ResourceName,
			Target:    cfg.ResourceName,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Target < result[j].Target
	})

	return result, nil
}

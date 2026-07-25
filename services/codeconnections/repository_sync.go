package codeconnections

import (
	"context"
	"crypto/sha1" //nolint:gosec // SHA1 used only to synthesize a deterministic revision Sha, not for security.
	"encoding/hex"
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

// syntheticRevisionSha deterministically derives a git-commit-shaped Sha
// (40 lowercase hex chars, matching a real SHA-1 git object ID) from stable
// sync-configuration identity fields. The real SHA wire type
// (aws-sdk-go-v2/service/codeconnections@v1.10.22 types.SHA) is unconstrained
// beyond min:1/max:255, but this emulation always returns the same value for
// the same configuration rather than a fabricated/random one, since nothing
// in this backend tracks actual repository commit history.
func syntheticRevisionSha(region, resourceName, syncType, branch, configFile string) string {
	sum := sha1.Sum( //nolint:gosec // SHA1 used only to synthesize a deterministic revision Sha, not for security.
		[]byte(region + "|" + resourceName + "|" + syncType + "|" + branch + "|" + configFile),
	)

	return hex.EncodeToString(sum[:])
}

// GetResourceSyncStatus returns the sync status for an AWS resource: the
// latest sync attempt (LatestSync), the resource's desired state
// (DesiredState), and the latest successful attempt (LatestSuccessfulSync),
// mirroring the real GetResourceSyncStatusOutput shape. Because this backend
// does not simulate actual git-repo content/history, every attempt is
// synthesized as an immediately-successful sync whose Initial/Target/Desired
// revisions all reflect the resource's current sync configuration.
func (b *InMemoryBackend) GetResourceSyncStatus(
	ctx context.Context,
	resourceName, syncType string,
) (*ResourceSyncStatus, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetResourceSyncStatus")
	defer b.mu.RUnlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))

	cfg, ok := b.syncConfigurations.Get(key)
	if !ok {
		return nil, ErrNotFound
	}

	revision := Revision{
		Branch:         cfg.Branch,
		Directory:      cfg.ConfigFile,
		OwnerID:        cfg.OwnerID,
		ProviderType:   cfg.ProviderType,
		RepositoryName: cfg.RepositoryName,
		Sha:            syntheticRevisionSha(region, resourceName, syncType, cfg.Branch, cfg.ConfigFile),
	}

	attempt := ResourceSyncAttempt{
		StartedAt:       time.Now().UTC(),
		Status:          "SUCCEEDED",
		Target:          resourceName,
		InitialRevision: revision,
		TargetRevision:  revision,
		Events:          []SyncEvent{},
	}

	successful := attempt

	return &ResourceSyncStatus{
		LatestSync:           attempt,
		DesiredState:         revision,
		LatestSuccessfulSync: &successful,
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

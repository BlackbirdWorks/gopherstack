package ecr

import (
	"context"
	"fmt"
	"time"
)

// PutReplicationConfiguration updates the registry replication configuration.
func (b *InMemoryBackend) PutReplicationConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
	cfg *ReplicationConfig,
) (*ReplicationConfig, error) {
	b.mu.Lock("PutReplicationConfiguration")
	defer b.mu.Unlock()

	if cfg == nil {
		cfg = &ReplicationConfig{}
	}

	b.replicationConfig = copyReplicationConfig(cfg)

	return copyReplicationConfig(b.replicationConfig), nil
}

// DescribeImageReplicationStatus returns the current replication status for an image.
func (b *InMemoryBackend) DescribeImageReplicationStatus(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	imageID ImageIdentifier,
) (*ImageReplicationStatusResult, error) {
	b.mu.RLock("DescribeImageReplicationStatus")
	defer b.mu.RUnlock()

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	img, ok := findImageLocked(b.images, b.imagesByRepo, repositoryName, b.tagIndex[repositoryName], imageID)
	if !ok {
		return nil, fmt.Errorf("%w: image not found", ErrImageNotFound)
	}

	// Compute one replication status entry per destination the registry
	// replication configuration actually targets for THIS repository. Rules whose
	// repositoryFilters do not match the repository contribute no destinations,
	// and a destination in the source region+account is skipped (AWS does not
	// replicate an image onto itself). Each destination's status is derived from
	// how long ago the image was pushed relative to the settle delay, so a
	// freshly pushed image reports IN_PROGRESS and later COMPLETE — real
	// per-destination state rather than a hardcoded COMPLETE.
	dests := b.replicationDestinationsForRepoLocked(repositoryName)
	now := time.Now()
	statuses := make([]ImageReplicationStatusEntry, 0, len(dests))

	for _, dest := range dests {
		registryID := dest.RegistryID
		if registryID == "" {
			registryID = b.accountID
		}

		statuses = append(statuses, ImageReplicationStatusEntry{
			Region:     dest.Region,
			RegistryID: registryID,
			Status:     b.replicationStatusForLocked(img, now),
		})
	}

	return &ImageReplicationStatusResult{
		ImageID:             img.ImageID,
		RepositoryName:      repositoryName,
		ReplicationStatuses: statuses,
	}, nil
}

// replicationDestinationsForRepoLocked returns the deduplicated set of
// replication destinations that apply to repositoryName under the current
// registry replication configuration. Rules with repositoryFilters are only
// honored when the repository matches, and destinations that resolve to the
// source region+account are excluded. Must be called with a read lock held.
func (b *InMemoryBackend) replicationDestinationsForRepoLocked(
	repositoryName string,
) []ReplicationDestination {
	if b.replicationConfig == nil {
		return nil
	}

	seen := make(map[string]struct{})
	dests := make([]ReplicationDestination, 0)

	for _, rule := range b.replicationConfig.Rules {
		if !repoMatchesFilters(repositoryName, rule.RepositoryFilters) {
			continue
		}

		for _, dest := range rule.Destinations {
			registryID := dest.RegistryID
			if registryID == "" {
				registryID = b.accountID
			}

			// AWS rejects a destination equal to the source registry (same
			// region AND same account); such a destination never appears in the
			// replication status.
			if dest.Region == b.region && registryID == b.accountID {
				continue
			}

			key := registryID + ":" + dest.Region
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}

			dests = append(dests, ReplicationDestination{Region: dest.Region, RegistryID: registryID})
		}
	}

	return dests
}

// replicationStatusForLocked derives the replication status of an image toward a
// destination. Replication is modeled as taking replicationSettleDelay to
// finish: an image pushed within that window reports IN_PROGRESS, otherwise
// COMPLETE. The default settle delay of zero means replication is reported as
// COMPLETE immediately, matching a fast/quiescent registry.
func (b *InMemoryBackend) replicationStatusForLocked(img *Image, now time.Time) string {
	if b.replicationSettleDelay > 0 && now.Sub(img.ImagePushedAt) < b.replicationSettleDelay {
		return replicationStatusInProgress
	}

	return replicationStatusComplete
}

func copyReplicationConfig(in *ReplicationConfig) *ReplicationConfig {
	if in == nil {
		return &ReplicationConfig{}
	}

	out := &ReplicationConfig{Rules: make([]ReplicationRule, len(in.Rules))}
	for i, rule := range in.Rules {
		out.Rules[i] = ReplicationRule{
			Destinations:      append([]ReplicationDestination(nil), rule.Destinations...),
			RepositoryFilters: append([]RepositoryFilter(nil), rule.RepositoryFilters...),
		}
	}

	return out
}

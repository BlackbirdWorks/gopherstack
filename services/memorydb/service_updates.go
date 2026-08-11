package memorydb

import (
	"context"
	"slices"
	"sort"
	"time"
)

// defaultServiceUpdates returns the built-in seed service updates. Extracted
// to a function (rather than duplicated inline) so the constructor and
// resetLocked's re-seed stay in sync automatically.
func defaultServiceUpdates() []*ServiceUpdate {
	return []*ServiceUpdate{
		{
			ServiceUpdateName:   "memorydb-20240601-redis-security",
			ReleaseDate:         time.Date(2024, time.June, 1, 0, 0, 0, 0, time.UTC),
			Description:         "Security update for Redis 7.x clusters",
			Status:              clusterStatusAvailable,
			Type:                "security-update",
			Engine:              engineRedis,
			AutoUpdateStartDate: time.Date(2024, time.July, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			ServiceUpdateName:   "memorydb-20240801-engine-update",
			ReleaseDate:         time.Date(2024, time.August, 1, 0, 0, 0, 0, time.UTC),
			Description:         "Engine update with performance improvements",
			Status:              clusterStatusAvailable,
			Type:                "engine-update",
			Engine:              engineRedis,
			AutoUpdateStartDate: time.Date(2024, time.September, 1, 0, 0, 0, 0, time.UTC),
		},
	}
}

// DescribeServiceUpdates returns service updates scoped to the clusters they
// apply to (real AWS: one response entry per cluster, via ServiceUpdate.ClusterName
// -- confirmed against the AWS API reference). req.ClusterNames restricts which
// clusters are considered; clusters aren't tracked, they're fanned out against
// every cluster whose Engine matches the update's Engine (the only real,
// non-fabricated link between a global update definition and the clusters it
// applies to -- MemoryDB service updates are engine-scoped, as seeded in
// defaultServiceUpdates' own Description text). Unknown ClusterNames entries
// simply match no cluster, matching this operation's error set (no
// ClusterNotFoundFault defined for it).
func (b *InMemoryBackend) DescribeServiceUpdates(
	ctx context.Context,
	req *describeServiceUpdatesRequest,
) ([]*ServiceUpdate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	targetClusters := b.serviceUpdateTargetClusters(ctx, req.ClusterNames)

	allUpdates := b.serviceUpdates.All()
	result := make([]*ServiceUpdate, 0, len(allUpdates)*len(targetClusters))
	for _, su := range allUpdates {
		if req.ServiceUpdateName != "" && su.ServiceUpdateName != req.ServiceUpdateName {
			continue
		}

		result = append(result, instancesForUpdate(su, targetClusters, req.Status)...)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ServiceUpdateName != result[j].ServiceUpdateName {
			return result[i].ServiceUpdateName < result[j].ServiceUpdateName
		}

		return result[i].ClusterName < result[j].ClusterName
	})

	return result, nil
}

// serviceUpdateTargetClusters resolves req.ClusterNames to the clusters
// DescribeServiceUpdates should fan updates out against, defaulting to every
// cluster in the region when unset. Must hold at least b.mu.RLock.
func (b *InMemoryBackend) serviceUpdateTargetClusters(ctx context.Context, clusterNames []string) []*Cluster {
	region := getRegion(ctx, b.defaultRegion)

	if len(clusterNames) == 0 {
		return tableAll(b.clusters[region])
	}

	var targetClusters []*Cluster
	for _, name := range clusterNames {
		if c, ok := tableGet(b.clusters[region], name); ok {
			targetClusters = append(targetClusters, c)
		}
	}

	return targetClusters
}

// instancesForUpdate fans a single update definition out into one ServiceUpdate
// per cluster whose Engine matches, applying the per-cluster applied/pending
// status and the Status filter.
func instancesForUpdate(su *ServiceUpdate, clusters []*Cluster, statusFilter []string) []*ServiceUpdate {
	result := make([]*ServiceUpdate, 0, len(clusters))

	for _, c := range clusters {
		if c.Engine != su.Engine {
			continue
		}

		status := su.Status
		if c.AppliedServiceUpdates[su.ServiceUpdateName] {
			status = serviceUpdateStatusComplete
		}

		if len(statusFilter) > 0 && !slices.Contains(statusFilter, status) {
			continue
		}

		cp := *su
		cp.ClusterName = c.Name
		cp.Status = status
		result = append(result, &cp)
	}

	return result
}

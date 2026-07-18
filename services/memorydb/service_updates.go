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
			AutoUpdateStartDate: time.Date(2024, time.July, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			ServiceUpdateName:   "memorydb-20240801-engine-update",
			ReleaseDate:         time.Date(2024, time.August, 1, 0, 0, 0, 0, time.UTC),
			Description:         "Engine update with performance improvements",
			Status:              clusterStatusAvailable,
			Type:                "engine-update",
			AutoUpdateStartDate: time.Date(2024, time.September, 1, 0, 0, 0, 0, time.UTC),
		},
	}
}

// DescribeServiceUpdates returns service updates, optionally filtered.
func (b *InMemoryBackend) DescribeServiceUpdates(
	_ context.Context,
	req *describeServiceUpdatesRequest,
) ([]*ServiceUpdate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	allUpdates := b.serviceUpdates.All()
	result := make([]*ServiceUpdate, 0, len(allUpdates))
	for _, su := range allUpdates {
		if req.ServiceUpdateName != "" && su.ServiceUpdateName != req.ServiceUpdateName {
			continue
		}
		if len(req.Status) > 0 && !slices.Contains(req.Status, su.Status) {
			continue
		}
		cp := *su
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ServiceUpdateName < result[j].ServiceUpdateName
	})

	return result, nil
}

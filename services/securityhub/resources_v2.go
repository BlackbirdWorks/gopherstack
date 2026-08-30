package securityhub

import (
	"maps"
	"sort"
	"time"
)

func (b *InMemoryBackend) GetResourcesV2(
	filters map[string]any, //nolint:revive // existing issue.
	nextToken string,
	maxResults int,
) ([]map[string]any, string) {
	b.mu.RLock("GetResourcesV2")
	defer b.mu.RUnlock()

	// Derive resource list from findings
	resourceMap := make(map[string]map[string]any)

	for _, finding := range b.findings {
		if resources, ok := finding["Resources"].([]any); ok {
			for _, r := range resources {
				if res, ok := r.(map[string]any); ok { //nolint:govet // existing issue.
					if id, ok := res["Id"].(string); ok && id != "" { //nolint:govet // existing issue.
						resourceMap[id] = res
					}
				}
			}
		}
	}

	all := make([]map[string]any, 0, len(resourceMap))

	for _, r := range resourceMap {
		cp := make(map[string]any)
		maps.Copy(cp, r)

		all = append(all, cp)
	}

	// resourceMap is a plain map: range order is unspecified and
	// re-randomized per call, so an unsorted result would drop or duplicate
	// resources across two separate GetResourcesV2 calls that straddle a
	// page boundary (Class E).
	sort.Slice(all, func(i, j int) bool {
		ii, _ := all[i]["Id"].(string)
		jj, _ := all[j]["Id"].(string)

		return ii < jj
	})

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) GetResourcesStatisticsV2(groupByFields []string) []map[string]any {
	resources, _ := b.GetResourcesV2(nil, "", maxDefaultResults)

	return groupByResults(resources, groupByFields, nil)
}

// GetResourcesTrendsV2 returns a single ResourcesTrendsMetricsResult data
// point (Timestamp + TrendsValues.ResourcesCount.AllResources --
// securityhub@v1.75.4 types/types.go:18189-18203,18244-18252,18051-18059).
// The real GetResourcesTrendsV2Input has no GroupByAttribute member
// (api_op_GetResourcesTrendsV2.go:22-46); this backend has no time-bucketed
// analytics engine, so unlike the real per-Granularity series this always
// returns one point for the whole store, timestamped at endTime.
func (b *InMemoryBackend) GetResourcesTrendsV2(startTime, endTime string) []map[string]any {
	resources, _ := b.GetResourcesV2(nil, "", maxDefaultResults)

	ts := endTime
	if ts == "" {
		ts = startTime
	}

	if ts == "" {
		ts = time.Now().UTC().Format(time.RFC3339)
	}

	return []map[string]any{
		{
			"Timestamp": ts,
			"TrendsValues": map[string]any{
				"ResourcesCount": map[string]any{
					"AllResources": len(resources),
				},
			},
		},
	}
}

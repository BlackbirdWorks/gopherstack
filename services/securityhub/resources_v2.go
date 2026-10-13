package securityhub

import (
	"maps"
	"sort"
	"time"
)

// resourceViews derives the resource list from stored findings (one entry per resource
// Id), flattened for ResourcesFilters evaluation. Caller holds b.mu.
func (b *InMemoryBackend) resourceViewsLocked() map[string]map[string]any {
	views := make(map[string]map[string]any)

	for _, finding := range b.findings {
		account, _ := finding[keyAwsAccountID].(string)

		resources, _ := finding["Resources"].([]any)
		for _, r := range resources {
			res, ok := r.(map[string]any)
			if !ok {
				continue
			}

			if id, idOK := res["Id"].(string); idOK && id != "" {
				views[id] = resourceView(res, account)
			}
		}
	}

	return views
}

func (b *InMemoryBackend) GetResourcesV2(
	filters map[string]any,
	nextToken string,
	maxResults int,
) ([]map[string]any, string) {
	b.mu.RLock("GetResourcesV2")
	defer b.mu.RUnlock()

	all := make([]map[string]any, 0)

	for _, view := range b.resourceViewsLocked() {
		if !matchesFiltersV2(resourceTarget(view), filters) {
			continue
		}

		all = append(all, originalResource(view))
	}

	// The views come from a plain map: sort so pages stay stable across calls.
	sort.Slice(all, func(i, j int) bool {
		ii, _ := all[i]["Id"].(string)
		jj, _ := all[j]["Id"].(string)

		return ii < jj
	})

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

// originalResource strips the filter-only keys resourceView adds.
func originalResource(view map[string]any) map[string]any {
	res := maps.Clone(view)
	for _, key := range resourceViewKeys {
		delete(res, key)
	}

	return res
}

func (b *InMemoryBackend) GetResourcesStatisticsV2(rules []GroupByRule, sortOrder string) []map[string]any {
	b.mu.RLock("GetResourcesStatisticsV2")
	defer b.mu.RUnlock()

	views := b.resourceViewsLocked()
	items := make([]map[string]any, 0, len(views))

	for _, view := range views {
		items = append(items, view)
	}

	return groupByResults(items, rules, nil, sortOrder, func(item, filters map[string]any) bool {
		return matchesFiltersV2(resourceTarget(item), filters)
	})
}

// GetResourcesTrendsV2 returns a single ResourcesTrendsMetricsResult data
// point (Timestamp + TrendsValues.ResourcesCount.AllResources --
// securityhub@v1.75.4 types/types.go:18189-18203,18244-18252,18051-18059).
// The real GetResourcesTrendsV2Input has no GroupByAttribute member
// (api_op_GetResourcesTrendsV2.go:22-46); this backend has no time-bucketed
// analytics engine, so unlike the real per-Granularity series this always
// returns one point for the whole store, timestamped at endTime.
func (b *InMemoryBackend) GetResourcesTrendsV2(startTime, endTime string, filters map[string]any) []map[string]any {
	resources, _ := b.GetResourcesV2(filters, "", maxDefaultResults)

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

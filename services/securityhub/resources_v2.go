package securityhub

import (
	"fmt"
	"maps"
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

	var all []map[string]any //nolint:prealloc // existing issue.

	for _, r := range resourceMap {
		cp := make(map[string]any)
		maps.Copy(cp, r)

		all = append(all, cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) GetResourcesStatisticsV2(groupByAttributes []string) []map[string]any {
	resources, _ := b.GetResourcesV2(nil, "", maxDefaultResults)

	type key struct{ attr, val string }
	counts := make(map[key]int)

	for _, r := range resources {
		for _, attr := range groupByAttributes {
			val := ""
			if v, ok := r[attr]; ok {
				val = fmt.Sprintf("%v", v)
			}

			counts[key{attr, val}]++
		}
	}

	var result []map[string]any //nolint:prealloc // existing issue.

	for k, count := range counts {
		result = append(result, map[string]any{
			keyGroupByAttribute: k.attr,
			"GroupByValue":      k.val,
			keyCount:            count,
		})
	}

	return result
}

func (b *InMemoryBackend) GetResourcesTrendsV2(
	groupByAttribute string,
	startTime, endTime string,
) []map[string]any {
	resources, _ := b.GetResourcesV2(nil, "", maxDefaultResults)

	return []map[string]any{
		{
			keyGroupByAttribute: groupByAttribute,
			"DateRanges": []map[string]any{
				{
					"DateRange": map[string]any{
						"StartDate": startTime,
						"EndDate":   endTime,
					},
					keyCount: len(resources),
				},
			},
		},
	}
}

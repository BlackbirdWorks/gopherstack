package awsconfig

import (
	"fmt"
	"slices"
	"strings"
)

const (
	groupByResourceType = "RESOURCE_TYPE"
	groupByAccountID    = "ACCOUNT_ID"
	groupByRegion       = "AWS_REGION"
)

// ResourceTypeCount is one per-type entry of GetDiscoveredResourceCounts.
type ResourceTypeCount struct {
	ResourceType string `json:"resourceType"`
	Count        int64  `json:"count"`
}

// GroupedResourceCount is one group of GetAggregateDiscoveredResourceCounts.
type GroupedResourceCount struct {
	GroupName     string `json:"GroupName"`
	ResourceCount int64  `json:"ResourceCount"`
}

// ResourceCountFilters narrows GetAggregateDiscoveredResourceCounts.
type ResourceCountFilters struct {
	AccountID    string `json:"AccountId,omitempty"`
	Region       string `json:"Region,omitempty"`
	ResourceType string `json:"ResourceType,omitempty"`
}

// typeCountsLocked returns sorted per-type counts, limited to types when non-empty.
func (b *InMemoryBackend) typeCountsLocked(types []string) ([]ResourceTypeCount, int64) {
	counts := make(map[string]int64)
	for _, item := range b.resourceConfigs.All() {
		counts[item.ResourceType]++
	}

	out := make([]ResourceTypeCount, 0, len(counts))

	var total int64

	for t, n := range counts {
		if len(types) > 0 && !slices.Contains(types, t) {
			continue
		}

		out = append(out, ResourceTypeCount{ResourceType: t, Count: n})
		total += n
	}

	slices.SortFunc(out, func(a, c ResourceTypeCount) int { return strings.Compare(a.ResourceType, c.ResourceType) })

	return out, total
}

// DiscoveredResourceTypeCounts returns per-type counts and their total.
func (b *InMemoryBackend) DiscoveredResourceTypeCounts(types []string) ([]ResourceTypeCount, int64) {
	b.mu.RLock("DiscoveredResourceTypeCounts")
	defer b.mu.RUnlock()

	return b.typeCountsLocked(types)
}

// AggregateResourceCounts groups discovered resources by groupBy (RESOURCE_TYPE,
// ACCOUNT_ID or AWS_REGION); this single-account emulator has one of each.
func (b *InMemoryBackend) AggregateResourceCounts(
	aggregatorName, groupBy string, f ResourceCountFilters,
) ([]GroupedResourceCount, int64, error) {
	b.mu.RLock("AggregateResourceCounts")
	defer b.mu.RUnlock()

	if err := b.requireAggregatorLocked(aggregatorName); err != nil {
		return nil, 0, err
	}

	switch groupBy {
	case "", groupByResourceType, groupByAccountID, groupByRegion:
	default:
		return nil, 0, fmt.Errorf("%w: invalid GroupByKey %q", ErrValidation, groupBy)
	}

	if (f.AccountID != "" && f.AccountID != b.accountID) || (f.Region != "" && f.Region != b.region) {
		return []GroupedResourceCount{}, 0, nil
	}

	var types []string
	if f.ResourceType != "" {
		types = []string{f.ResourceType}
	}

	perType, total := b.typeCountsLocked(types)

	switch groupBy {
	case groupByResourceType:
		groups := make([]GroupedResourceCount, 0, len(perType))
		for _, c := range perType {
			groups = append(groups, GroupedResourceCount{GroupName: c.ResourceType, ResourceCount: c.Count})
		}

		return groups, total, nil
	case groupByAccountID:
		return []GroupedResourceCount{{GroupName: b.accountID, ResourceCount: total}}, total, nil
	case groupByRegion:
		return []GroupedResourceCount{{GroupName: b.region, ResourceCount: total}}, total, nil
	default:
		return []GroupedResourceCount{}, total, nil
	}
}

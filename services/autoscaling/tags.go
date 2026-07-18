package autoscaling

import (
	"fmt"
	"sort"
)

// CreateOrUpdateTags creates or updates tags on Auto Scaling resources.
// Only group (auto-scaling-group) resource tags are currently supported.
func (b *InMemoryBackend) CreateOrUpdateTags(tags []ResourceTag) error {
	b.mu.Lock("CreateOrUpdateTags")
	defer b.mu.Unlock()

	for _, tag := range tags {
		if tag.ResourceType != resourceTypeAutoScalingGroup {
			continue
		}

		g, ok := b.groups.Get(tag.ResourceID)
		if !ok {
			return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, tag.ResourceID)
		}

		updated := false

		for i, t := range g.Tags {
			if t.Key == tag.Key {
				g.Tags[i].Value = tag.Value
				updated = true

				break
			}
		}

		if !updated {
			g.Tags = append(g.Tags, Tag{Key: tag.Key, Value: tag.Value})
		}
	}

	return nil
}

// DeleteTags removes tags from Auto Scaling resources.
// Only auto-scaling-group resource tags are supported.
func (b *InMemoryBackend) DeleteTags(tags []ResourceTag) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	for _, tag := range tags {
		if tag.ResourceType != resourceTypeAutoScalingGroup {
			continue
		}

		g, ok := b.groups.Get(tag.ResourceID)
		if !ok {
			return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, tag.ResourceID)
		}

		newTags := make([]Tag, 0, len(g.Tags))

		for _, t := range g.Tags {
			if t.Key != tag.Key {
				newTags = append(newTags, t)
			}
		}

		g.Tags = newTags
	}

	return nil
}

// buildTagFilterMap converts a slice of TagFilters into a nested map for O(1) lookups.
func buildTagFilterMap(filters []TagFilter) map[string]map[string]bool {
	m := make(map[string]map[string]bool, len(filters))

	for _, f := range filters {
		vals := make(map[string]bool, len(f.Values))

		for _, v := range f.Values {
			vals[v] = true
		}

		m[f.Name] = vals
	}

	return m
}

// tagMatchesFilters reports whether the tag identified by (resourceID, key, value) passes all filters.
func tagMatchesFilters(filterMap map[string]map[string]bool, resourceID, key, value string) bool {
	if len(filterMap) == 0 {
		return true
	}

	if ids, ok := filterMap[resourceTypeAutoScalingGroup]; ok && !ids[resourceID] {
		return false
	}

	if keys, ok := filterMap["key"]; ok && !keys[key] {
		return false
	}

	if vals, ok := filterMap["value"]; ok && !vals[value] {
		return false
	}

	return true
}

// DescribeTags returns tags for Auto Scaling resources, with optional filtering.
func (b *InMemoryBackend) DescribeTags(filters []TagFilter) ([]ResourceTag, error) {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	filterMap := buildTagFilterMap(filters)

	var result []ResourceTag

	for _, g := range b.groups.All() {
		for _, t := range g.Tags {
			if tagMatchesFilters(filterMap, g.AutoScalingGroupName, t.Key, t.Value) {
				result = append(result, ResourceTag{
					ResourceID:   g.AutoScalingGroupName,
					ResourceType: resourceTypeAutoScalingGroup,
					Key:          t.Key,
					Value:        t.Value,
				})
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].ResourceID != result[j].ResourceID {
			return result[i].ResourceID < result[j].ResourceID
		}

		return result[i].Key < result[j].Key
	})

	return result, nil
}

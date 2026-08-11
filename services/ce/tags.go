package ce

import "maps"

// ListTagsForResource returns the tags for a CE resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if cat, ok := b.costCategories.Get(resourceARN); ok {
		out := make(map[string]string, len(cat.Tags))
		maps.Copy(out, cat.Tags)

		return out, nil
	}

	if mon, ok := b.anomalyMonitors.Get(resourceARN); ok {
		out := make(map[string]string, len(mon.Tags))
		maps.Copy(out, mon.Tags)

		return out, nil
	}

	if sub, ok := b.anomalySubscriptions.Get(resourceARN); ok {
		out := make(map[string]string, len(sub.Tags))
		maps.Copy(out, sub.Tags)

		return out, nil
	}

	return nil, ErrNotFound
}

// TagResource adds or updates tags on a CE resource.
func (b *InMemoryBackend) TagResource(resourceARN string, resourceTags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if cat, ok := b.costCategories.Get(resourceARN); ok {
		maps.Copy(cat.Tags, resourceTags)

		return nil
	}

	if mon, ok := b.anomalyMonitors.Get(resourceARN); ok {
		maps.Copy(mon.Tags, resourceTags)

		return nil
	}

	if sub, ok := b.anomalySubscriptions.Get(resourceARN); ok {
		maps.Copy(sub.Tags, resourceTags)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a CE resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if cat, ok := b.costCategories.Get(resourceARN); ok {
		for _, k := range tagKeys {
			delete(cat.Tags, k)
		}

		return nil
	}

	if mon, ok := b.anomalyMonitors.Get(resourceARN); ok {
		for _, k := range tagKeys {
			delete(mon.Tags, k)
		}

		return nil
	}

	if sub, ok := b.anomalySubscriptions.Get(resourceARN); ok {
		for _, k := range tagKeys {
			delete(sub.Tags, k)
		}

		return nil
	}

	return ErrNotFound
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every CE resource ARN (cost categories, anomaly monitors,
// anomaly subscriptions) that currently has at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	b.costCategories.Range(func(cat *CostCategory) bool {
		if len(cat.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: cat.ARN, Tags: cloneCETags(cat.Tags)})
		}

		return true
	})

	b.anomalyMonitors.Range(func(mon *AnomalyMonitor) bool {
		if len(mon.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: mon.MonitorARN, Tags: cloneCETags(mon.Tags)})
		}

		return true
	})

	b.anomalySubscriptions.Range(func(sub *AnomalySubscription) bool {
		if len(sub.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: sub.SubscriptionARN, Tags: cloneCETags(sub.Tags)})
		}

		return true
	})

	return out
}

func cloneCETags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out
}

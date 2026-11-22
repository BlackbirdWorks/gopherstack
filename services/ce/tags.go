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

// TaggedResources lists every cost category, anomaly monitor and anomaly subscription that carries at least
// one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(true)
}

// InventoryResources lists every cost category, anomaly monitor and anomaly subscription, tagged or not.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(false)
}

func (b *InMemoryBackend) entries(taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, v := range b.costCategories.All() {
		if !taggedOnly || len(v.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: v.ARN, Tags: cloneCETags(v.Tags)})
		}
	}

	for _, v := range b.anomalyMonitors.All() {
		if !taggedOnly || len(v.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: v.MonitorARN, Tags: cloneCETags(v.Tags)})
		}
	}

	for _, v := range b.anomalySubscriptions.All() {
		if !taggedOnly || len(v.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: v.SubscriptionARN, Tags: cloneCETags(v.Tags)})
		}
	}

	return out
}

func cloneCETags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out
}

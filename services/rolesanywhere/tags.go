package rolesanywhere

import "context"

// TagResource adds tags to a resource. Region is resolved from the resource ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, tags []TagEntry) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	region := regionFromARN(resourceARN, getRegion(ctx, b.defaultRegion))
	tagStore := b.tagsStore(region)
	existing := tagStore[resourceARN]

	for _, newTag := range tags {
		updated := false

		for i, t := range existing {
			if t.Key == newTag.Key {
				existing[i].Value = newTag.Value
				updated = true

				break
			}
		}

		if !updated {
			existing = append(existing, newTag)
		}
	}

	tagStore[resourceARN] = existing

	return nil
}

// UntagResource removes tags from a resource. Region is resolved from the resource ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	region := regionFromARN(resourceARN, getRegion(ctx, b.defaultRegion))
	tagStore := b.tagsStore(region)
	existing := tagStore[resourceARN]
	keySet := make(map[string]bool, len(tagKeys))

	for _, k := range tagKeys {
		keySet[k] = true
	}

	filtered := existing[:0]

	for _, t := range existing {
		if !keySet[t.Key] {
			filtered = append(filtered, t)
		}
	}

	tagStore[resourceARN] = filtered

	return nil
}

// ListTagsForResource returns tags for a resource. Region is resolved from the resource ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) ([]TagEntry, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	region := regionFromARN(resourceARN, getRegion(ctx, b.defaultRegion))

	return cloneTags(b.tags[region][resourceARN]), nil
}

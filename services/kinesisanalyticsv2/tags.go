package kinesisanalyticsv2

import (
	"context"
	"sort"
)

// TagResource adds tags to an application.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceARN string, tags []Tag) error {
	region := regionFromARN(resourceARN, b.defaultRegion)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	app := b.findByARN(region, resourceARN)
	if app == nil {
		return ErrNotFound
	}

	for _, t := range tags {
		found := false

		for i, existing := range app.Tags {
			if existing.Key == t.Key {
				app.Tags[i].Value = t.Value
				found = true

				break
			}
		}

		if !found {
			app.Tags = append(app.Tags, t)
		}
	}

	return nil
}

// UntagResource removes tags from an application.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceARN string, tagKeys []string) error {
	region := regionFromARN(resourceARN, b.defaultRegion)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	app := b.findByARN(region, resourceARN)
	if app == nil {
		return ErrNotFound
	}

	keySet := make(map[string]struct{}, len(tagKeys))
	for _, k := range tagKeys {
		keySet[k] = struct{}{}
	}

	filtered := make([]Tag, 0, len(app.Tags))
	for _, t := range app.Tags {
		if _, remove := keySet[t.Key]; !remove {
			filtered = append(filtered, t)
		}
	}

	app.Tags = filtered

	return nil
}

// ListTagsForResource returns tags for an application, sorted by key.
func (b *InMemoryBackend) ListTagsForResource(_ context.Context, resourceARN string) ([]Tag, error) {
	region := regionFromARN(resourceARN, b.defaultRegion)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	app := b.findByARN(region, resourceARN)
	if app == nil {
		return nil, ErrNotFound
	}

	cp := cloneTags(app.Tags)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Key < cp[j].Key })

	return cp, nil
}

// findByARN finds an application by its ARN using O(1) index lookup, scoped
// to region (matching the pre-Phase-3.3 per-region ARN map: an ARN whose
// embedded region does not match the caller-derived region is treated as not
// found). Must be called with lock held.
func (b *InMemoryBackend) findByARN(region, resourceARN string) *Application {
	for _, app := range b.applicationsByARN.Get(resourceARN) {
		if app.Region == region {
			return app
		}
	}

	return nil
}

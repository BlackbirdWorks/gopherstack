package codeartifact

import (
	"context"
	"fmt"
)

// TagResource adds or replaces tags on a resource by ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, kv map[string]string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	for _, d := range b.domainsByRegion.Get(region) {
		if d.ARN == resourceARN {
			d.Tags.Merge(kv)

			return nil
		}
	}
	for _, r := range b.repositoriesByRegion.Get(region) {
		if r.ARN == resourceARN {
			r.Tags.Merge(kv)

			return nil
		}
	}
	for _, pg := range b.packageGroupsByRegion.Get(region) {
		if pg.ARN == resourceARN {
			pg.Tags.Merge(kv)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, d := range b.domainsByRegion.Get(region) {
		if d.ARN == resourceARN {
			d.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}
	for _, r := range b.repositoriesByRegion.Get(region) {
		if r.ARN == resourceARN {
			r.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}
	for _, pg := range b.packageGroupsByRegion.Get(region) {
		if pg.ARN == resourceARN {
			pg.Tags.DeleteKeys(tagKeys)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// ListTagsForResource returns tags for a resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	for _, d := range b.domainsByRegion.Get(region) {
		if d.ARN == resourceARN {
			return d.Tags.Clone(), nil
		}
	}
	for _, r := range b.repositoriesByRegion.Get(region) {
		if r.ARN == resourceARN {
			return r.Tags.Clone(), nil
		}
	}
	for _, pg := range b.packageGroupsByRegion.Get(region) {
		if pg.ARN == resourceARN {
			return pg.Tags.Clone(), nil
		}
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

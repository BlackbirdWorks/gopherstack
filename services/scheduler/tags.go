package scheduler

import (
	"context"
	"fmt"
)

func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, kv map[string]string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if s := b.scheduleByARN(region, resourceARN); s != nil {
		s.Tags.Merge(kv)

		return nil
	}

	if g := b.scheduleGroupByARN(region, resourceARN); g != nil {
		g.Tags.Merge(kv)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if s := b.scheduleByARN(region, resourceARN); s != nil {
		s.Tags.DeleteKeys(tagKeys)

		return nil
	}

	if g := b.scheduleGroupByARN(region, resourceARN); g != nil {
		g.Tags.DeleteKeys(tagKeys)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if s := b.scheduleByARN(region, resourceARN); s != nil {
		return s.Tags.Clone(), nil
	}

	if g := b.scheduleGroupByARN(region, resourceARN); g != nil {
		return g.Tags.Clone(), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

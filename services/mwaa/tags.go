package mwaa

import (
	"context"
	"fmt"
	"maps"
)

// TagResource adds or updates tags on a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, tags map[string]string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	env := b.findByARN(region, resourceARN)
	if env == nil {
		return ErrEnvironmentNotFound
	}

	if env.Tags == nil {
		env.Tags = make(map[string]string)
	}

	// Project the merged tag count to enforce the per-resource limit.
	projected := maps.Clone(env.Tags)
	maps.Copy(projected, tags)

	if len(projected) > maxTagsPerResource {
		return fmt.Errorf(
			"%w: resource would have %d tags, which exceeds the limit of %d",
			ErrInvalidParameter, len(projected), maxTagsPerResource,
		)
	}

	maps.Copy(env.Tags, tags)

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	env := b.findByARN(region, resourceARN)
	if env == nil {
		return ErrEnvironmentNotFound
	}

	for _, k := range tagKeys {
		delete(env.Tags, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	env := b.findByARN(region, resourceARN)
	if env == nil {
		return nil, ErrEnvironmentNotFound
	}

	result := make(map[string]string, len(env.Tags))
	maps.Copy(result, env.Tags)

	return result, nil
}

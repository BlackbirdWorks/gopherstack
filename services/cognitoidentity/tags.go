package cognitoidentity

import (
	"context"
	"fmt"
	"maps"
)

// ListTagsForResource returns the tags for an identity pool resource by its ARN.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceARN string,
) (map[string]string, error) {
	region := regionFromARN(resourceARN, getRegion(ctx, b.region))

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	pool, ok := b.poolByARN(region, resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource %q not found", ErrIdentityPoolNotFound, resourceARN)
	}

	return cloneStringMap(pool.Tags), nil
}

// TagResource adds or updates tags on an identity pool resource by ARN.
func (b *InMemoryBackend) TagResource(
	ctx context.Context,
	resourceARN string,
	tags map[string]string,
) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	region := regionFromARN(resourceARN, getRegion(ctx, b.region))

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	pool, ok := b.poolByARN(region, resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %q not found", ErrIdentityPoolNotFound, resourceARN)
	}

	if pool.Tags == nil {
		pool.Tags = make(map[string]string)
	}

	maps.Copy(pool.Tags, tags)

	return nil
}

// UntagResource removes the given tag keys from an identity pool resource by ARN.
func (b *InMemoryBackend) UntagResource(
	ctx context.Context,
	resourceARN string,
	tagKeys []string,
) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	if len(tagKeys) == 0 {
		return fmt.Errorf("%w: TagKeys must not be empty", ErrInvalidParameter)
	}

	region := regionFromARN(resourceARN, getRegion(ctx, b.region))

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	pool, ok := b.poolByARN(region, resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %q not found", ErrIdentityPoolNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(pool.Tags, k)
	}

	return nil
}

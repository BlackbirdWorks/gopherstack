package textract

import (
	"context"
	"fmt"
	"maps"
)

// cloneTags returns a non-nil copy of a tags map.
func cloneTags(tags map[string]string) map[string]string {
	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// TagResource adds or replaces tags on an adapter or adapter version identified by ARN.
// Region is resolved from the ARN, falling back to the context region.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, tags map[string]string) error {
	region := regionFromARN(resourceARN, getRegion(ctx, b.region))

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	// Try adapter version first (ARN contains /version/).
	if av, ok := resolveARNToAdapterVersion(b.adapterVersions, region, resourceARN); ok {
		maps.Copy(av.Tags, tags)

		return nil
	}

	// Try adapter.
	if a, ok := resolveARNToAdapter(b.adapters, region, resourceARN); ok {
		maps.Copy(a.Tags, tags)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrAdapterNotFound, resourceARN)
}

// UntagResource removes the specified tag keys from an adapter or adapter version.
// Region is resolved from the ARN, falling back to the context region.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error {
	region := regionFromARN(resourceARN, getRegion(ctx, b.region))

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	// Try adapter version first.
	if av, ok := resolveARNToAdapterVersion(b.adapterVersions, region, resourceARN); ok {
		for _, k := range tagKeys {
			delete(av.Tags, k)
		}

		return nil
	}

	// Try adapter.
	if a, ok := resolveARNToAdapter(b.adapters, region, resourceARN); ok {
		for _, k := range tagKeys {
			delete(a.Tags, k)
		}

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrAdapterNotFound, resourceARN)
}

// ListTagsForResource returns a copy of the tags for an adapter or adapter version.
// Region is resolved from the ARN, falling back to the context region.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	region := regionFromARN(resourceARN, getRegion(ctx, b.region))

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	// Try adapter version first.
	if av, ok := resolveARNToAdapterVersion(b.adapterVersions, region, resourceARN); ok {
		return cloneTags(av.Tags), nil
	}

	// Try adapter.
	if a, ok := resolveARNToAdapter(b.adapters, region, resourceARN); ok {
		return cloneTags(a.Tags), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrAdapterNotFound, resourceARN)
}

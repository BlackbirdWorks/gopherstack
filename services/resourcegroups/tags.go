package resourcegroups

import (
	"context"
	"fmt"
	"strings"
)

// validateTagKeys validates that no reserved aws: prefix tag keys are present.
func validateTagKeys(tagMap map[string]string) error {
	for k := range tagMap {
		if strings.HasPrefix(strings.ToLower(k), "aws:") {
			return fmt.Errorf(
				"%w: tag key %q uses the reserved prefix \"aws:\"; these keys are managed by AWS",
				ErrValidation,
				k,
			)
		}
	}

	return nil
}

// GetTagsByARN returns the tags for the resource group identified by ARN.
func (b *InMemoryBackend) GetTagsByARN(ctx context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTagsByARN")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	g := b.findByARN(region, resourceARN)
	if g == nil {
		return nil, fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	return g.Tags.Clone(), nil
}

// AddTagsByARN merges newTags into the resource group identified by ARN and
// returns the resulting tag set. Rejects reserved aws: tag key prefixes.
func (b *InMemoryBackend) AddTagsByARN(
	ctx context.Context,
	resourceARN string,
	newTags map[string]string,
) (map[string]string, error) {
	if err := validateTagKeys(newTags); err != nil {
		return nil, err
	}

	b.mu.Lock("AddTagsByARN")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g := b.findByARN(region, resourceARN)
	if g == nil {
		return nil, fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	g.Tags.Merge(newTags)

	return g.Tags.Clone(), nil
}

// RemoveTagsByARN removes the specified tag keys from the resource group
// identified by ARN.
func (b *InMemoryBackend) RemoveTagsByARN(ctx context.Context, resourceARN string, keys []string) error {
	b.mu.Lock("RemoveTagsByARN")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g := b.findByARN(region, resourceARN)
	if g == nil {
		return fmt.Errorf("%w: group with ARN %s not found", ErrNotFound, resourceARN)
	}

	g.Tags.DeleteKeys(keys)

	return nil
}

// findByARN looks up a group by its ARN within the given region (must be called under a lock).
// An ARN uniquely identifies a group, so at most one entry is ever grouped under the index key.
func (b *InMemoryBackend) findByARN(region, resourceARN string) *Group {
	if matches := b.groupsByARN.Get(regionKey(region, resourceARN)); len(matches) > 0 {
		return matches[0]
	}

	return nil
}

package pipes

import (
	"context"
	"fmt"
	"maps"
)

func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, kv map[string]string) error {
	if err := validateTags(kv); err != nil {
		return err
	}
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	p, ok := b.pipeByARN(region, resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	merged := mergeTags(p.Tags, kv)
	if len(merged) > maxTagsPerPipe {
		return fmt.Errorf("%w: pipe would exceed %d tags limit", ErrValidation, maxTagsPerPipe)
	}
	p.Tags = merged

	return nil
}

func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	p, ok := b.pipeByARN(region, resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	for _, k := range keys {
		delete(p.Tags, k)
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	p, ok := b.pipeByARN(region, resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	result := make(map[string]string, len(p.Tags))
	maps.Copy(result, p.Tags)

	return result, nil
}

// pipeByARN looks up a pipe by ARN within region via the pipesByARN secondary
// index, returning (nil, false) if no pipe in that region has that ARN. ARNs
// are unique per pipe, so the index group has at most one entry.
func (b *InMemoryBackend) pipeByARN(region, resourceARN string) (*Pipe, bool) {
	list := b.pipesByARNIndex(region).Get(resourceARN)
	if len(list) == 0 {
		return nil, false
	}

	return list[0], true
}

func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}

func validateTags(tags map[string]string) error {
	for k, v := range tags {
		if len(k) == 0 {
			return fmt.Errorf("%w: tag key must not be empty", ErrValidation)
		}
		if len(k) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key %q exceeds maximum length of %d",
				ErrValidation,
				k,
				maxTagKeyLen,
			)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value for key %q exceeds maximum length of %d",
				ErrValidation,
				k,
				maxTagValueLen,
			)
		}
	}

	return nil
}

package applicationautoscaling

import (
	"fmt"
	"maps"
)

// TagResource adds or updates tags on a scalable target identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	group := b.targetsByARN.Get(resourceARN)
	if len(group) == 0 {
		return withResourceName(
			fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceARN),
			resourceARN,
		)
	}

	t := group[0]

	if t.Tags == nil {
		t.Tags = make(map[string]string)
	}

	if err := mergeTags(t.Tags, kv, ErrTooManyTags); err != nil {
		return withResourceName(err, resourceARN)
	}

	return nil
}

// ListTagsForResource returns tags for a scalable target identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	group := b.targetsByARN.Get(resourceARN)
	if len(group) == 0 {
		return nil, withResourceName(
			fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceARN),
			resourceARN,
		)
	}

	t := group[0]
	out := make(map[string]string, len(t.Tags))
	maps.Copy(out, t.Tags)

	return out, nil
}

// UntagResource removes tags from a scalable target identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	group := b.targetsByARN.Get(resourceARN)
	if len(group) == 0 {
		return withResourceName(
			fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceARN),
			resourceARN,
		)
	}

	t := group[0]

	for _, k := range tagKeys {
		delete(t.Tags, k)
	}

	return nil
}

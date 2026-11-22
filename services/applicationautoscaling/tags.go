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
//
// Unlike TagResource/UntagResource, ListTagsForResource's own
// deserializeOpError switch in the vendored SDK types only
// ResourceNotFoundException -- it has no ValidationException case -- so an
// empty ResourceARN reports ResourceNotFoundException here instead of
// ErrValidation, matching the modeled error surface rather than a code the
// op's own switch cannot type.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
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

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every scalable target that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(true)
}

// InventoryResources lists every scalable target, tagged or not.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(false)
}

func (b *InMemoryBackend) entries(taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	all := b.scalableTargets.All()
	out := make([]TaggedEntry, 0, len(all))

	for _, t := range all {
		if !taggedOnly || len(t.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: t.ARN, Tags: maps.Clone(t.Tags)})
		}
	}

	return out
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

package inspector2

import (
	"fmt"
	"maps"
)

// AWS Inspector2 tag limits.
const (
	maxTagKeyLen   = 128
	maxTagValueLen = 256
	maxTagCount    = 50
)

// validateTags enforces AWS tag limits: key 1-128 chars, value 0-256 chars, max 50 tags.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot specify more than %d tags", ErrValidation, maxTagCount)
	}

	for k, v := range tags {
		if k == "" || len(k) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key must be between 1 and %d characters",
				ErrValidation,
				maxTagKeyLen,
			)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value must be at most %d characters",
				ErrValidation,
				maxTagValueLen,
			)
		}
	}

	return nil
}

// TagResource adds or replaces tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrTagsResourceNotFound
	}

	existing := b.tags[resourceARN]
	if len(existing)+len(tags) > maxTagCount {
		return fmt.Errorf(
			"%w: resource would exceed maximum of %d tags",
			ErrValidation,
			maxTagCount,
		)
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	if f, ok := b.filters.Get(resourceARN); ok {
		if f.Tags == nil {
			f.Tags = make(map[string]string)
		}

		maps.Copy(f.Tags, tags)
	}

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrTagsResourceNotFound
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)

		if f, ok := b.filters.Get(resourceARN); ok {
			delete(f.Tags, k)
		}
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.resourceExists(resourceARN) {
		return nil, ErrTagsResourceNotFound
	}

	result := make(map[string]string, len(b.tags[resourceARN]))
	maps.Copy(result, b.tags[resourceARN])

	return result, nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every filter plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))
	seen := make(map[string]struct{}, len(b.tags))

	for key, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, TaggedEntry{ARN: key, Tags: maps.Clone(t)})
	}

	if withUntagged {
		for _, key := range b.resourceARNsLocked() {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				out = append(out, TaggedEntry{ARN: key})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) resourceARNsLocked() []string {
	out := make([]string, 0, b.filters.Len())

	for _, f := range b.filters.All() {
		out = append(out, f.Arn)
	}

	return out
}

// resourceExists returns true if the ARN corresponds to a known resource.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) resourceExists(resourceARN string) bool {
	if b.filters.Has(resourceARN) {
		return true
	}

	// Accept any previously tagged ARN (including account-level ARNs).
	_, tagged := b.tags[resourceARN]

	return tagged
}

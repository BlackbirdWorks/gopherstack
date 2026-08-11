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

// TaggedResources returns every Inspector2 resource ARN that currently has
// at least one tag applied via TagResource. In practice this is always a
// filter ARN: resourceExists only recognizes filters (or an ARN already
// present in b.tags), and only CreateFilter seeds b.tags at creation time.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for resourceARN, tags := range b.tags {
		if len(tags) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: resourceARN, Tags: maps.Clone(tags)})
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

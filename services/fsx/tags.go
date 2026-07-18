package fsx

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return ErrResourceNotFound
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	existing := b.tags[resourceARN]
	newKeys := 0
	for _, t := range tags {
		if _, ok := existing[t.Key]; !ok {
			newKeys++
		}
	}

	if len(existing)+newKeys > maxTagsPerResource {
		return fmt.Errorf("%w: adding %d tag(s) would exceed the %d-tag limit",
			ErrTagLimitExceeded, newKeys, maxTagsPerResource)
	}

	for _, t := range tags {
		existing[t.Key] = t.Value
	}

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return ErrResourceNotFound
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns the tags on a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.arnExists(resourceARN) {
		return nil, ErrResourceNotFound
	}

	return tagsMapToSlice(b.tags[resourceARN]), nil
}

// validateTags returns ErrTagInvalid if any tag key or value violates FSx constraints:
// key must be 1–128 chars and must not start with "aws:"; value must be 0–256 chars.
func validateTags(tags []Tag) error {
	for _, t := range tags {
		klen := utf8.RuneCountInString(t.Key)
		if klen == 0 || klen > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be 1–%d chars, got %d", ErrTagInvalid, maxTagKeyLen, klen)
		}

		if strings.HasPrefix(strings.ToLower(t.Key), "aws:") {
			return fmt.Errorf("%w: tag key must not start with reserved prefix \"aws:\"", ErrTagInvalid)
		}

		if vlen := utf8.RuneCountInString(t.Value); vlen > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be 0–%d chars, got %d", ErrTagInvalid, maxTagValueLen, vlen)
		}
	}

	return nil
}

func tagsSliceToMap(tags []Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

func tagsMapToSlice(m map[string]string) []Tag {
	if len(m) == 0 {
		return nil
	}

	keys := collections.SortedKeys(m)

	tags := make([]Tag, len(keys))
	for i, k := range keys {
		tags[i] = Tag{Key: k, Value: m[k]}
	}

	return tags
}

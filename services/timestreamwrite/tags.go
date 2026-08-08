package timestreamwrite

import (
	"fmt"
	"maps"
)

// TagResource stores tags for the given ARN.
// It accepts database, table, and scheduled-query ARNs because the Timestream
// Write and Query services share a single TagResource endpoint.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownARNLocked(arn) {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, arn)
	}

	if b.tags[arn] == nil {
		b.tags[arn] = make(map[string]string)
	}

	maps.Copy(b.tags[arn], tags)

	return nil
}

// UntagResource removes tag keys from the given ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.tags[arn] == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(b.tags[arn], k)
	}

	return nil
}

// ListTagsForResource returns tags for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	result := make(map[string]string, len(b.tags[arn]))
	maps.Copy(result, b.tags[arn])

	return result
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every Timestream Write database or table ARN that
// currently has at least one tag applied via TagResource.
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

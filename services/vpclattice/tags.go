package vpclattice

import (
	"maps"
)

// ------- Tagging operations -------

// TagResource adds tags to a resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.tags[resourceArn]; !ok {
		b.tags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if t, ok := b.tags[resourceArn]; ok {
		for _, k := range keys {
			delete(t, k)
		}
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, ok := b.tags[resourceArn]
	if !ok {
		return make(map[string]string), nil
	}

	result := make(map[string]string, len(t))
	maps.Copy(result, t)

	return result, nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every VPC Lattice resource ARN that currently has
// at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for arn, tags := range b.tags {
		if len(tags) == 0 {
			continue
		}

		cp := make(map[string]string, len(tags))
		maps.Copy(cp, tags)
		out = append(out, TaggedEntry{ARN: arn, Tags: cp})
	}

	return out
}

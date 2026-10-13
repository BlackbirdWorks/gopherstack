package iot

import "maps"

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every resource that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))

	for resourceARN, m := range b.resourceTags {
		if len(m) > 0 {
			out = append(out, TaggedEntry{ARN: resourceARN, Tags: maps.Clone(m)})
		}
	}

	return out
}

// TagResource adds tags to resourceARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	return b.TagResourceGeneric(resourceARN, tags)
}

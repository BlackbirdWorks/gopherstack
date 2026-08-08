package mediaconvert

import "maps"

// GetTags returns a copy of tags for the given resource ARN.
func (b *InMemoryBackend) GetTags(resourceARN string) map[string]string {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	t := b.tags[resourceARN]
	cp := make(map[string]string, len(t))

	maps.Copy(cp, t)

	return cp
}

// TagResource adds or updates tags for the given resource ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	b.storeTagsLocked(resourceARN, tags)
}

// UntagResource removes the specified tag keys from the resource ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	if len(b.tags[resourceARN]) == 0 {
		delete(b.tags, resourceARN)
	}
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every MediaConvert resource ARN that currently has
// at least one tag applied via TagResource.
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

// storeTagsLocked merges tags into the ARN tag map.
// Caller must hold the write lock.
func (b *InMemoryBackend) storeTagsLocked(resourceARN string, tags map[string]string) {
	if len(tags) == 0 {
		return
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string, len(tags))
	}

	maps.Copy(b.tags[resourceARN], tags)
}

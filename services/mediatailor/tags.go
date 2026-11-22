package mediatailor

import (
	"maps"
)

// --- Tag operations ---

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing := b.tags[resourceARN]
	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

// TagResource adds tags to a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tag keys from a resource.
// Idempotent: returns nil if the resource has no tags.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
	if existing == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
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

// InventoryResources lists every playback configuration, channel, source location, VOD source, live source,
// prefetch schedule, program and function plus every tagged ARN of the tag store.
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
		out = append(out, TaggedEntry{ARN: key, Tags: copyTags(t)})
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
	out := make(
		[]string,
		0,
		b.playbackConfigurations.Len()+b.channels.Len()+b.sourceLocations.Len()+b.vodSources.Len()+
			b.liveSources.Len()+b.prefetchSchedules.Len()+b.programs.Len()+b.functions.Len(),
	)

	for _, v := range b.playbackConfigurations.All() {
		out = append(out, v.PlaybackConfigurationARN)
	}

	for _, v := range b.channels.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.sourceLocations.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.vodSources.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.liveSources.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.prefetchSchedules.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.programs.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.functions.All() {
		out = append(out, v.ARN)
	}

	return out
}

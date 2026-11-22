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

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every queue, job template, preset and job plus every tagged ARN of the tag store.
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
	out := make([]string, 0, b.queues.Len()+b.jobTemplates.Len()+b.presets.Len()+b.jobs.Len())

	for _, v := range b.queues.All() {
		out = append(out, v.Arn)
	}

	for _, v := range b.jobTemplates.All() {
		out = append(out, v.Arn)
	}

	for _, v := range b.presets.All() {
		out = append(out, v.Arn)
	}

	for _, v := range b.jobs.All() {
		out = append(out, v.Arn)
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

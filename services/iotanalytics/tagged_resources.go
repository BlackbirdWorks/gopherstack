package iotanalytics

import "maps"

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every channel, datastore, dataset and pipeline plus every tagged ARN of the tag store.
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
	out := make([]string, 0, b.channels.Len()+b.datastores.Len()+b.datasets.Len()+b.pipelines.Len())

	for _, v := range b.channels.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.datastores.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.datasets.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.pipelines.All() {
		out = append(out, v.ARN)
	}

	return out
}

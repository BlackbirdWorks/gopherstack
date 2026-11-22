package cognitoidp

import "maps"

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.resourceTags[arn] == nil {
		b.resourceTags[arn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[arn], tags)
}

// UntagResource removes tag keys from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.resourceTags[arn] == nil {
		return
	}

	for _, k := range tagKeys {
		delete(b.resourceTags[arn], k)
	}
}

// ListTagsForResource returns a copy of the tag map for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	return maps.Clone(b.resourceTags[arn])
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingCognitoIDP).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every user pool and user pool replica plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.resourceTags))
	seen := make(map[string]struct{}, len(b.resourceTags))

	for key, t := range b.resourceTags {
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
	out := make([]string, 0, b.pools.Len()+b.userPoolReplicas.Len())

	for _, v := range b.pools.All() {
		out = append(out, v.ARN)
	}

	for _, v := range b.userPoolReplicas.All() {
		out = append(out, v.ARN)
	}

	return out
}

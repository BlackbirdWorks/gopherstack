package opsworks

import (
	"maps"
	"sort"
)

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrStackNotFound
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrStackNotFound
	}

	existing := b.tags[resourceARN]
	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTags lists tags for a resource with pagination support.
func (b *InMemoryBackend) ListTags(
	resourceARN string,
	maxResults int32,
	nextToken string,
) (map[string]string, string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	if !b.resourceExists(resourceARN) {
		return nil, "", ErrStackNotFound
	}

	allTags := b.tags[resourceARN]

	// Build a sorted list of keys for deterministic pagination.
	keys := make([]string, 0, len(allTags))
	for k := range allTags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Determine start index from nextToken.
	startIdx := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				startIdx = i

				break
			}
		}
	}

	// Apply maxResults limit.
	limit := len(keys) - startIdx
	if maxResults > 0 && int(maxResults) < limit {
		limit = int(maxResults)
	}

	result := make(map[string]string, limit)
	for i := startIdx; i < startIdx+limit; i++ {
		result[keys[i]] = allTags[keys[i]]
	}

	// Compute next token.
	outToken := ""
	if startIdx+limit < len(keys) {
		outToken = keys[startIdx+limit]
	}

	return result, outToken, nil
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

// InventoryResources lists every stack and layer plus every tagged ARN of the tag store.
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
	out := make([]string, 0, b.stacks.Len()+b.layers.Len())

	for _, s := range b.stacks.All() {
		out = append(out, s.Arn)
	}

	for _, l := range b.layers.All() {
		out = append(out, b.layerARN(l.LayerID))
	}

	return out
}

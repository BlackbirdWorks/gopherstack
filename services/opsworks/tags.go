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

// TaggedResources returns every stack or layer ARN with at least one tag,
// for the resourcegroupstaggingapi integration (cli.go's wireTaggingOpsWorks).
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for resourceARN, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		tagsCopy := make(map[string]string, len(t))
		maps.Copy(tagsCopy, t)
		out = append(out, TaggedEntry{ARN: resourceARN, Tags: tagsCopy})
	}

	return out
}

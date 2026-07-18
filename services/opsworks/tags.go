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

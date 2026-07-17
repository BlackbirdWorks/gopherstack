package datasync

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownResource(resourceArn) {
		return ErrNotFound
	}

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceArn], tags)

	if a, ok := b.agents.Get(resourceArn); ok {
		if a.Tags == nil {
			a.Tags = make(map[string]string)
		}
		maps.Copy(a.Tags, tags)
	}

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.isKnownResource(resourceArn) {
		return ErrNotFound
	}

	for _, k := range keys {
		delete(b.tags[resourceArn], k)
	}

	if a, ok := b.agents.Get(resourceArn); ok {
		for _, k := range keys {
			delete(a.Tags, k)
		}
	}

	return nil
}

// ListTagsForResource returns tags for a resource with pagination.
func (b *InMemoryBackend) ListTagsForResource(
	resourceArn string,
	maxResults int32,
	nextToken string,
) (map[string]string, string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.isKnownResource(resourceArn) {
		return nil, "", ErrNotFound
	}

	// Build sorted key list for stable pagination.
	tagMap := b.tags[resourceArn]
	keys := collections.SortedKeys(tagMap)

	type tagEntry struct {
		key   string
		value string
	}

	all := make([]tagEntry, 0, len(keys))
	for _, k := range keys {
		all = append(all, tagEntry{k, tagMap[k]})
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	result := make(map[string]string, len(pg.Data))
	for _, e := range pg.Data {
		result[e.key] = e.value
	}

	return result, pg.Next, nil
}

// isKnownResource returns true if the ARN corresponds to a known agent, location, or task.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) isKnownResource(a string) bool {
	return b.agents.Has(a) || b.locations.Has(a) || b.tasks.Has(a)
}

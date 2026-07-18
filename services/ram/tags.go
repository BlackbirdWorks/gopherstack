package ram

import (
	"fmt"
	"maps"
)

// TagResource adds or updates tags on a resource share identified by ARN.
func (b *InMemoryBackend) TagResource(shareARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares.Get(shareARN)
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	rs.Tags = mergeTags(rs.Tags, kv)

	return nil
}

// UntagResource removes specified tag keys from a resource share.
func (b *InMemoryBackend) UntagResource(shareARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares.Get(shareARN)
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	for _, k := range keys {
		delete(rs.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource share identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(shareARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	rs, ok := b.resourceShares.Get(shareARN)
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	result := make(map[string]string, len(rs.Tags))
	maps.Copy(result, rs.Tags)

	return result, nil
}

// mergeTags merges new tags into existing ones, returning a new map.
func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}

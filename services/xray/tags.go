package xray

import (
	"maps"
)

// TagResource adds or updates tags on a resource identified by ARN.
// Tags are stored in a per-ARN map on the backend.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.resourceTags == nil {
		b.resourceTags = make(map[string]map[string]string)
	}

	existing, ok := b.resourceTags[resourceARN]
	if !ok {
		existing = make(map[string]string)
		b.resourceTags[resourceARN] = existing
	}

	maps.Copy(existing, tags)
}

// UntagResource removes the specified tag keys from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.resourceTags == nil {
		return
	}

	existing := b.resourceTags[resourceARN]
	for _, k := range tagKeys {
		delete(existing, k)
	}
}

// ListTagsForResource returns all tags for the given resource ARN as a slice of key/value maps.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) []map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if b.resourceTags == nil {
		return []map[string]string{}
	}

	tags := b.resourceTags[resourceARN]
	out := make([]map[string]string, 0, len(tags))

	for k, v := range tags {
		out = append(out, map[string]string{"Key": k, "Value": v})
	}

	return out
}

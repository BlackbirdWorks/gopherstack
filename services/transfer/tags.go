package transfer

import (
	"maps"
)

// TagResource applies tags to a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.tagsStore[resourceARN]; !ok {
		b.tagsStore[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tagsStore[resourceARN], tags)

	return nil
}

// UntagResource removes tag keys from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if existing, ok := b.tagsStore[resourceARN]; ok {
		for _, k := range tagKeys {
			delete(existing, k)
		}
	}

	return nil
}

// ListTagsForResource returns tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.tagsStore[resourceARN]
	if !ok {
		return make(map[string]string)
	}

	out := make(map[string]string, len(existing))
	maps.Copy(out, existing)

	return out
}

// initTagsStore seeds tagsStore[resourceARN] with creation-time tags so that
// ListTagsForResource returns them even before any TagResource call.
// Caller must hold b.mu (write lock).
func (b *InMemoryBackend) initTagsStore(resourceARN string, tags map[string]string) {
	if len(tags) == 0 {
		return
	}

	if _, ok := b.tagsStore[resourceARN]; !ok {
		b.tagsStore[resourceARN] = make(map[string]string, len(tags))
	}

	maps.Copy(b.tagsStore[resourceARN], tags)
}

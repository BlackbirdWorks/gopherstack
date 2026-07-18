package bedrockagent

import (
	"context"
	"maps"
)

// ---------------------------------------------------------------------------
// Tagging operations
// ---------------------------------------------------------------------------

// ListTagsForResource returns tags for a resource ARN.
func (b *InMemoryBackend) ListTagsForResource(
	_ context.Context, resourceARN string,
) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, ok := b.tags[resourceARN]
	if !ok {
		return map[string]string{}, nil
	}

	return maps.Clone(t), nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(
	_ context.Context, resourceARN string, tags map[string]string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(
	_ context.Context, resourceARN string, tagKeys []string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	t := b.tags[resourceARN]

	for _, k := range tagKeys {
		delete(t, k)
	}

	return nil
}

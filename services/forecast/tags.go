package forecast

import (
	"fmt"
	"maps"
)

// TagResource adds tags to a resource. Real Amazon Forecast returns
// ResourceNotFoundException when resourceARN does not identify an existing
// resource -- TagResource does not silently create tag state for ARNs no
// resource ever owned.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.arnIndex[resourceARN]; !ok {
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceARN)
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource. Real Amazon Forecast returns
// ResourceNotFoundException when resourceARN does not identify an existing
// resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.arnIndex[resourceARN]; !ok {
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceARN)
	}

	if b.tags[resourceARN] != nil {
		for _, k := range tagKeys {
			delete(b.tags[resourceARN], k)
		}
	}

	return nil
}

// ListTagsForResource lists tags for a resource. Real Amazon Forecast returns
// ResourceNotFoundException when resourceARN does not identify an existing
// resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.arnIndex[resourceARN]; !ok {
		return nil, fmt.Errorf("%w: resource %q", ErrNotFound, resourceARN)
	}

	result := make(map[string]string)
	maps.Copy(result, b.tags[resourceARN])

	return result, nil
}

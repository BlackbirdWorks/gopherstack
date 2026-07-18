package appstream

import "maps"

// TagResource applies tags to an AppStream resource ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.isKnownARN(arn) {
		return ErrNotFound
	}

	if b.tags[arn] == nil {
		b.tags[arn] = make(map[string]string)
	}

	maps.Copy(b.tags[arn], tags)

	return nil
}

// UntagResource removes tags from an AppStream resource ARN.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.isKnownARN(arn) {
		return ErrNotFound
	}

	for _, k := range keys {
		delete(b.tags[arn], k)
	}

	return nil
}

// ListTagsForResource returns tags for an AppStream resource ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.isKnownARN(arn) {
		return nil, ErrNotFound
	}

	result := make(map[string]string)
	maps.Copy(result, b.tags[arn])

	return result, nil
}

// isKnownARN returns true if the ARN corresponds to a known resource.
// Must be called with at least a read lock held.
func (b *InMemoryBackend) isKnownARN(arn string) bool {
	_, ok := b.tags[arn]

	return ok
}

package codecommit

import "fmt"

// TagResource adds or replaces tags on a repository by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	name, ok := b.repositoriesByARN[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	r, _ := b.repositories.Get(name)
	r.Tags.Merge(kv)

	return nil
}

// UntagResource removes tags from a repository by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	name, ok := b.repositoriesByARN[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	r, _ := b.repositories.Get(name)
	r.Tags.DeleteKeys(tagKeys)

	return nil
}

// ListTagsForResource returns tags for a repository by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	name, ok := b.repositoriesByARN[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	r, _ := b.repositories.Get(name)

	return r.Tags.Clone(), nil
}

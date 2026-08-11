package opensearch

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// findTaggableByARN resolves the *tags.Tags of the domain or application
// identified by resourceARN. Real OpenSearch ListTags/AddTags/RemoveTags
// accept both domain and application ARNs.
func (b *InMemoryBackend) findTaggableByARN(resourceARN string) *tags.Tags {
	if d := b.findDomainByARN(resourceARN); d != nil {
		return d.Tags
	}

	if matches := b.applicationsByARN.Get(resourceARN); len(matches) > 0 {
		return matches[0].Tags
	}

	return nil
}

// ListTags returns tags for the domain or application identified by ARN.
func (b *InMemoryBackend) ListTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	t := b.findTaggableByARN(resourceARN)
	if t == nil {
		return nil, fmt.Errorf("%w: resource not found for ARN %s", ErrDomainNotFound, resourceARN)
	}

	return t.Clone(), nil
}

// AddTags adds or updates tags on the domain or application identified by ARN.
func (b *InMemoryBackend) AddTags(resourceARN string, kv map[string]string) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	t := b.findTaggableByARN(resourceARN)
	if t == nil {
		return fmt.Errorf("%w: resource not found for ARN %s", ErrDomainNotFound, resourceARN)
	}

	t.Merge(kv)

	return nil
}

// RemoveTags removes tag keys from the domain or application identified by ARN.
func (b *InMemoryBackend) RemoveTags(resourceARN string, keys []string) error {
	b.mu.Lock("RemoveTags")
	defer b.mu.Unlock()

	t := b.findTaggableByARN(resourceARN)
	if t == nil {
		return fmt.Errorf("%w: resource not found for ARN %s", ErrDomainNotFound, resourceARN)
	}

	t.DeleteKeys(keys)

	return nil
}

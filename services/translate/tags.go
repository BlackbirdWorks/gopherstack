package translate

import (
	"fmt"
	"maps"
)

// TagResource adds or replaces tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, newTags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	if tooManyTags(b.tags[resourceARN], newTags) {
		return fmt.Errorf(
			"%w: resource %q would exceed the %d-tag limit",
			ErrTooManyTags,
			resourceARN,
			maxTagsPerResource,
		)
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], newTags)

	return nil
}

// UntagResource removes specific tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.arnExists(resourceARN) {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	for _, k := range keys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.arnExists(resourceARN) {
		return nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceARN)
	}

	return copyMap(b.tags[resourceARN]), nil
}

// arnExists checks whether the ARN corresponds to any stored resource.
func (b *InMemoryBackend) arnExists(resourceARN string) bool {
	found := false

	b.terminologies.Range(func(t *Terminology) bool {
		if t.ARN == resourceARN {
			found = true

			return false
		}

		return true
	})

	if found {
		return true
	}

	b.parallelData.Range(func(pd *ParallelData) bool {
		if pd.ARN == resourceARN {
			found = true

			return false
		}

		return true
	})

	return found
}

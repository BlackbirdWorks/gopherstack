package polly

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// TagResource adds tags to known task ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags []Tag) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	current, ok := b.tags[resourceArn]
	if !ok {
		return fmt.Errorf("%w: resource %q not found", ErrResourceNotFound, resourceArn)
	}
	if len(current)+len(tags) > maxTagCount {
		return fmt.Errorf("%w: resource would exceed %d tag limit", ErrValidation, maxTagCount)
	}
	for _, tag := range tags {
		current[tag.Key] = tag.Value
	}

	return nil
}

// UntagResource removes tag keys from known task ARN.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	current, ok := b.tags[resourceArn]
	if !ok {
		return fmt.Errorf("%w: resource %q not found", ErrResourceNotFound, resourceArn)
	}
	for _, key := range keys {
		delete(current, key)
	}

	return nil
}

// ListTagsForResource returns sorted task tags.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) ([]Tag, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	current, ok := b.tags[resourceArn]
	if !ok {
		return nil, fmt.Errorf("%w: resource %q not found", ErrResourceNotFound, resourceArn)
	}

	keys := collections.SortedKeys(current)

	out := make([]Tag, 0, len(keys))
	for _, key := range keys {
		out = append(out, Tag{Key: key, Value: current[key]})
	}

	return out, nil
}

func validateTags(tags []Tag) error {
	seen := make(map[string]bool, len(tags))
	for _, tag := range tags {
		if tag.Key == "" || len(tag.Key) > maxTagKeyLen || seen[tag.Key] {
			return fmt.Errorf(
				"%w: tag keys must be non-empty, unique, and at most %d characters",
				ErrValidation, maxTagKeyLen,
			)
		}
		if len(tag.Value) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrValidation, maxTagValueLen)
		}
		seen[tag.Key] = true
	}

	return nil
}

package ecs

import "fmt"

// resourceTagKey returns the tags-map key for a resource ARN.
func resourceTagKey(resourceArn string) string { return resourceArn }

// copyTags returns a deep copy of the given tag slice.
func copyTags(tags []Tag) []Tag {
	if tags == nil {
		return nil
	}

	out := make([]Tag, len(tags))
	copy(out, tags)

	return out
}

// TagResource applies key/value tags to a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags []Tag) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: resourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	b.setResourceTagsLocked(resourceArn, tags)

	return nil
}

// setResourceTagsLocked merges tags into the resourceTags side map for a
// resource ARN, overwriting any existing tag with the same key. Shared by
// TagResource and resource-creation ops (for example CreateCluster) that
// accept an initial set of tags while already holding the write lock. Must be
// called with the write lock held.
func (b *InMemoryBackend) setResourceTagsLocked(resourceArn string, tags []Tag) {
	if b.resourceTags == nil {
		b.resourceTags = make(map[string][]Tag)
	}

	existing := b.resourceTags[resourceTagKey(resourceArn)]
	// Merge: existing tags with same key are overwritten.
	tagMap := make(map[string]string, len(existing)+len(tags))

	for _, t := range existing {
		tagMap[t.Key] = t.Value
	}

	for _, t := range tags {
		tagMap[t.Key] = t.Value
	}

	merged := make([]Tag, 0, len(tagMap))

	for k, v := range tagMap {
		merged = append(merged, Tag{Key: k, Value: v})
	}

	b.resourceTags[resourceTagKey(resourceArn)] = merged
}

// UntagResource removes tags with the given keys from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: resourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.resourceTags == nil {
		return nil
	}

	key := resourceTagKey(resourceArn)
	existing := b.resourceTags[key]

	if len(existing) == 0 {
		return nil
	}

	keySet := make(map[string]struct{}, len(tagKeys))

	for _, k := range tagKeys {
		keySet[k] = struct{}{}
	}

	filtered := existing[:0]

	for _, t := range existing {
		if _, remove := keySet[t.Key]; !remove {
			filtered = append(filtered, t)
		}
	}

	b.resourceTags[key] = filtered

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) ([]Tag, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", ErrInvalidParameter)
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if b.resourceTags == nil {
		return []Tag{}, nil
	}

	tags := b.resourceTags[resourceTagKey(resourceArn)]

	out := make([]Tag, len(tags))
	copy(out, tags)

	return out, nil
}

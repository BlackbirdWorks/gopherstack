package iotwireless

import "maps"

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.resourceTags[arn]; !ok {
		b.resourceTags[arn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[arn], tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
// If all tags are removed the empty map entry is cleaned up to prevent memory leaks.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.resourceTags[arn]; !ok {
		return nil
	}

	for _, k := range tagKeys {
		delete(b.resourceTags[arn], k)
	}

	if len(b.resourceTags[arn]) == 0 {
		delete(b.resourceTags, arn)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tags, ok := b.resourceTags[arn]
	if !ok {
		return map[string]string{}, nil
	}

	result := make(map[string]string, len(tags))
	maps.Copy(result, tags)

	return result, nil
}

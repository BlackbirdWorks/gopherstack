package cognitoidp

import "maps"

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.resourceTags[arn] == nil {
		b.resourceTags[arn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[arn], tags)
}

// UntagResource removes tag keys from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.resourceTags[arn] == nil {
		return
	}

	for _, k := range tagKeys {
		delete(b.resourceTags[arn], k)
	}
}

// ListTagsForResource returns a copy of the tag map for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	return maps.Clone(b.resourceTags[arn])
}

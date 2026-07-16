package s3control

import "maps"

// ---- Resource Tags ----

// ListTagsForResource returns all tags for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags := b.resourceTags[arn]
	if tags == nil {
		return map[string]string{}
	}

	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// TagResource adds or updates tags on the given ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.resourceTags[arn]
	if existing == nil {
		existing = make(map[string]string, len(tags))
	}

	maps.Copy(existing, tags)
	b.resourceTags[arn] = existing
}

// UntagResource removes specific tag keys from the given ARN.
func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	tags := b.resourceTags[arn]
	if tags == nil {
		return
	}

	for _, k := range tagKeys {
		delete(tags, k)
	}
}

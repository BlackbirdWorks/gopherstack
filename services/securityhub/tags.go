package securityhub

import "maps"

func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if existing, ok := b.tags[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(existing, k)
		}
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	result := make(map[string]string)

	if existing, ok := b.tags[resourceArn]; ok {
		maps.Copy(result, existing)
	}

	return result, nil
}

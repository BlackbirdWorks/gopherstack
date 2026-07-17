package sesv2

import "maps"

func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.resourceTags[arn] == nil {
		b.resourceTags[arn] = make(map[string]string)
	}

	maps.Copy(b.resourceTags[arn], tags)

	return nil
}

func (b *InMemoryBackend) UntagResource(arn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	m := b.resourceTags[arn]
	for _, k := range tagKeys {
		delete(m, k)
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	out := make(map[string]string, len(b.resourceTags[arn]))
	maps.Copy(out, b.resourceTags[arn])

	return out, nil
}

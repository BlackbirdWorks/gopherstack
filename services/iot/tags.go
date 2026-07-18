package iot

import "maps"

func (b *InMemoryBackend) TagResourceGeneric(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.resourceTags[resourceARN] == nil {
		b.resourceTags[resourceARN] = make(map[string]string)
	}
	maps.Copy(b.resourceTags[resourceARN], tags)

	return nil
}

func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if tags, ok := b.resourceTags[resourceARN]; ok {
		for _, k := range tagKeys {
			delete(tags, k)
		}
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(resourceARN string) map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tags := b.resourceTags[resourceARN]
	if tags == nil {
		return map[string]string{}
	}
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out
}

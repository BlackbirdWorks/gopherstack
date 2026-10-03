package kafkaconnect

import "maps"

// TagResource adds or replaces tags on a connector, custom plugin, or worker configuration by ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if c, ok := b.connectors.Get(resourceArn); ok {
		maps.Copy(c.Tags, tags)

		return nil
	}

	if p, ok := b.customPlugins.Get(resourceArn); ok {
		maps.Copy(p.Tags, tags)

		return nil
	}

	if w, ok := b.workerConfigurations.Get(resourceArn); ok {
		maps.Copy(w.Tags, tags)

		return nil
	}

	return ErrResourceNotFound
}

// UntagResource removes tags from a connector, custom plugin, or worker configuration by ARN.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if c, ok := b.connectors.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(c.Tags, k)
		}

		return nil
	}

	if p, ok := b.customPlugins.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(p.Tags, k)
		}

		return nil
	}

	if w, ok := b.workerConfigurations.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(w.Tags, k)
		}

		return nil
	}

	return ErrResourceNotFound
}

// ListTagsForResource returns all tags on a connector, custom plugin, or worker configuration by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if c, ok := b.connectors.Get(resourceArn); ok {
		return maps.Clone(c.Tags), nil
	}

	if p, ok := b.customPlugins.Get(resourceArn); ok {
		return maps.Clone(p.Tags), nil
	}

	if w, ok := b.workerConfigurations.Get(resourceArn); ok {
		return maps.Clone(w.Tags), nil
	}

	return nil, ErrResourceNotFound
}

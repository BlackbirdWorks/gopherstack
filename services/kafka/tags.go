package kafka

import (
	"context"
	"maps"
)

// TagResource adds tags to a cluster, configuration, replicator, VPC
// connection, or channel by ARN.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if c, ok := b.clusters.Get(resourceArn); ok {
		maps.Copy(c.Tags, tags)

		return nil
	}

	if c, ok := b.configurations.Get(resourceArn); ok {
		maps.Copy(c.Tags, tags)

		return nil
	}

	if r, ok := b.replicators.Get(resourceArn); ok {
		maps.Copy(r.Tags, tags)

		return nil
	}

	if v, ok := b.vpcConnections.Get(resourceArn); ok {
		maps.Copy(v.Tags, tags)

		return nil
	}

	if ch, ok := b.channels.Get(resourceArn); ok {
		maps.Copy(ch.Tags, tags)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a cluster, configuration, replicator, VPC
// connection, or channel by ARN.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if c, ok := b.clusters.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(c.Tags, k)
		}

		return nil
	}

	if c, ok := b.configurations.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(c.Tags, k)
		}

		return nil
	}

	if r, ok := b.replicators.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	}

	if v, ok := b.vpcConnections.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(v.Tags, k)
		}

		return nil
	}

	if ch, ok := b.channels.Get(resourceArn); ok {
		for _, k := range tagKeys {
			delete(ch.Tags, k)
		}

		return nil
	}

	return ErrNotFound
}

// GetTags retrieves tags for a cluster, configuration, replicator, VPC
// connection, or channel by ARN.
func (b *InMemoryBackend) GetTags(_ context.Context, resourceArn string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	if c, ok := b.clusters.Get(resourceArn); ok {
		return maps.Clone(c.Tags), nil
	}

	if c, ok := b.configurations.Get(resourceArn); ok {
		return maps.Clone(c.Tags), nil
	}

	if r, ok := b.replicators.Get(resourceArn); ok {
		return maps.Clone(r.Tags), nil
	}

	if v, ok := b.vpcConnections.Get(resourceArn); ok {
		return maps.Clone(v.Tags), nil
	}

	if ch, ok := b.channels.Get(resourceArn); ok {
		return maps.Clone(ch.Tags), nil
	}

	return nil, ErrNotFound
}

package kafka

import (
	"context"
	"fmt"
	"maps"
	"slices"
)

// CreateTopic creates a topic on an MSK cluster.
func (b *InMemoryBackend) CreateTopic(
	_ context.Context,
	clusterArn, topicName string,
	replicationFactor, numPartitions int32,
	configEntries map[string]string,
) (*Topic, error) {
	if topicName == "" {
		return nil, fmt.Errorf("topicName is required: %w", ErrValidation)
	}

	b.mu.Lock("CreateTopic")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	key := topicKey(clusterArn, topicName)
	if b.topics.Has(key) {
		return nil, ErrAlreadyExists
	}

	topic := &Topic{
		TopicName:         topicName,
		ClusterArn:        clusterArn,
		ReplicationFactor: replicationFactor,
		NumPartitions:     numPartitions,
		ConfigEntries:     nonNilMapCopy(configEntries),
	}
	b.topics.Put(topic)

	return cloneTopic(topic), nil
}

// DeleteTopic deletes a topic from an MSK cluster.
func (b *InMemoryBackend) DeleteTopic(_ context.Context, clusterArn, topicName string) error {
	b.mu.Lock("DeleteTopic")
	defer b.mu.Unlock()

	if !b.clusters.Has(clusterArn) {
		return ErrNotFound
	}

	if !b.topics.Delete(topicKey(clusterArn, topicName)) {
		return ErrNotFound
	}

	return nil
}

// DescribeTopic retrieves a topic by cluster ARN and topic name.
func (b *InMemoryBackend) DescribeTopic(_ context.Context, clusterArn, topicName string) (*Topic, error) {
	b.mu.RLock("DescribeTopic")
	defer b.mu.RUnlock()

	t, ok := b.topics.Get(topicKey(clusterArn, topicName))
	if !ok {
		return nil, ErrNotFound
	}

	return cloneTopic(t), nil
}

// DescribeTopicPartitions retrieves a topic's partition count.
func (b *InMemoryBackend) DescribeTopicPartitions(ctx context.Context, clusterArn, topicName string) (*Topic, error) {
	return b.DescribeTopic(ctx, clusterArn, topicName)
}

// ListTopics returns all topics for a cluster sorted by topic name.
func (b *InMemoryBackend) ListTopics(_ context.Context, clusterArn string) ([]*Topic, error) {
	b.mu.RLock("ListTopics")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	topics := b.topicsByCluster.Get(clusterArn)
	out := make([]*Topic, 0, len(topics))

	for _, t := range topics {
		out = append(out, cloneTopic(t))
	}

	slices.SortFunc(out, func(a, b *Topic) int {
		if a.TopicName < b.TopicName {
			return -1
		}
		if a.TopicName > b.TopicName {
			return 1
		}

		return 0
	})

	return out, nil
}

// UpdateTopic updates a topic's config entries and/or partition count.
func (b *InMemoryBackend) UpdateTopic(
	_ context.Context,
	clusterArn, topicName string,
	numPartitions int32,
	configEntries map[string]string,
) (*Topic, error) {
	b.mu.Lock("UpdateTopic")
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topicKey(clusterArn, topicName))
	if !ok {
		return nil, ErrNotFound
	}

	if numPartitions > 0 {
		t.NumPartitions = numPartitions
	}

	if configEntries != nil {
		maps.Copy(t.ConfigEntries, configEntries)
	}

	return cloneTopic(t), nil
}

// AddTopicInternal creates a topic directly for testing purposes.
func (b *InMemoryBackend) AddTopicInternal(clusterArn, topicName string) *Topic {
	b.mu.Lock("AddTopicInternal")
	defer b.mu.Unlock()

	topic := &Topic{
		TopicName:         topicName,
		ClusterArn:        clusterArn,
		ReplicationFactor: defaultReplicationFactor,
		NumPartitions:     defaultPartitionCount,
		ConfigEntries:     make(map[string]string),
	}
	b.topics.Put(topic)

	return cloneTopic(topic)
}

// cloneTopic creates a deep copy of a Topic.
func cloneTopic(t *Topic) *Topic {
	return &Topic{
		TopicName:         t.TopicName,
		ClusterArn:        t.ClusterArn,
		ReplicationFactor: t.ReplicationFactor,
		NumPartitions:     t.NumPartitions,
		ConfigEntries:     nonNilMapCopy(t.ConfigEntries),
	}
}

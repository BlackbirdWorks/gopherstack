package kafka

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// topicARN builds the ARN for a topic on clusterArn, reusing the cluster's own
// "<name>/<uuid>" resource path segment the way real MSK topic ARNs do:
// arn:{partition}:kafka:{region}:{account}:topic/{clusterName}/{clusterUUID}/{topicName}.
func topicARN(clusterArn, topicName string) string {
	const clusterMarker = ":cluster/"

	prefix, clusterPath, ok := strings.Cut(clusterArn, clusterMarker)
	if !ok {
		// Malformed/test ARN without the usual "cluster/" resource marker:
		// fall back to appending a topic resource segment directly.
		return clusterArn + "/topic/" + topicName
	}

	return prefix + ":topic/" + clusterPath + "/" + topicName
}

// CreateTopic creates a topic on an MSK cluster. Real MSK creates topics
// synchronously from the caller's perspective (no polling protocol is exposed
// for topic creation the way Cluster CREATING->ACTIVE is), so the topic is
// ACTIVE immediately.
func (b *InMemoryBackend) CreateTopic(
	_ context.Context,
	clusterArn, topicName string,
	replicationFactor, partitionCount int32,
	configs string,
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
		ClusterArn:        clusterArn,
		TopicArn:          topicARN(clusterArn, topicName),
		TopicName:         topicName,
		Status:            TopicStateActive,
		ReplicationFactor: replicationFactor,
		PartitionCount:    partitionCount,
		Configs:           configs,
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

// DescribeTopicPartitions synthesizes per-partition placement info for a
// topic: for partition i, the leader/replica set is a round-robin assignment
// over the owning cluster's broker IDs (1..NumberOfBrokerNodes), with the
// full replica set reported in-sync (this in-memory emulator has no real
// broker/ISR state to diverge from). Returns ErrNotFound if the cluster or
// topic doesn't exist.
func (b *InMemoryBackend) DescribeTopicPartitions(
	_ context.Context, clusterArn, topicName string,
) ([]*TopicPartitionInfo, error) {
	b.mu.RLock("DescribeTopicPartitions")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	t, ok := b.topics.Get(topicKey(clusterArn, topicName))
	if !ok {
		return nil, ErrNotFound
	}

	numBrokers := max(c.NumberOfBrokerNodes, 1)
	replicaCount := min(max(t.ReplicationFactor, 1), numBrokers)

	partitions := make([]*TopicPartitionInfo, 0, t.PartitionCount)
	for i := range t.PartitionCount {
		leader := i%numBrokers + 1
		replicas := make([]int32, replicaCount)
		for r := range replicaCount {
			replicas[r] = (leader-1+r)%numBrokers + 1
		}

		partitions = append(partitions, &TopicPartitionInfo{
			Partition: i,
			Leader:    leader,
			Replicas:  replicas,
			Isr:       append([]int32(nil), replicas...),
		})
	}

	return partitions, nil
}

// ListTopics returns topics for a cluster sorted by topic name, optionally
// filtered to those whose name starts with nameFilter (matching real MSK's
// topicNameFilter query parameter; an empty nameFilter matches everything).
func (b *InMemoryBackend) ListTopics(_ context.Context, clusterArn, nameFilter string) ([]*Topic, error) {
	b.mu.RLock("ListTopics")
	defer b.mu.RUnlock()

	if !b.clusters.Has(clusterArn) {
		return nil, ErrNotFound
	}

	topics := b.topicsByCluster.Get(clusterArn)
	out := make([]*Topic, 0, len(topics))

	for _, t := range topics {
		if nameFilter != "" && !strings.HasPrefix(t.TopicName, nameFilter) {
			continue
		}

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

// UpdateTopic updates a topic's configs and/or partition count.
func (b *InMemoryBackend) UpdateTopic(
	_ context.Context,
	clusterArn, topicName string,
	partitionCount int32,
	configs string,
) (*Topic, error) {
	b.mu.Lock("UpdateTopic")
	defer b.mu.Unlock()

	t, ok := b.topics.Get(topicKey(clusterArn, topicName))
	if !ok {
		return nil, ErrNotFound
	}

	if partitionCount > 0 {
		t.PartitionCount = partitionCount
	}

	if configs != "" {
		t.Configs = configs
	}

	return cloneTopic(t), nil
}

// AddTopicInternal creates a topic directly for testing purposes.
func (b *InMemoryBackend) AddTopicInternal(clusterArn, topicName string) *Topic {
	b.mu.Lock("AddTopicInternal")
	defer b.mu.Unlock()

	topic := &Topic{
		ClusterArn:        clusterArn,
		TopicArn:          topicARN(clusterArn, topicName),
		TopicName:         topicName,
		Status:            TopicStateActive,
		ReplicationFactor: defaultReplicationFactor,
		PartitionCount:    defaultPartitionCount,
	}
	b.topics.Put(topic)

	return cloneTopic(topic)
}

// cloneTopic creates a deep copy of a Topic.
func cloneTopic(t *Topic) *Topic {
	return &Topic{
		ClusterArn:        t.ClusterArn,
		TopicArn:          t.TopicArn,
		TopicName:         t.TopicName,
		Status:            t.Status,
		ReplicationFactor: t.ReplicationFactor,
		PartitionCount:    t.PartitionCount,
		Configs:           t.Configs,
	}
}

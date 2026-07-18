package kafka

import (
	"context"
	"strings"
)

// ListNodes returns broker node stubs for a cluster.
func (b *InMemoryBackend) ListNodes(_ context.Context, clusterArn string) ([]*BrokerNode, error) {
	b.mu.RLock("ListNodes")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	out := make([]*BrokerNode, 0, int(c.NumberOfBrokerNodes))

	for i := range c.NumberOfBrokerNodes {
		out = append(out, &BrokerNode{
			BrokerID:     i + 1,
			InstanceType: c.BrokerNodeGroupInfo.InstanceType,
		})
	}

	return out, nil
}

// ListKafkaVersions returns supported Kafka versions, matching current MSK availability.
// Kafka versions are global (not region-scoped), so ctx is unused.
func (b *InMemoryBackend) ListKafkaVersions(_ context.Context) []*MSKVersion {
	return []*MSKVersion{
		{Version: "3.8.0.kraft", Status: ClusterStateActive},
		{Version: "3.7.x.kraft", Status: ClusterStateActive},
		{Version: kafkaVersion360, Status: ClusterStateActive},
		{Version: kafkaVersion351, Status: ClusterStateActive},
		{Version: "3.4.0", Status: ClusterStateActive},
		{Version: "3.3.2", Status: ClusterStateActive},
		{Version: "3.3.1", Status: ClusterStateActive},
		{Version: "2.8.2.tiered", Status: ClusterStateActive},
		{Version: "2.8.1", Status: ClusterStateActive},
		{Version: "2.8.0", Status: "DEPRECATED"},
		{Version: "2.6.0", Status: "DEPRECATED"},
	}
}

// GetCompatibleKafkaVersions returns Kafka versions compatible with the cluster's current version.
// KRaft clusters can only target KRaft versions. ZooKeeper clusters can target ZooKeeper versions up to 3.x.
func (b *InMemoryBackend) GetCompatibleKafkaVersions(_ context.Context, clusterArn string) ([]*MSKVersion, error) {
	b.mu.RLock("GetCompatibleKafkaVersions")
	defer b.mu.RUnlock()

	c, ok := b.clusters.Get(clusterArn)
	if !ok {
		return nil, ErrNotFound
	}

	current := c.KafkaVersion
	isKRaft := strings.HasSuffix(current, ".kraft")

	if isKRaft {
		return []*MSKVersion{
			{Version: "3.8.0.kraft", Status: ClusterStateActive},
			{Version: "3.7.x.kraft", Status: ClusterStateActive},
		}, nil
	}

	// ZooKeeper-based: return non-KRaft active versions higher than current.
	// For simplicity, return a curated list of ZooKeeper-compatible upgrades.
	return []*MSKVersion{
		{Version: kafkaVersion360, Status: ClusterStateActive},
		{Version: kafkaVersion351, Status: ClusterStateActive},
		{Version: "3.4.0", Status: ClusterStateActive},
		{Version: "3.3.2", Status: ClusterStateActive},
		{Version: "2.8.2.tiered", Status: ClusterStateActive},
	}, nil
}

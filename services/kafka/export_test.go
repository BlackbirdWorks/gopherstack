package kafka

// ExportedCluster is an alias for Cluster used in external tests.
type ExportedCluster = Cluster

// ExportedConfiguration is an alias for Configuration used in external tests.
type ExportedConfiguration = Configuration

// ParseKafkaPathForTest exposes parseKafkaPath for unit tests.
func ParseKafkaPathForTest(method, path string) (string, string) {
	return parseKafkaPath(method, path)
}

// ClusterCount returns the number of clusters in the backend across all regions.
func ClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return b.clusters.Len()
}

// ConfigurationCount returns the number of configurations in the backend across all regions.
func ConfigurationCount(b *InMemoryBackend) int {
	b.mu.RLock("ConfigurationCount")
	defer b.mu.RUnlock()

	return b.configurations.Len()
}

// ReplicatorCount returns the number of replicators in the backend across all regions.
func ReplicatorCount(b *InMemoryBackend) int {
	b.mu.RLock("ReplicatorCount")
	defer b.mu.RUnlock()

	return b.replicators.Len()
}

// TopicCount returns the number of topics in the backend across all regions.
func TopicCount(b *InMemoryBackend) int {
	b.mu.RLock("TopicCount")
	defer b.mu.RUnlock()

	return b.topics.Len()
}

// VpcConnectionCount returns the number of VPC connections in the backend across all regions.
func VpcConnectionCount(b *InMemoryBackend) int {
	b.mu.RLock("VpcConnectionCount")
	defer b.mu.RUnlock()

	return b.vpcConnections.Len()
}

// ClusterOperationCount returns the number of cluster operations in the backend across all regions.
func ClusterOperationCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterOperationCount")
	defer b.mu.RUnlock()

	return b.clusterOperations.Len()
}

// ScramSecretCount returns the number of SCRAM secrets across all clusters and regions.
func ScramSecretCount(b *InMemoryBackend) int {
	b.mu.RLock("ScramSecretCount")
	defer b.mu.RUnlock()

	total := 0
	for _, secrets := range b.scramSecrets {
		total += len(secrets)
	}

	return total
}

// HasClusterPolicy reports whether a cluster has a resource-based policy stored.
func HasClusterPolicy(b *InMemoryBackend, clusterArn string) bool {
	b.mu.RLock("HasClusterPolicy")
	defer b.mu.RUnlock()

	_, ok := b.clusterPolicies[clusterArn]

	return ok
}

// GetStoredCluster returns the raw (unwrapped) stored cluster for inspection in tests.
func GetStoredCluster(b *InMemoryBackend, clusterArn string) *Cluster {
	b.mu.RLock("GetStoredCluster")
	defer b.mu.RUnlock()

	c, _ := b.clusters.Get(clusterArn)

	return c
}

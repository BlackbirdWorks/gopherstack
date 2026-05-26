package kafka

// ExportedCluster is an alias for Cluster used in external tests.
type ExportedCluster = Cluster

// ExportedConfiguration is an alias for Configuration used in external tests.
type ExportedConfiguration = Configuration

// ParseKafkaPathForTest exposes parseKafkaPath for unit tests.
func ParseKafkaPathForTest(method, path string) (string, string) {
	return parseKafkaPath(method, path)
}

// ClusterCount returns the number of clusters in the backend.
func ClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return len(b.clusters)
}

// ConfigurationCount returns the number of configurations in the backend.
func ConfigurationCount(b *InMemoryBackend) int {
	b.mu.RLock("ConfigurationCount")
	defer b.mu.RUnlock()

	return len(b.configurations)
}

// ReplicatorCount returns the number of replicators in the backend.
func ReplicatorCount(b *InMemoryBackend) int {
	b.mu.RLock("ReplicatorCount")
	defer b.mu.RUnlock()

	return len(b.replicators)
}

// TopicCount returns the number of topics in the backend.
func TopicCount(b *InMemoryBackend) int {
	b.mu.RLock("TopicCount")
	defer b.mu.RUnlock()

	return len(b.topics)
}

// VpcConnectionCount returns the number of VPC connections in the backend.
func VpcConnectionCount(b *InMemoryBackend) int {
	b.mu.RLock("VpcConnectionCount")
	defer b.mu.RUnlock()

	return len(b.vpcConnections)
}

// ClusterOperationCount returns the number of cluster operations in the backend.
func ClusterOperationCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterOperationCount")
	defer b.mu.RUnlock()

	return len(b.clusterOperations)
}

// ScramSecretCount returns the number of SCRAM secrets across all clusters.
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

	return b.clusters[clusterArn]
}

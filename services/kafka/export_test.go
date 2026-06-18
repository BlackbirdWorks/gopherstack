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

	total := 0
	for _, regionClusters := range b.clusters {
		total += len(regionClusters)
	}

	return total
}

// ConfigurationCount returns the number of configurations in the backend across all regions.
func ConfigurationCount(b *InMemoryBackend) int {
	b.mu.RLock("ConfigurationCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionConfigs := range b.configurations {
		total += len(regionConfigs)
	}

	return total
}

// ReplicatorCount returns the number of replicators in the backend across all regions.
func ReplicatorCount(b *InMemoryBackend) int {
	b.mu.RLock("ReplicatorCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionReplicators := range b.replicators {
		total += len(regionReplicators)
	}

	return total
}

// TopicCount returns the number of topics in the backend across all regions.
func TopicCount(b *InMemoryBackend) int {
	b.mu.RLock("TopicCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionTopics := range b.topics {
		total += len(regionTopics)
	}

	return total
}

// VpcConnectionCount returns the number of VPC connections in the backend across all regions.
func VpcConnectionCount(b *InMemoryBackend) int {
	b.mu.RLock("VpcConnectionCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionConns := range b.vpcConnections {
		total += len(regionConns)
	}

	return total
}

// ClusterOperationCount returns the number of cluster operations in the backend across all regions.
func ClusterOperationCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterOperationCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionOps := range b.clusterOperations {
		total += len(regionOps)
	}

	return total
}

// ScramSecretCount returns the number of SCRAM secrets across all clusters and regions.
func ScramSecretCount(b *InMemoryBackend) int {
	b.mu.RLock("ScramSecretCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionSecrets := range b.scramSecrets {
		for _, secrets := range regionSecrets {
			total += len(secrets)
		}
	}

	return total
}

// HasClusterPolicy reports whether a cluster has a resource-based policy stored.
func HasClusterPolicy(b *InMemoryBackend, clusterArn string) bool {
	b.mu.RLock("HasClusterPolicy")
	defer b.mu.RUnlock()

	region := regionFromARN(clusterArn, b.region)
	_, ok := b.clusterPolicies[region][clusterArn]

	return ok
}

// GetStoredCluster returns the raw (unwrapped) stored cluster for inspection in tests.
func GetStoredCluster(b *InMemoryBackend, clusterArn string) *Cluster {
	b.mu.RLock("GetStoredCluster")
	defer b.mu.RUnlock()

	region := regionFromARN(clusterArn, b.region)

	return b.clusters[region][clusterArn]
}

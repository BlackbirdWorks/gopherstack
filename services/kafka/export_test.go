package kafka

// ExportedCluster is an alias for Cluster used in external tests.
type ExportedCluster = Cluster

// ExportedConfiguration is an alias for Configuration used in external tests.
type ExportedConfiguration = Configuration

// ParseKafkaPathForTest exposes parseKafkaPath for unit tests.
func ParseKafkaPathForTest(method, path string) (string, string) {
	return parseKafkaPath(method, path)
}

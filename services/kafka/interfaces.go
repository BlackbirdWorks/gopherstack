package kafka

// StorageBackend defines the interface for Kafka (MSK) backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Cluster operations
	CreateCluster(
		name, kafkaVersion string,
		numBrokers int32,
		brokerInfo BrokerNodeGroupInfo,
		tags map[string]string,
	) (*Cluster, error)
	DescribeCluster(clusterArn string) (*Cluster, error)
	ListClusters() []*Cluster
	DeleteCluster(clusterArn string) error

	// Configuration operations
	CreateConfiguration(
		name, description string,
		kafkaVersions []string,
		serverProperties string,
	) (*Configuration, error)
	DescribeConfiguration(configArn string) (*Configuration, error)
	ListConfigurations() []*Configuration
	DeleteConfiguration(configArn string) error

	// Tag operations
	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, tagKeys []string) error
	GetTags(resourceArn string) (map[string]string, error)

	// SCRAM secret operations
	BatchAssociateScramSecret(clusterArn string, secretArnList []string) ([]ScramSecretError, error)
	BatchDisassociateScramSecret(clusterArn string, secretArnList []string) ([]ScramSecretError, error)

	// Replicator operations
	CreateReplicator(name, description, serviceExecutionRoleArn string, tags map[string]string) (*Replicator, error)
	DeleteReplicator(replicatorArn string) error

	// Topic operations
	CreateTopic(
		clusterArn, topicName string,
		replicationFactor, numPartitions int32,
		configEntries map[string]string,
	) (*Topic, error)
	DeleteTopic(clusterArn, topicName string) error

	// VPC connection operations
	CreateVpcConnection(targetClusterArn, vpcID, authentication string, tags map[string]string) (*VpcConnection, error)
	DeleteVpcConnection(vpcConnectionArn string) error

	// Cluster policy operations
	DeleteClusterPolicy(clusterArn string) error

	// Cluster operation operations
	DescribeClusterOperation(clusterOperationArn string) (*ClusterOperation, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

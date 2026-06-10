package kafka

// StorageBackend defines the interface for Kafka (MSK) backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Cluster operations
	CreateCluster(
		name, kafkaVersion string,
		numBrokers int32,
		brokerInfo BrokerNodeGroupInfo,
		clientAuth *ClientAuthentication,
		tags map[string]string,
	) (*Cluster, error)
	CreateServerlessCluster(name string, serverless *ServerlessClusterInfo, tags map[string]string) (*Cluster, error)
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
	DescribeReplicator(replicatorArn string) (*Replicator, error)
	ListReplicators() []*Replicator
	UpdateReplicationInfo(replicatorArn, description string) (*Replicator, error)

	// Topic operations
	CreateTopic(
		clusterArn, topicName string,
		replicationFactor, numPartitions int32,
		configEntries map[string]string,
	) (*Topic, error)
	DeleteTopic(clusterArn, topicName string) error
	DescribeTopic(clusterArn, topicName string) (*Topic, error)
	DescribeTopicPartitions(clusterArn, topicName string) (*Topic, error)
	ListTopics(clusterArn string) ([]*Topic, error)
	UpdateTopic(clusterArn, topicName string, numPartitions int32, configEntries map[string]string) (*Topic, error)

	// VPC connection operations
	CreateVpcConnection(targetClusterArn, vpcID, authentication string, tags map[string]string) (*VpcConnection, error)
	DeleteVpcConnection(vpcConnectionArn string) error
	DescribeVpcConnection(vpcConnectionArn string) (*VpcConnection, error)
	ListVpcConnections() []*VpcConnection
	ListClientVpcConnections(clusterArn string) ([]*VpcConnection, error)
	RejectClientVpcConnection(vpcConnectionArn string) error

	// Cluster policy operations
	DeleteClusterPolicy(clusterArn string) error
	GetClusterPolicy(clusterArn string) (string, error)
	PutClusterPolicy(clusterArn, policy string) error

	// Cluster operation operations
	DescribeClusterOperation(clusterOperationArn string) (*ClusterOperation, error)
	DescribeClusterOperationV2(clusterOperationArn string) (*ClusterOperation, error)
	ListClusterOperations(clusterArn string) ([]*ClusterOperation, error)
	ListClusterOperationsV2(clusterArn string) ([]*ClusterOperation, error)

	// Configuration revision operations
	DescribeConfigurationRevision(configArn string, revision int64) (*ConfigurationRevision, error)
	UpdateConfiguration(configArn, description, serverProperties string) (*Configuration, error)
	ListConfigurationRevisions(configArn string) ([]*ConfigurationRevision, error)

	// Broker / cluster update operations
	UpdateBrokerCount(clusterArn string, numBrokers int32) (*ClusterOperation, error)
	UpdateBrokerStorage(clusterArn string, volumeSize int32) (*ClusterOperation, error)
	UpdateBrokerType(clusterArn, instanceType string) (*ClusterOperation, error)
	UpdateClusterConfiguration(clusterArn, configArn string, revision int64) (*ClusterOperation, error)
	UpdateClusterKafkaVersion(clusterArn, targetKafkaVersion string) (*ClusterOperation, error)
	UpdateConnectivity(clusterArn string, settings UpdateConnectivitySettings) (*ClusterOperation, error)
	UpdateMonitoring(clusterArn string, settings UpdateMonitoringSettings) (*ClusterOperation, error)
	UpdateRebalancing(clusterArn string) (*ClusterOperation, error)
	UpdateSecurity(clusterArn string, settings UpdateSecuritySettings) (*ClusterOperation, error)
	UpdateStorage(clusterArn string, settings UpdateStorageSettings) (*ClusterOperation, error)
	RebootBroker(clusterArn string, brokerIDs []string) (*ClusterOperation, error)

	// SCRAM secret list
	ListScramSecrets(clusterArn string) ([]string, error)

	// Node / version ops
	ListNodes(clusterArn string) ([]*BrokerNode, error)
	ListKafkaVersions() []*MSKVersion
	GetCompatibleKafkaVersions(clusterArn string) ([]*MSKVersion, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

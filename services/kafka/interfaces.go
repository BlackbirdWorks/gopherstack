package kafka

import "context"

// StorageBackend defines the interface for Kafka (MSK) backend implementations.
// All mutating methods must be safe for concurrent use.
//
// Every operation takes a context.Context so the backend can resolve the caller's
// AWS region (for create/list operations) and route to the correct per-region
// store. Operations that target an existing resource ARN resolve their region
// from the ARN itself, falling back to the context region.
type StorageBackend interface {
	// Cluster operations
	CreateCluster(
		ctx context.Context,
		name, kafkaVersion string,
		numBrokers int32,
		brokerInfo BrokerNodeGroupInfo,
		clientAuth *ClientAuthentication,
		tags map[string]string,
	) (*Cluster, error)
	CreateServerlessCluster(
		ctx context.Context, name string, serverless *ServerlessClusterInfo, tags map[string]string,
	) (*Cluster, error)
	DescribeCluster(ctx context.Context, clusterArn string) (*Cluster, error)
	ListClusters(ctx context.Context) []*Cluster
	DeleteCluster(ctx context.Context, clusterArn string) error

	// Configuration operations
	CreateConfiguration(
		ctx context.Context,
		name, description string,
		kafkaVersions []string,
		serverProperties string,
	) (*Configuration, error)
	DescribeConfiguration(ctx context.Context, configArn string) (*Configuration, error)
	ListConfigurations(ctx context.Context) []*Configuration
	DeleteConfiguration(ctx context.Context, configArn string) error

	// Tag operations
	TagResource(ctx context.Context, resourceArn string, tags map[string]string) error
	UntagResource(ctx context.Context, resourceArn string, tagKeys []string) error
	GetTags(ctx context.Context, resourceArn string) (map[string]string, error)

	// SCRAM secret operations
	BatchAssociateScramSecret(
		ctx context.Context,
		clusterArn string,
		secretArnList []string,
	) ([]ScramSecretError, error)
	BatchDisassociateScramSecret(
		ctx context.Context, clusterArn string, secretArnList []string,
	) ([]ScramSecretError, error)

	// Replicator operations
	CreateReplicator(
		ctx context.Context,
		name, description, serviceExecutionRoleArn string,
		kafkaClusters []ClusterConfig,
		replicationInfoList []ReplicationInfoConfig,
		tags map[string]string,
	) (*Replicator, error)
	DeleteReplicator(ctx context.Context, replicatorArn string) error
	DescribeReplicator(ctx context.Context, replicatorArn string) (*Replicator, error)
	ListReplicators(ctx context.Context) []*Replicator
	UpdateReplicationInfo(
		ctx context.Context,
		replicatorArn, currentVersion, sourceKafkaClusterArn, targetKafkaClusterArn string,
		topicReplication *TopicReplicationConfig,
		consumerGroupReplication *ConsumerGroupReplicationConfig,
	) (*Replicator, error)

	// Topic operations
	CreateTopic(
		ctx context.Context,
		clusterArn, topicName string,
		replicationFactor, partitionCount int32,
		configs string,
	) (*Topic, error)
	DeleteTopic(ctx context.Context, clusterArn, topicName string) error
	DescribeTopic(ctx context.Context, clusterArn, topicName string) (*Topic, error)
	DescribeTopicPartitions(ctx context.Context, clusterArn, topicName string) ([]*TopicPartitionInfo, error)
	ListTopics(ctx context.Context, clusterArn, topicNameFilter string) ([]*Topic, error)
	UpdateTopic(
		ctx context.Context, clusterArn, topicName string, partitionCount int32, configs string,
	) (*Topic, error)

	// VPC connection operations
	CreateVpcConnection(
		ctx context.Context,
		targetClusterArn, vpcID, authentication string,
		clientSubnets, securityGroups []string,
		tags map[string]string,
	) (*VpcConnection, error)
	DeleteVpcConnection(ctx context.Context, vpcConnectionArn string) error
	DescribeVpcConnection(ctx context.Context, vpcConnectionArn string) (*VpcConnection, error)
	ListVpcConnections(ctx context.Context) []*VpcConnection
	ListClientVpcConnections(ctx context.Context, clusterArn string) ([]*VpcConnection, error)
	RejectClientVpcConnection(ctx context.Context, vpcConnectionArn string) error

	// Cluster policy operations
	DeleteClusterPolicy(ctx context.Context, clusterArn string) error
	GetClusterPolicy(ctx context.Context, clusterArn string) (string, error)
	PutClusterPolicy(ctx context.Context, clusterArn, policy string) error

	// Cluster operation operations
	DescribeClusterOperation(ctx context.Context, clusterOperationArn string) (*ClusterOperation, error)
	DescribeClusterOperationV2(ctx context.Context, clusterOperationArn string) (*ClusterOperation, error)
	ListClusterOperations(ctx context.Context, clusterArn string) ([]*ClusterOperation, error)
	ListClusterOperationsV2(ctx context.Context, clusterArn string) ([]*ClusterOperation, error)

	// Configuration revision operations
	DescribeConfigurationRevision(ctx context.Context, configArn string, revision int64) (*ConfigurationRevision, error)
	UpdateConfiguration(ctx context.Context, configArn, description, serverProperties string) (*Configuration, error)
	ListConfigurationRevisions(ctx context.Context, configArn string) ([]*ConfigurationRevision, error)

	// Broker / cluster update operations
	UpdateBrokerCount(ctx context.Context, clusterArn string, numBrokers int32) (*ClusterOperation, error)
	UpdateBrokerStorage(ctx context.Context, clusterArn string, volumeSize int32) (*ClusterOperation, error)
	UpdateBrokerType(ctx context.Context, clusterArn, instanceType string) (*ClusterOperation, error)
	UpdateClusterConfiguration(
		ctx context.Context,
		clusterArn, configArn string,
		revision int64,
	) (*ClusterOperation, error)
	UpdateClusterKafkaVersion(ctx context.Context, clusterArn, targetKafkaVersion string) (*ClusterOperation, error)
	UpdateConnectivity(
		ctx context.Context,
		clusterArn string,
		settings UpdateConnectivitySettings,
	) (*ClusterOperation, error)
	UpdateMonitoring(
		ctx context.Context,
		clusterArn string,
		settings UpdateMonitoringSettings,
	) (*ClusterOperation, error)
	UpdateRebalancing(ctx context.Context, clusterArn string) (*ClusterOperation, error)
	UpdateSecurity(ctx context.Context, clusterArn string, settings UpdateSecuritySettings) (*ClusterOperation, error)
	UpdateStorage(ctx context.Context, clusterArn string, settings UpdateStorageSettings) (*ClusterOperation, error)
	RebootBroker(ctx context.Context, clusterArn string, brokerIDs []string) (*ClusterOperation, error)

	// SCRAM secret list
	ListScramSecrets(ctx context.Context, clusterArn string) ([]string, error)

	// Node / version ops
	ListNodes(ctx context.Context, clusterArn string) ([]*BrokerNode, error)
	ListKafkaVersions(ctx context.Context) []*MSKVersion
	GetCompatibleKafkaVersions(ctx context.Context, clusterArn string) ([]*MSKVersion, error)

	// Channel operations
	CreateChannel(
		ctx context.Context,
		clusterArn, channelName string,
		topicConfigurationList []TopicConfiguration,
		encryptionConfiguration *ChannelEncryptionConfiguration,
		icebergDestinationConfiguration *IcebergDestinationConfiguration,
		s3DestinationConfiguration *S3DestinationConfiguration,
		loggingInfo *ChannelLoggingInfo,
		tags map[string]string,
	) (*Channel, error)
	DeleteChannel(ctx context.Context, clusterArn, channelArn string) (*Channel, error)
	DescribeChannel(ctx context.Context, clusterArn, channelArn string) (*Channel, error)
	ListChannels(ctx context.Context, clusterArn, topicNameFilter string) ([]*Channel, error)
	UpdateChannel(
		ctx context.Context,
		clusterArn, channelArn string,
		icebergDestinationUpdate *IcebergDestinationUpdate,
		s3DestinationUpdate *S3DestinationUpdate,
	) (*Channel, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

package kafka

const (
	// kafkaVersion360 is the MSK Kafka 3.6.0 version identifier.
	kafkaVersion360 = "3.6.0"
	// kafkaVersion351 is the MSK Kafka 3.5.1 version identifier.
	kafkaVersion351 = "3.5.1"
)

const (
	// ReplicatorStateRunning indicates a running replicator.
	ReplicatorStateRunning = "RUNNING"
	// VpcConnectionStateAvailable indicates a VPC connection that is available.
	VpcConnectionStateAvailable = "AVAILABLE"
	// ClusterOperationStateUpdateComplete indicates a completed cluster operation.
	ClusterOperationStateUpdateComplete = "UPDATE_COMPLETE"
	// DefaultClusterVersion is the default MSK cluster version identifier.
	DefaultClusterVersion = "K3AEGXETSR30VB"
)

const (
	// ClusterStateActive indicates a running cluster.
	ClusterStateActive = "ACTIVE"
	// ClusterStateCreating indicates a cluster being provisioned.
	ClusterStateCreating = "CREATING"
	// ClusterStateDeleting indicates a cluster being removed.
	ClusterStateDeleting = "DELETING"
	// ClusterStateFailed indicates a cluster in a failed state.
	ClusterStateFailed = "FAILED"
	// ClusterStateUpdating indicates a cluster undergoing an update.
	ClusterStateUpdating = "UPDATING"
	// ClusterStateRebootingBroker indicates a broker reboot in progress.
	ClusterStateRebootingBroker = "REBOOTING_BROKER"
	// ClusterStateMaintenance indicates a cluster in a maintenance window.
	ClusterStateMaintenance = "MAINTENANCE"
	// ClusterStateHealing indicates a cluster undergoing node healing.
	ClusterStateHealing = "HEALING"
)

const (
	// ClusterTypeProvisioned is the standard MSK cluster type.
	ClusterTypeProvisioned = "PROVISIONED"
	// ClusterTypeServerless is the MSK Serverless cluster type.
	ClusterTypeServerless = "SERVERLESS"
)

const (
	// EnhancedMonitoringDefault is the default monitoring level.
	EnhancedMonitoringDefault = "DEFAULT"
	// EnhancedMonitoringPerBroker enables per-broker metrics.
	EnhancedMonitoringPerBroker = "PER_BROKER"
	// EnhancedMonitoringPerTopicPerBroker enables per-topic-per-broker metrics.
	EnhancedMonitoringPerTopicPerBroker = "PER_TOPIC_PER_BROKER"
	// EnhancedMonitoringPerTopicPerPartition enables per-topic-per-partition metrics.
	EnhancedMonitoringPerTopicPerPartition = "PER_TOPIC_PER_PARTITION"
)

const (
	// StorageModeLocal is the standard EBS-only storage mode.
	StorageModeLocal = "LOCAL"
	// StorageModeTiered enables tiered storage (remote storage offload).
	StorageModeTiered = "TIERED"
)

const (
	// EncryptionInTransitTLS requires TLS for all client-broker traffic.
	EncryptionInTransitTLS = "TLS"
	// EncryptionInTransitTLSPlaintext allows both TLS and plaintext.
	EncryptionInTransitTLSPlaintext = "TLS_PLAINTEXT"
	// EncryptionInTransitPlaintext allows plaintext only (dev/test use).
	EncryptionInTransitPlaintext = "PLAINTEXT"
)

const (
	// defaultBrokerCount is used in seed helpers for testing.
	defaultBrokerCount = 3
	// defaultReplicationFactor is used in AddTopicInternal.
	defaultReplicationFactor = 3
	// defaultPartitionCount is used in AddTopicInternal.
	defaultPartitionCount = 1
	// maxClustersPerRegion caps the number of clusters per region to prevent unbounded growth.
	maxClustersPerRegion = 500
)

// ProvisionedThroughput holds EBS provisioned throughput config.
type ProvisionedThroughput struct {
	VolumeThroughput int32 `json:"volumeThroughput,omitempty"`
	Enabled          bool  `json:"enabled"`
}

// EBSStorageInfo holds EBS volume config.
type EBSStorageInfo struct {
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput,omitempty"`
	VolumeSize            int32                  `json:"volumeSize,omitempty"`
}

// StorageInfo holds broker storage config.
type StorageInfo struct {
	EbsStorageInfo *EBSStorageInfo `json:"ebsStorageInfo,omitempty"`
}

// PublicAccess holds public access configuration for broker connectivity.
type PublicAccess struct {
	Type string `json:"type,omitempty"`
}

// VpcConnectivitySaslIam holds IAM authentication settings for VPC connectivity.
type VpcConnectivitySaslIam struct {
	Enabled bool `json:"enabled"`
}

// VpcConnectivitySaslScram holds SCRAM authentication settings for VPC connectivity.
type VpcConnectivitySaslScram struct {
	Enabled bool `json:"enabled"`
}

// VpcConnectivitySasl holds SASL settings for VPC connectivity.
type VpcConnectivitySasl struct {
	Iam   *VpcConnectivitySaslIam   `json:"iam,omitempty"`
	Scram *VpcConnectivitySaslScram `json:"scram,omitempty"`
}

// VpcConnectivityTLS holds TLS settings for VPC connectivity.
type VpcConnectivityTLS struct {
	Enabled bool `json:"enabled"`
}

// VpcConnectivityClientAuthentication holds authentication settings for VPC connectivity.
type VpcConnectivityClientAuthentication struct {
	Sasl *VpcConnectivitySasl `json:"sasl,omitempty"`
	TLS  *VpcConnectivityTLS  `json:"tls,omitempty"`
}

// VpcConnectivity holds VPC connectivity configuration.
type VpcConnectivity struct {
	ClientAuthentication *VpcConnectivityClientAuthentication `json:"clientAuthentication,omitempty"`
}

// ConnectivityInfo holds broker connectivity configuration.
type ConnectivityInfo struct {
	PublicAccess    *PublicAccess    `json:"publicAccess,omitempty"`
	VpcConnectivity *VpcConnectivity `json:"vpcConnectivity,omitempty"`
}

// BrokerNodeGroupInfo holds broker node configuration.
type BrokerNodeGroupInfo struct {
	ConnectivityInfo     *ConnectivityInfo `json:"connectivityInfo,omitempty"`
	StorageInfo          *StorageInfo      `json:"storageInfo,omitempty"`
	BrokerAZDistribution string            `json:"brokerAZDistribution,omitempty"`
	InstanceType         string            `json:"instanceType"`
	ZoneIDs              []string          `json:"zoneIds,omitempty"`
	ClientSubnets        []string          `json:"clientSubnets"`
	SecurityGroups       []string          `json:"securityGroups,omitempty"`
}

// ConfigurationInfo holds a cluster configuration reference.
type ConfigurationInfo struct {
	Arn      string `json:"arn"`
	Revision int64  `json:"revision"`
}

// ClientAuthentication holds MSK cluster authentication configuration.
type ClientAuthentication struct {
	Sasl            *SaslSettings            `json:"sasl,omitempty"`
	TLS             *TLSSettings             `json:"tls,omitempty"`
	Unauthenticated *UnauthenticatedSettings `json:"unauthenticated,omitempty"`
}

// SaslSettings holds SASL authentication settings.
type SaslSettings struct {
	Scram *SaslScram `json:"scram,omitempty"`
	Iam   *SaslIam   `json:"iam,omitempty"`
}

// SaslScram holds SASL/SCRAM settings.
type SaslScram struct {
	Enabled bool `json:"enabled"`
}

// SaslIam holds SASL/IAM settings.
type SaslIam struct {
	Enabled bool `json:"enabled"`
}

// TLSSettings holds TLS authentication settings.
type TLSSettings struct {
	CertificateAuthorityArnList []string `json:"certificateAuthorityArnList,omitempty"`
	Enabled                     bool     `json:"enabled"`
}

// UnauthenticatedSettings holds unauthenticated access settings.
type UnauthenticatedSettings struct {
	Enabled bool `json:"enabled"`
}

// EncryptionAtRest holds at-rest encryption configuration.
type EncryptionAtRest struct {
	DataVolumeKMSKeyID string `json:"dataVolumeKMSKeyId,omitempty"`
}

// EncryptionInTransit holds in-transit encryption configuration.
type EncryptionInTransit struct {
	ClientBroker string `json:"clientBroker,omitempty"`
	InCluster    bool   `json:"inCluster"`
}

// EncryptionInfo holds cluster encryption configuration.
type EncryptionInfo struct {
	EncryptionAtRest    *EncryptionAtRest    `json:"encryptionAtRest,omitempty"`
	EncryptionInTransit *EncryptionInTransit `json:"encryptionInTransit,omitempty"`
}

// JmxExporter holds JMX exporter settings.
type JmxExporter struct {
	EnabledInBroker bool `json:"enabledInBroker"`
}

// NodeExporter holds Node exporter settings.
type NodeExporter struct {
	EnabledInBroker bool `json:"enabledInBroker"`
}

// PrometheusInfo holds Prometheus scraping configuration.
type PrometheusInfo struct {
	JmxExporter  *JmxExporter  `json:"jmxExporter,omitempty"`
	NodeExporter *NodeExporter `json:"nodeExporter,omitempty"`
}

// OpenMonitoring holds open monitoring configuration.
type OpenMonitoring struct {
	Prometheus *PrometheusInfo `json:"prometheus,omitempty"`
}

// CloudWatchLogs holds CloudWatch log delivery settings.
type CloudWatchLogs struct {
	LogGroup string `json:"logGroup,omitempty"`
	Enabled  bool   `json:"enabled"`
}

// Firehose holds Kinesis Data Firehose log delivery settings.
type Firehose struct {
	DeliveryStream string `json:"deliveryStream,omitempty"`
	Enabled        bool   `json:"enabled"`
}

// S3Logs holds S3 log delivery settings.
type S3Logs struct {
	Bucket  string `json:"bucket,omitempty"`
	Prefix  string `json:"prefix,omitempty"`
	Enabled bool   `json:"enabled"`
}

// BrokerLogs holds broker log delivery destinations.
type BrokerLogs struct {
	CloudWatchLogs *CloudWatchLogs `json:"cloudWatchLogs,omitempty"`
	Firehose       *Firehose       `json:"firehose,omitempty"`
	S3             *S3Logs         `json:"s3,omitempty"`
}

// LoggingInfo holds cluster logging configuration.
type LoggingInfo struct {
	BrokerLogs *BrokerLogs `json:"brokerLogs,omitempty"`
}

// StateInfo holds cluster state detail for error conditions.
type StateInfo struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// ServerlessVpcConfig holds VPC configuration for a serverless cluster.
type ServerlessVpcConfig struct {
	SubnetIDs        []string `json:"subnetIds,omitempty"`
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`
}

// ServerlessClientAuthentication holds authentication settings for a serverless cluster.
type ServerlessClientAuthentication struct {
	Sasl *SaslSettings `json:"sasl,omitempty"`
}

// ServerlessClusterInfo holds serverless-specific cluster configuration.
type ServerlessClusterInfo struct {
	ClientAuthentication *ServerlessClientAuthentication `json:"clientAuthentication,omitempty"`
	VpcConfigs           []ServerlessVpcConfig           `json:"vpcConfigs,omitempty"`
}

// Cluster represents an MSK cluster.
type Cluster struct {
	Tags                 map[string]string      `json:"-"`
	ClientAuthentication *ClientAuthentication  `json:"clientAuthentication,omitempty"`
	EncryptionInfo       *EncryptionInfo        `json:"encryptionInfo,omitempty"`
	OpenMonitoring       *OpenMonitoring        `json:"openMonitoring,omitempty"`
	LoggingInfo          *LoggingInfo           `json:"loggingInfo,omitempty"`
	StateInfo            *StateInfo             `json:"stateInfo,omitempty"`
	Serverless           *ServerlessClusterInfo `json:"serverless,omitempty"`
	ConfigurationInfo    *ConfigurationInfo     `json:"configurationInfo,omitempty"`
	ClusterArn           string                 `json:"clusterArn"`
	ClusterName          string                 `json:"clusterName"`
	ClusterType          string                 `json:"clusterType"`
	KafkaVersion         string                 `json:"kafkaVersion,omitempty"`
	State                string                 `json:"state"`
	CurrentVersion       string                 `json:"currentVersion"`
	ActiveOperationArn   string                 `json:"activeOperationArn,omitempty"`
	EnhancedMonitoring   string                 `json:"enhancedMonitoring,omitempty"`
	StorageMode          string                 `json:"storageMode,omitempty"`
	CreationTime         string                 `json:"creationTime,omitempty"`
	BrokerNodeGroupInfo  BrokerNodeGroupInfo    `json:"brokerNodeGroupInfo"`
	NumberOfBrokerNodes  int32                  `json:"numberOfBrokerNodes"`
	pollCount            int                    // tracks CREATING→ACTIVE progression; not serialized
}

// Configuration represents an MSK configuration.
type Configuration struct {
	Tags             map[string]string `json:"-"`
	Arn              string            `json:"arn"`
	Name             string            `json:"name"`
	Description      string            `json:"description,omitempty"`
	ServerProperties string            `json:"serverProperties"`
	KafkaVersions    []string          `json:"kafkaVersions"`
}

// ConfigurationRevision represents a revision of an MSK configuration.
type ConfigurationRevision struct {
	ConfigurationArn string `json:"configurationArn"`
	Description      string `json:"description,omitempty"`
	ServerProperties string `json:"serverProperties,omitempty"`
	Revision         int64  `json:"revision"`
}

// UpdateConnectivitySettings is the payload for UpdateConnectivity.
type UpdateConnectivitySettings struct {
	ConnectivityInfo *ConnectivityInfo
}

// UpdateMonitoringSettings is the payload for UpdateMonitoring.
type UpdateMonitoringSettings struct {
	OpenMonitoring     *OpenMonitoring
	LoggingInfo        *LoggingInfo
	EnhancedMonitoring string
}

// UpdateSecuritySettings is the payload for UpdateSecurity.
type UpdateSecuritySettings struct {
	ClientAuthentication *ClientAuthentication
	EncryptionInfo       *EncryptionInfo
}

// UpdateStorageSettings is the payload for UpdateStorage.
type UpdateStorageSettings struct {
	ProvisionedThroughput *ProvisionedThroughput
	StorageMode           string
	VolumeSizeGB          int32
}

// BrokerNode represents a stub broker node.
type BrokerNode struct {
	InstanceType string `json:"instanceType,omitempty"`
	BrokerID     int32  `json:"brokerId"`
}

// MSKVersion represents an available Kafka version.
type MSKVersion struct {
	Version string `json:"version"`
	Status  string `json:"status"`
}

// ScramSecretError represents an error that occurred while associating or disassociating a SCRAM secret.
type ScramSecretError struct {
	SecretArn    string `json:"secretArn"`
	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// Replicator represents an MSK replicator.
type Replicator struct {
	Tags                    map[string]string `json:"-"`
	ReplicatorArn           string            `json:"replicatorArn"`
	ReplicatorName          string            `json:"replicatorName"`
	Description             string            `json:"description,omitempty"`
	ServiceExecutionRoleArn string            `json:"serviceExecutionRoleArn"`
	ReplicatorState         string            `json:"replicatorState"`
}

// Topic represents an MSK topic on a cluster.
type Topic struct {
	ConfigEntries     map[string]string `json:"configEntries,omitempty"`
	TopicName         string            `json:"topicName"`
	ClusterArn        string            `json:"clusterArn"`
	ReplicationFactor int32             `json:"replicationFactor"`
	NumPartitions     int32             `json:"numPartitions"`
}

// VpcConnection represents an MSK VPC connection.
type VpcConnection struct {
	Tags             map[string]string `json:"-"`
	VpcConnectionArn string            `json:"vpcConnectionArn"`
	TargetClusterArn string            `json:"targetClusterArn"`
	VpcID            string            `json:"vpcId"`
	Authentication   string            `json:"authentication,omitempty"`
	State            string            `json:"state"`
}

// ClusterOperation represents an MSK cluster operation.
type ClusterOperation struct {
	SourceClusterInfo   *MutableClusterInfo `json:"sourceClusterInfo,omitempty"`
	TargetClusterInfo   *MutableClusterInfo `json:"targetClusterInfo,omitempty"`
	ClusterOperationArn string              `json:"clusterOperationArn"`
	ClusterArn          string              `json:"clusterArn"`
	OperationType       string              `json:"operationType"`
	OperationState      string              `json:"operationState"`
}

// MutableClusterInfo captures the subset of cluster configuration that an update
// operation changes. DescribeClusterOperation returns it as SourceClusterInfo
// (the state before the operation) and TargetClusterInfo (the requested state).
type MutableClusterInfo struct {
	ConnectivityInfo     *ConnectivityInfo     `json:"connectivityInfo,omitempty"`
	OpenMonitoring       *OpenMonitoring       `json:"openMonitoring,omitempty"`
	LoggingInfo          *LoggingInfo          `json:"loggingInfo,omitempty"`
	ClientAuthentication *ClientAuthentication `json:"clientAuthentication,omitempty"`
	EncryptionInfo       *EncryptionInfo       `json:"encryptionInfo,omitempty"`
	StorageMode          string                `json:"storageMode,omitempty"`
	EnhancedMonitoring   string                `json:"enhancedMonitoring,omitempty"`
	BrokerEBSVolumeInfo  []BrokerEBSVolumeInfo `json:"brokerEBSVolumeInfo,omitempty"`
	NumberOfBrokerNodes  int32                 `json:"numberOfBrokerNodes,omitempty"`
}

// BrokerEBSVolumeInfo describes a per-broker EBS volume target for UpdateStorage.
type BrokerEBSVolumeInfo struct {
	ProvisionedThroughput *ProvisionedThroughput `json:"provisionedThroughput,omitempty"`
	KafkaBrokerNodeID     string                 `json:"kafkaBrokerNodeId,omitempty"`
	VolumeSizeGB          int32                  `json:"volumeSizeGB,omitempty"`
}

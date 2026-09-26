package kafkaconnect

// ConnectorSpec carries every CreateConnector input field except the
// account/region needed to mint the ARN.
type ConnectorSpec struct {
	Capacity                         Capacity
	ConnectorConfiguration           map[string]string
	Tags                             map[string]string
	WorkerConfiguration              *WorkerConfigRef
	WorkerLogDelivery                *WorkerLogDelivery
	Description                      string
	Name                             string
	KafkaConnectVersion              string
	KafkaClusterClientAuthentication string
	KafkaClusterEncryptionInTransit  string
	ServiceExecutionRoleArn          string
	NetworkType                      string
	ApacheKafkaCluster               ApacheKafkaCluster
	Plugins                          []PluginRef
}

// ConnectorUpdate carries UpdateConnector's mutually exclusive target fields;
// exactly one of Capacity or ConnectorConfiguration is non-nil.
type ConnectorUpdate struct {
	Capacity               *Capacity
	ConnectorConfiguration map[string]string
}

// StorageBackend is the interface for the MSK Connect backend.
type StorageBackend interface {
	CreateConnector(accountID, region string, spec ConnectorSpec) (*Connector, error)
	DescribeConnector(connectorArn string) (*Connector, error)
	ListConnectors(namePrefix, nextToken string, maxResults int) ([]*Connector, string, error)
	UpdateConnector(
		connectorArn, currentVersion string,
		update ConnectorUpdate,
	) (*Connector, *ConnectorOperation, error)
	DeleteConnector(connectorArn, currentVersion string) (*Connector, error)
	DescribeConnectorOperation(operationArn string) (*ConnectorOperation, error)
	ListConnectorOperations(connectorArn, nextToken string, maxResults int) ([]*ConnectorOperation, string, error)

	CreateCustomPlugin(
		accountID, region, name, description, contentType, bucketArn, fileKey, objectVersion string,
		tags map[string]string,
	) (*CustomPlugin, error)
	DescribeCustomPlugin(customPluginArn string) (*CustomPlugin, error)
	ListCustomPlugins(namePrefix, nextToken string, maxResults int) ([]*CustomPlugin, string, error)
	DeleteCustomPlugin(customPluginArn string) (*CustomPlugin, error)

	CreateWorkerConfiguration(
		accountID, region, name, description, propertiesFileContent string,
		tags map[string]string,
	) (*WorkerConfiguration, error)
	DescribeWorkerConfiguration(workerConfigurationArn string) (*WorkerConfiguration, error)
	ListWorkerConfigurations(namePrefix, nextToken string, maxResults int) ([]*WorkerConfiguration, string, error)
	DeleteWorkerConfiguration(workerConfigurationArn string) (*WorkerConfiguration, error)

	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, tagKeys []string) error
	ListTagsForResource(resourceArn string) (map[string]string, error)

	Reset()
}

// Compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

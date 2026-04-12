package sagemaker

// StorageBackend defines the interface for SageMaker backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	CreateModel(
		name, executionRoleARN string,
		primaryContainer *ContainerDefinition,
		containers []ContainerDefinition,
		tags map[string]string,
	) (*Model, error)
	DescribeModel(name string) (*Model, error)
	ListModels(nextToken string) ([]*Model, string)
	DeleteModel(name string) error

	CreateEndpointConfig(
		name string,
		productionVariants []ProductionVariant,
		tags map[string]string,
	) (*EndpointConfig, error)
	DescribeEndpointConfig(name string) (*EndpointConfig, error)
	ListEndpointConfigs(nextToken string) ([]*EndpointConfig, string)
	DeleteEndpointConfig(name string) error

	AddTags(resourceARN string, tags map[string]string) error
	ListTags(resourceARN string) (map[string]string, error)
	DeleteTags(resourceARN string, tagKeys []string) error

	AddAssociation(
		sourceArn, destinationArn, associationType string,
		tags map[string]string,
	) (*Association, error)
	AssociateTrialComponent(trialName, trialComponentName string) (*TrialComponentAssociation, error)

	AttachClusterNodeVolume(clusterName, nodeID string, volume ClusterNodeVolume) (string, string, error)
	BatchAddClusterNodes(clusterName string, nodeConfigs []ClusterNode) (string, []string, error)
	BatchDeleteClusterNodes(clusterName string, nodeIDs []string) (string, []string, []string, error)
	BatchDescribeModelPackage(modelPackageArns []string) map[string]ModelPackageBatchResult
	BatchRebootClusterNodes(clusterName string, nodeIDs []string) (string, []string, []string, error)
	BatchReplaceClusterNodes(clusterName string, nodes []ClusterNode) (string, []string, error)

	CreateAction(
		name, actionType, description, status string,
		source ActionSource,
		properties map[string]string,
		tags map[string]string,
	) (*Action, error)
	CreateAlgorithm(name, description string, tags map[string]string) (*Algorithm, error)

	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// Ensure InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

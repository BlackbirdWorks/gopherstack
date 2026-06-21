package sagemaker

import "context"

// StorageBackend defines the interface for SageMaker backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	CreateModel(
		ctx context.Context,
		name, executionRoleARN string,
		primaryContainer *ContainerDefinition,
		containers []ContainerDefinition,
		tags map[string]string,
	) (*Model, error)
	DescribeModel(ctx context.Context, name string) (*Model, error)
	ListModels(ctx context.Context, nextToken string) ([]*Model, string)
	DeleteModel(ctx context.Context, name string) error
	SetModelExtras(
		ctx context.Context,
		name string,
		vpcConfig *VpcConfig,
		enableNetworkIsolation bool,
		inferenceExecConfig *InferenceExecutionConfig,
	) error

	CreateEndpointConfig(
		ctx context.Context,
		name string,
		productionVariants []ProductionVariant,
		tags map[string]string,
	) (*EndpointConfig, error)
	DescribeEndpointConfig(ctx context.Context, name string) (*EndpointConfig, error)
	ListEndpointConfigs(ctx context.Context, nextToken string) ([]*EndpointConfig, string)
	DeleteEndpointConfig(ctx context.Context, name string) error
	SetEndpointConfigExtras(
		ctx context.Context,
		name string,
		dataCaptureConfig *DataCaptureConfig,
		asyncInferenceConfig *AsyncInferenceConfig,
		vpcConfig *VpcConfig,
		executionRoleArn string,
		kmsKeyID string,
		shadowProductionVariants []ProductionVariant,
		enableNetworkIsolation bool,
	) error

	AddTags(ctx context.Context, resourceARN string, tags map[string]string) error
	ListTags(ctx context.Context, resourceARN string) (map[string]string, error)
	DeleteTags(ctx context.Context, resourceARN string, tagKeys []string) error

	AddAssociation(
		ctx context.Context,
		sourceArn, destinationArn, associationType string,
		tags map[string]string,
	) (*Association, error)
	AssociateTrialComponent(
		ctx context.Context,
		trialName, trialComponentName string,
	) (*TrialComponentAssociation, error)

	AttachClusterNodeVolume(
		ctx context.Context,
		clusterName, nodeID string,
		volume ClusterNodeVolume,
	) (string, string, error)
	BatchAddClusterNodes(ctx context.Context, clusterName string, nodeConfigs []ClusterNode) (string, []string, error)
	BatchDeleteClusterNodes(
		ctx context.Context,
		clusterName string,
		nodeIDs []string,
	) (string, []string, []string, error)
	BatchDescribeModelPackage(ctx context.Context, modelPackageArns []string) map[string]ModelPackageBatchResult
	BatchRebootClusterNodes(
		ctx context.Context,
		clusterName string,
		nodeIDs []string,
	) (string, []string, []string, error)
	BatchReplaceClusterNodes(ctx context.Context, clusterName string, nodes []ClusterNode) (string, []string, error)

	CreateAction(
		ctx context.Context,
		name, actionType, description, status string,
		source ActionSource,
		properties map[string]string,
		tags map[string]string,
	) (*Action, error)
	CreateAlgorithm(ctx context.Context, name, description string, tags map[string]string) (*Algorithm, error)

	CreateEndpoint(ctx context.Context, name, endpointConfigName string, tags map[string]string) (*Endpoint, error)
	DescribeEndpoint(ctx context.Context, name string) (*Endpoint, error)
	ListEndpoints(ctx context.Context, nextToken string) ([]*Endpoint, string)
	DeleteEndpoint(ctx context.Context, name string) error
	UpdateEndpoint(ctx context.Context, name, endpointConfigName string) (*Endpoint, error)

	CreateTrainingJob(
		ctx context.Context,
		name, roleArn string,
		algorithmSpec map[string]string,
		tags map[string]string,
	) (*TrainingJob, error)
	DescribeTrainingJob(ctx context.Context, name string) (*TrainingJob, error)
	ListTrainingJobs(ctx context.Context, nextToken string) ([]*TrainingJob, string)
	StopTrainingJob(ctx context.Context, name string) error
	DeleteTrainingJob(ctx context.Context, name string) error

	CreateNotebookInstance(
		ctx context.Context,
		name, instanceType, roleArn string,
		tags map[string]string,
	) (*NotebookInstance, error)
	DescribeNotebookInstance(ctx context.Context, name string) (*NotebookInstance, error)
	ListNotebookInstances(
		ctx context.Context,
		nextToken string,
		filter ListNotebookInstancesFilter,
	) ([]*NotebookInstance, string)
	DeleteNotebookInstance(ctx context.Context, name string) error
	StartNotebookInstance(ctx context.Context, name string) error
	StopNotebookInstance(ctx context.Context, name string) error
	UpdateNotebookInstance(ctx context.Context, name, instanceType string) error
	CreatePresignedNotebookInstanceURL(ctx context.Context, name string) (string, error)

	CreateHyperParameterTuningJob(
		ctx context.Context,
		name, strategy string,
		tags map[string]string,
	) (*HyperParameterTuningJob, error)
	DescribeHyperParameterTuningJob(ctx context.Context, name string) (*HyperParameterTuningJob, error)
	ListHyperParameterTuningJobs(ctx context.Context, nextToken string) ([]*HyperParameterTuningJob, string)
	StopHyperParameterTuningJob(ctx context.Context, name string) error
	DeleteHyperParameterTuningJob(ctx context.Context, name string) error

	Reset()
	Region() string
	AccountID() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Ensure InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

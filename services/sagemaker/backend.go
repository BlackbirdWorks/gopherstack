package sagemaker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// generateID returns a 24-char random hex string (12 random bytes).
func generateID() string {
	b := make([]byte, idByteLen)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

const (
	statusRunning = "Running"
)

const sagemakerDefaultPageSize = 100

const (
	algorithmStatusCompleted   = "Completed"
	clusterStatusInService     = "InService"
	modelPackageStatusApproved = "Approved"
)

var (
	// ErrModelNotFound is returned when a model does not exist.
	ErrModelNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrModelAlreadyExists is returned when a model already exists.
	ErrModelAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrEndpointConfigNotFound is returned when an endpoint config does not exist.
	ErrEndpointConfigNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrEndpointConfigAlreadyExists is returned when an endpoint config already exists.
	ErrEndpointConfigAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrAssociationNotFound is returned when an association does not exist.
	ErrAssociationNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrAssociationAlreadyExists is returned when an association already exists.
	ErrAssociationAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrActionNotFound is returned when an action does not exist.
	ErrActionNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrActionAlreadyExists is returned when an action already exists.
	ErrActionAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrAlgorithmNotFound is returned when an algorithm does not exist.
	ErrAlgorithmNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrAlgorithmAlreadyExists is returned when an algorithm already exists.
	ErrAlgorithmAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrClusterNotFound is returned when a cluster does not exist.
	ErrClusterNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrClusterAlreadyExists is returned when a cluster with the same name already exists.
	ErrClusterAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrModelPackageNotFound is returned when a model package does not exist.
	ErrModelPackageNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrValidation is returned for invalid input parameters.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrArtifactNotFound is returned when a lineage artifact does not exist.
	ErrArtifactNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrArtifactAlreadyExists is returned when a lineage artifact already exists.
	ErrArtifactAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrContextNotFound is returned when a lineage context does not exist.
	ErrContextNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrContextAlreadyExists is returned when a lineage context already exists.
	ErrContextAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrLineageGroupNotFound is returned when a lineage group does not exist.
	ErrLineageGroupNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrLineageGroupPolicyNotFound is returned when a lineage group has no resource policy attached.
	ErrLineageGroupPolicyNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
)

// ImageConfig specifies where SageMaker should pull the container image from.
type ImageConfig struct {
	RepositoryAccessMode string `json:"RepositoryAccessMode,omitempty"`
}

// MultiModelConfig specifies the multi-model serving mode.
type MultiModelConfig struct {
	ModelCacheSetting string `json:"ModelCacheSetting,omitempty"`
}

// ContainerDefinition holds image details for a model container.
type ContainerDefinition struct {
	Environment                map[string]string `json:"Environment,omitempty"`
	ImageConfig                *ImageConfig      `json:"ImageConfig,omitempty"`
	MultiModelConfig           *MultiModelConfig `json:"MultiModelConfig,omitempty"`
	Image                      string            `json:"Image,omitempty"`
	ModelDataURL               string            `json:"ModelDataUrl,omitempty"`
	ModelPackageName           string            `json:"ModelPackageName,omitempty"`
	ModelDataSource            string            `json:"ModelDataSource,omitempty"`
	InferenceSpecificationName string            `json:"InferenceSpecificationName,omitempty"`
	ContainerHostname          string            `json:"ContainerHostname,omitempty"`
	Mode                       string            `json:"Mode,omitempty"`
}

// InferenceExecutionConfig controls serial vs. direct routing in multi-container models.
type InferenceExecutionConfig struct {
	Mode string `json:"Mode,omitempty"` // Serial or Direct
}

// Model represents a SageMaker model.
type Model struct {
	CreationTime             time.Time                 `json:"CreationTime"`
	Tags                     map[string]string         `json:"Tags,omitempty"`
	VpcConfig                *VpcConfig                `json:"VpcConfig,omitempty"`
	InferenceExecutionConfig *InferenceExecutionConfig `json:"InferenceExecutionConfig,omitempty"`
	ModelName                string                    `json:"ModelName"`
	ModelARN                 string                    `json:"ModelARN"`
	ExecutionRoleARN         string                    `json:"ExecutionRoleARN"`
	PrimaryContainer         *ContainerDefinition      `json:"PrimaryContainer,omitempty"`
	Containers               []ContainerDefinition     `json:"Containers,omitempty"`
	EnableNetworkIsolation   bool                      `json:"EnableNetworkIsolation,omitempty"`
}

// cloneContainer returns a deep copy of a ContainerDefinition, including its Environment map.
func cloneContainer(c ContainerDefinition) ContainerDefinition {
	c.Environment = maps.Clone(c.Environment)
	if c.ImageConfig != nil {
		ic := *c.ImageConfig
		c.ImageConfig = &ic
	}
	if c.MultiModelConfig != nil {
		mc := *c.MultiModelConfig
		c.MultiModelConfig = &mc
	}

	return c
}

// cloneModel returns a deep copy of m, including nested maps and slices.
func cloneModel(m *Model) *Model {
	cp := *m
	cp.Tags = maps.Clone(m.Tags)

	if m.PrimaryContainer != nil {
		pc := cloneContainer(*m.PrimaryContainer)
		cp.PrimaryContainer = &pc
	}

	cp.Containers = make([]ContainerDefinition, len(m.Containers))

	for i, c := range m.Containers {
		cp.Containers[i] = cloneContainer(c)
	}

	if m.VpcConfig != nil {
		vpc := *m.VpcConfig
		vpc.SecurityGroupIDs = append([]string(nil), m.VpcConfig.SecurityGroupIDs...)
		vpc.Subnets = append([]string(nil), m.VpcConfig.Subnets...)
		cp.VpcConfig = &vpc
	}
	if m.InferenceExecutionConfig != nil {
		iec := *m.InferenceExecutionConfig
		cp.InferenceExecutionConfig = &iec
	}

	return &cp
}

// ServerlessConfig configures serverless inference for a variant.
type ServerlessConfig struct {
	MemorySizeInMB         int32 `json:"MemorySizeInMB,omitempty"`
	MaxConcurrency         int32 `json:"MaxConcurrency,omitempty"`
	ProvisionedConcurrency int32 `json:"ProvisionedConcurrency,omitempty"`
}

// CoreDumpConfig specifies where core dumps are stored.
type CoreDumpConfig struct {
	DestinationS3Uri string `json:"DestinationS3Uri,omitempty"`
	KmsKeyID         string `json:"KmsKeyId,omitempty"`
}

// ProductionVariant holds configuration for a production variant in an endpoint config.
type ProductionVariant struct {
	ServerlessConfig                            *ServerlessConfig `json:"ServerlessConfig,omitempty"`
	CoreDumpConfig                              *CoreDumpConfig   `json:"CoreDumpConfig,omitempty"`
	VariantName                                 string            `json:"VariantName"`
	ModelName                                   string            `json:"ModelName"`
	AcceleratorType                             string            `json:"AcceleratorType,omitempty"`
	InstanceType                                string            `json:"InstanceType,omitempty"`
	InferenceAmiVersion                         string            `json:"InferenceAmiVersion,omitempty"`
	InitialVariantWeight                        float64           `json:"InitialVariantWeight,omitempty"`
	InitialInstanceCount                        int32             `json:"InitialInstanceCount,omitempty"`
	VolumeSizeInGB                              int32             `json:"VolumeSizeInGB,omitempty"`
	ModelDataDownloadTimeoutInSeconds           int32             `json:"ModelDataDownloadTimeoutInSeconds,omitempty"`
	ContainerStartupHealthCheckTimeoutInSeconds int32             `json:"ContainerStartupHealthCheckTimeoutInSeconds,omitempty"` //nolint:lll // AWS API field name exceeds 120 chars; cannot be shortened
	EnableSSMAccess                             bool              `json:"EnableSSMAccess,omitempty"`
}

// DataCaptureConfig specifies real-time data capture for an endpoint config.
type DataCaptureConfig struct {
	DestinationS3Uri          string `json:"DestinationS3Uri"`
	CaptureMode               string `json:"CaptureMode,omitempty"`
	KmsKeyID                  string `json:"KmsKeyId,omitempty"`
	InitialSamplingPercentage int32  `json:"InitialSamplingPercentage,omitempty"`
	EnableCapture             bool   `json:"EnableCapture,omitempty"`
}

// AsyncInferenceConfig configures asynchronous inference for an endpoint.
type AsyncInferenceConfig struct {
	OutputConfig AsyncOutputConfig `json:"OutputConfig"`
}

// AsyncOutputConfig specifies the async inference output location.
type AsyncOutputConfig struct {
	S3OutputPath string `json:"S3OutputPath"`
	KmsKeyID     string `json:"KmsKeyId,omitempty"`
}

// EndpointConfig represents a SageMaker endpoint configuration.
type EndpointConfig struct {
	CreationTime             time.Time             `json:"CreationTime"`
	Tags                     map[string]string     `json:"Tags,omitempty"`
	VpcConfig                *VpcConfig            `json:"VpcConfig,omitempty"`
	DataCaptureConfig        *DataCaptureConfig    `json:"DataCaptureConfig,omitempty"`
	AsyncInferenceConfig     *AsyncInferenceConfig `json:"AsyncInferenceConfig,omitempty"`
	EndpointConfigName       string                `json:"EndpointConfigName"`
	EndpointConfigARN        string                `json:"EndpointConfigARN"`
	ExecutionRoleArn         string                `json:"ExecutionRoleArn,omitempty"`
	KmsKeyID                 string                `json:"KmsKeyId,omitempty"`
	ProductionVariants       []ProductionVariant   `json:"ProductionVariants,omitempty"`
	ShadowProductionVariants []ProductionVariant   `json:"ShadowProductionVariants,omitempty"`
	EnableNetworkIsolation   bool                  `json:"EnableNetworkIsolation,omitempty"`
}

// cloneProductionVariant returns a deep copy of a ProductionVariant.
func cloneProductionVariant(pv ProductionVariant) ProductionVariant {
	if pv.ServerlessConfig != nil {
		sc := *pv.ServerlessConfig
		pv.ServerlessConfig = &sc
	}
	if pv.CoreDumpConfig != nil {
		cdc := *pv.CoreDumpConfig
		pv.CoreDumpConfig = &cdc
	}

	return pv
}

// cloneEndpointConfig returns a deep copy of ec.
func cloneEndpointConfig(ec *EndpointConfig) *EndpointConfig {
	cp := *ec
	cp.Tags = maps.Clone(ec.Tags)
	cp.ProductionVariants = make([]ProductionVariant, len(ec.ProductionVariants))
	for i, pv := range ec.ProductionVariants {
		cp.ProductionVariants[i] = cloneProductionVariant(pv)
	}
	cp.ShadowProductionVariants = make([]ProductionVariant, len(ec.ShadowProductionVariants))
	for i, pv := range ec.ShadowProductionVariants {
		cp.ShadowProductionVariants[i] = cloneProductionVariant(pv)
	}
	if ec.VpcConfig != nil {
		vpc := *ec.VpcConfig
		vpc.SecurityGroupIDs = append([]string(nil), ec.VpcConfig.SecurityGroupIDs...)
		vpc.Subnets = append([]string(nil), ec.VpcConfig.Subnets...)
		cp.VpcConfig = &vpc
	}
	if ec.DataCaptureConfig != nil {
		dcc := *ec.DataCaptureConfig
		cp.DataCaptureConfig = &dcc
	}
	if ec.AsyncInferenceConfig != nil {
		aic := *ec.AsyncInferenceConfig
		cp.AsyncInferenceConfig = &aic
	}

	return &cp
}

// Association represents a SageMaker ML lineage association.
type Association struct {
	CreationTime    time.Time         `json:"CreationTime"`
	Tags            map[string]string `json:"Tags,omitempty"`
	SourceArn       string            `json:"SourceArn"`
	DestinationArn  string            `json:"DestinationArn"`
	AssociationType string            `json:"AssociationType,omitempty"`
	AssociationArn  string            `json:"AssociationArn"`
}

// TrialComponentAssociation tracks which trial components are associated with a trial.
type TrialComponentAssociation struct {
	CreationTime       time.Time `json:"CreationTime"`
	TrialName          string    `json:"TrialName"`
	TrialComponentName string    `json:"TrialComponentName"`
	TrialArn           string    `json:"TrialArn"`
	TrialComponentArn  string    `json:"TrialComponentArn"`
}

// ActionSource represents the source of a SageMaker action.
type ActionSource struct {
	SourceURI  string `json:"SourceUri"`
	SourceType string `json:"SourceType,omitempty"`
}

// Action represents a SageMaker ML lineage action.
type Action struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Properties       map[string]string `json:"Properties,omitempty"`
	Source           ActionSource      `json:"Source"`
	ActionName       string            `json:"ActionName"`
	ActionArn        string            `json:"ActionArn"`
	ActionType       string            `json:"ActionType"`
	Description      string            `json:"Description,omitempty"`
	Status           string            `json:"Status,omitempty"`
}

// cloneAction returns a deep copy of a.
func cloneAction(a *Action) *Action {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)
	cp.Properties = maps.Clone(a.Properties)

	return &cp
}

// AlgorithmStatusItem represents the status of a single algorithm image scan
// or validation check.
type AlgorithmStatusItem struct {
	Name          string `json:"Name"`
	Status        string `json:"Status"`
	FailureReason string `json:"FailureReason,omitempty"`
}

// AlgorithmStatusDetails represents the detailed status of an algorithm.
type AlgorithmStatusDetails struct {
	ImageScanStatuses  []AlgorithmStatusItem `json:"ImageScanStatuses"`
	ValidationStatuses []AlgorithmStatusItem `json:"ValidationStatuses"`
}

// Algorithm represents a SageMaker algorithm specification.
type Algorithm struct {
	CreationTime            time.Time              `json:"CreationTime"`
	Tags                    map[string]string      `json:"Tags,omitempty"`
	AlgorithmName           string                 `json:"AlgorithmName"`
	AlgorithmArn            string                 `json:"AlgorithmArn"`
	AlgorithmDescription    string                 `json:"AlgorithmDescription,omitempty"`
	AlgorithmStatus         string                 `json:"AlgorithmStatus"`
	ProductID               string                 `json:"ProductId,omitempty"`
	AlgorithmStatusDetails  AlgorithmStatusDetails `json:"AlgorithmStatusDetails"`
	TrainingSpecification   json.RawMessage        `json:"TrainingSpecification,omitempty"`
	InferenceSpecification  json.RawMessage        `json:"InferenceSpecification,omitempty"`
	ValidationSpecification json.RawMessage        `json:"ValidationSpecification,omitempty"`
	CertifyForMarketplace   bool                   `json:"CertifyForMarketplace,omitempty"`
}

// cloneAlgorithm returns a deep copy of al.
func cloneAlgorithm(al *Algorithm) *Algorithm {
	cp := *al
	cp.Tags = maps.Clone(al.Tags)
	cp.TrainingSpecification = append(json.RawMessage(nil), al.TrainingSpecification...)
	cp.InferenceSpecification = append(json.RawMessage(nil), al.InferenceSpecification...)
	cp.ValidationSpecification = append(json.RawMessage(nil), al.ValidationSpecification...)
	cp.AlgorithmStatusDetails.ImageScanStatuses = append(
		[]AlgorithmStatusItem(nil), al.AlgorithmStatusDetails.ImageScanStatuses...,
	)
	cp.AlgorithmStatusDetails.ValidationStatuses = append(
		[]AlgorithmStatusItem(nil), al.AlgorithmStatusDetails.ValidationStatuses...,
	)

	return &cp
}

// cloneAssociation returns a deep copy of a.
func cloneAssociation(a *Association) *Association {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)

	return &cp
}

// cloneTrialComponentAssociation returns a deep copy of a.
func cloneTrialComponentAssociation(a *TrialComponentAssociation) *TrialComponentAssociation {
	cp := *a

	return &cp
}

// cloneCluster returns a deep copy of c including Nodes.
func cloneCluster(c *Cluster) *Cluster {
	cp := *c
	cp.Nodes = make(map[string]*ClusterNode, len(c.Nodes))

	for k, v := range c.Nodes {
		nodeCopy := *v
		cp.Nodes[k] = &nodeCopy
	}

	cp.InstanceGroups = make([]ClusterInstanceGroup, len(c.InstanceGroups))
	copy(cp.InstanceGroups, c.InstanceGroups)
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

// ClusterNodeVolume represents a volume attached to a cluster node.
type ClusterNodeVolume struct {
	VolumeName string `json:"VolumeName"`
	SizeInGB   int32  `json:"SizeInGB,omitempty"`
}

// ClusterNode represents a node in a SageMaker cluster.
type ClusterNode struct {
	NodeID            string              `json:"NodeId"`
	InstanceType      string              `json:"InstanceType,omitempty"`
	NodeStatus        string              `json:"NodeStatus"`
	InstanceGroupName string              `json:"InstanceGroupName,omitempty"`
	Volumes           []ClusterNodeVolume `json:"Volumes,omitempty"`
}

// ClusterInstanceGroup represents an instance group specification/details for a
// SageMaker HyperPod cluster (a merged view of the AWS
// ClusterInstanceGroupSpecification and ClusterInstanceGroupDetails shapes).
type ClusterInstanceGroup struct {
	InstanceGroupName string `json:"InstanceGroupName"`
	InstanceType      string `json:"InstanceType,omitempty"`
	ExecutionRole     string `json:"ExecutionRole,omitempty"`
	InstanceCount     int32  `json:"InstanceCount,omitempty"`
}

// Cluster represents a SageMaker HyperPod cluster.
type Cluster struct {
	CreationTime   time.Time               `json:"CreationTime"`
	Nodes          map[string]*ClusterNode `json:"-"`
	Tags           map[string]string       `json:"Tags,omitempty"`
	ClusterArn     string                  `json:"ClusterArn"`
	ClusterName    string                  `json:"ClusterName"`
	ClusterStatus  string                  `json:"ClusterStatus"`
	NodeRecovery   string                  `json:"NodeRecovery,omitempty"`
	InstanceGroups []ClusterInstanceGroup  `json:"InstanceGroups,omitempty"`
}

// ModelPackage represents a SageMaker model package.
type ModelPackage struct {
	CreationTime            time.Time         `json:"CreationTime"`
	Tags                    map[string]string `json:"Tags,omitempty"`
	ModelPackageName        string            `json:"ModelPackageName"`
	ModelPackageArn         string            `json:"ModelPackageArn"`
	ModelPackageGroupName   string            `json:"ModelPackageGroupName,omitempty"`
	ModelPackageStatus      string            `json:"ModelPackageStatus"`
	ModelApprovalStatus     string            `json:"ModelApprovalStatus,omitempty"`
	ModelPackageDescription string            `json:"ModelPackageDescription,omitempty"`
}

// cloneModelPackage returns a deep copy of mp.
func cloneModelPackage(mp *ModelPackage) *ModelPackage {
	cp := *mp
	cp.Tags = maps.Clone(mp.Tags)

	return &cp
}

// InMemoryBackend is an in-memory store for SageMaker resources.
//
// All resource maps are nested by region (outer key = region) so that
// same-named resources are isolated across regions. The per-region inner maps
// are created lazily via the *Store helpers. Callers must hold b.mu while
// accessing the inner maps.
type InMemoryBackend struct {
	models                     map[string]*store.Table[Model]
	endpointConfigs            map[string]*store.Table[EndpointConfig]
	endpoints                  map[string]*store.Table[Endpoint]
	trainingJobs               map[string]*store.Table[TrainingJob]
	notebooks                  map[string]*store.Table[NotebookInstance]
	hpTuningJobs               map[string]*store.Table[HyperParameterTuningJob]
	associations               map[string]*store.Table[Association]
	trialComponentAssociations map[string]*store.Table[TrialComponentAssociation]
	actions                    map[string]*store.Table[Action]
	artifacts                  map[string]*store.Table[Artifact] // region -> ArtifactArn -> Artifact
	contexts                   map[string]*store.Table[Context]  // region -> ContextName -> Context
	algorithms                 map[string]*store.Table[Algorithm]
	clusters                   map[string]*store.Table[Cluster]
	modelPackages              map[string]*store.Table[ModelPackage]
	modelPackageGroups         map[string]*store.Table[ModelPackageGroup]
	autoMLJobs                 map[string]*store.Table[AutoMLJob]
	codeRepositories           map[string]*store.Table[CodeRepository]
	projects                   map[string]*store.Table[Project]
	spaces                     map[string]*store.Table[Space]
	smImages                   map[string]*store.Table[SMImage]
	imageVersions              map[string]map[string]map[int]*ImageVersion // region → imageName → version → ImageVersion
	imageVersionCounts         map[string]map[string]int                   // region → imageName → latest version number
	compilationJobs            map[string]*store.Table[CompilationJob]
	monitoringSchedules        map[string]*store.Table[MonitoringSchedule]
	workteams                  map[string]*store.Table[Workteam]
	labelingJobs               map[string]*store.Table[LabelingJob]
	dataQualityJobDefs         map[string]*store.Table[JobDefinition]
	modelBiasJobDefs           map[string]*store.Table[JobDefinition]
	modelQualityJobDefs        map[string]*store.Table[JobDefinition]
	modelExplainJobDefs        map[string]*store.Table[JobDefinition]
	// monitoringAlerts is region -> store.Table[MonitoringAlert], keyed by
	// monitoringAlertKey(scheduleName, alertName).
	monitoringAlerts map[string]*store.Table[MonitoringAlert]
	// monitoringAlertHistory is region -> history entries.
	monitoringAlertHistory map[string][]*MonitoringAlertHistoryEntry
	// monitoringExecutions is region -> "scheduleName|processingJobArn" -> execution.
	monitoringExecutions   map[string]*store.Table[MonitoringExecution]
	humanTaskUis           map[string]*store.Table[HumanTaskUI]
	workforces             map[string]*store.Table[Workforce]
	flowDefinitions        map[string]*store.Table[FlowDefinition]
	appImageConfigs        map[string]*store.Table[AppImageConfig]
	inferenceExperiments   map[string]*store.Table[InferenceExperiment]
	mlflowTrackingServers  map[string]*store.Table[MlflowTrackingServer]
	mlflowApps             map[string]*store.Table[MlflowApp]
	modelCards             map[string]*store.Table[ModelCard]
	optimizationJobs       map[string]*store.Table[OptimizationJob]
	studioLifecycleConfigs map[string]*store.Table[StudioLifecycleConfig]
	partnerApps            map[string]*store.Table[PartnerApp]
	trainingPlans          map[string]*store.Table[TrainingPlan]
	reservedCapacities     map[string]*store.Table[ReservedCapacity]
	// trainingPlanExtensionOfferings is region -> extensionOfferingID -> pending extension offer.
	trainingPlanExtensionOfferings map[string]*store.Table[pendingTrainingPlanExtension]
	// modelCardExportJobs is region -> ModelCardExportJobArn -> job.
	modelCardExportJobs          map[string]*store.Table[ModelCardExportJob]
	modelARNIndex                map[string]map[string]string // region → ARN → model name
	endpointConfigARNIndex       map[string]map[string]string // region → ARN → endpoint config name
	endpointARNIndex             map[string]map[string]string // region → ARN → endpoint name
	trainingJobARNIndex          map[string]map[string]string // region → ARN → training job name
	notebookARNIndex             map[string]map[string]string // region → ARN → notebook instance name
	hpTuningJobARNIndex          map[string]map[string]string // region → ARN → HP tuning job name
	actionARNIndex               map[string]map[string]string // region → ARN → action name
	contextARNIndex              map[string]map[string]string // region → ARN → context name
	algorithmARNIndex            map[string]map[string]string // region → ARN → algorithm name
	clusterARNIndex              map[string]map[string]string // region → ARN → cluster name
	modelPackageARNIndex         map[string]map[string]string // region → ARN → model package ARN
	processingJobARNIndex        map[string]map[string]string // region → ARN → job name
	transformJobARNIndex         map[string]map[string]string // region → ARN → job name
	domains                      map[string]*store.Table[Domain]
	userProfiles                 map[string]*store.Table[UserProfile]
	apps                         map[string]*store.Table[App]
	featureGroups                map[string]*store.Table[FeatureGroup]
	featureRecords               map[string]*store.Table[FeatureRecord]
	featureMetadata              map[string]*store.Table[FeatureMetadata]
	pipelines                    map[string]*store.Table[Pipeline]
	pipelineExecutions           map[string]*store.Table[PipelineExecution]
	pipelineExecSteps            map[string]*store.Table[PipelineExecutionStep]
	experiments                  map[string]*store.Table[Experiment]
	trials                       map[string]*store.Table[Trial]
	trialComponents              map[string]*store.Table[TrialComponent]
	notebookLifecycleConfigs     map[string]*store.Table[NotebookInstanceLifecycleConfig]
	processingJobs               map[string]*store.Table[ProcessingJob]
	transformJobs                map[string]*store.Table[TransformJob]
	edgePackagingJobs            map[string]*store.Table[EdgePackagingJob]
	edgeDeploymentPlans          map[string]*store.Table[EdgeDeploymentPlan]
	inferenceRecommendationsJobs map[string]*store.Table[InferenceRecommendationsJob]
	deviceFleets                 map[string]*store.Table[DeviceFleet]
	devices                      map[string]*store.Table[Device]
	inferenceComponents          map[string]*store.Table[InferenceComponent]
	clusterSchedulerConfigs      map[string]*store.Table[ClusterSchedulerConfig]
	computeQuotas                map[string]*store.Table[ComputeQuota]
	hubs                         map[string]*store.Table[Hub]
	hubContents                  map[string]*store.Table[HubContent]
	// pipelineVersions is region -> pipelineName -> versions, ordered oldest-first.
	pipelineVersions map[string]map[string][]*PipelineVersion
	// servicecatalogPortfolioEnabled is region -> whether the SageMaker
	// Service Catalog portfolio has been enabled via
	// EnableSagemakerServicecatalogPortfolio. Absent/false means Disabled.
	servicecatalogPortfolioEnabled map[string]bool
	// registry lets Reset collapse the per-region store.Table lifecycle for
	// every resource collection below to one registry.ResetAll() call, and
	// backs Snapshot/Restore via registry.SnapshotAll()/RestoreAll(). Each
	// resource field above is itself a map[string]*store.Table[T] keyed by
	// region (one store.Table per region, registered lazily on first use of
	// that region) rather than a single flat store.Table[T], because these
	// resources are natively region-partitioned collections; see the
	// xxxStore(r) helpers below for the lazy-create-and-register point.
	registry        *store.Registry
	lifecycleParent context.Context
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
	wg              sync.WaitGroup
}

// NewInMemoryBackend creates a new in-memory SageMaker backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new in-memory SageMaker backend whose
// lifecycle goroutines (status-transition simulators) are children of svcCtx, so
// they are cancelled when the service shuts down rather than leaking. If svcCtx is
// nil, [context.Background] is used.
//
//nolint:funlen // must initialise every resource map field; splitting would obscure the invariant
func NewInMemoryBackendWithContext(
	svcCtx context.Context,
	accountID, region string,
) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	b := &InMemoryBackend{
		lifecycleParent:              svcCtx,
		models:                       make(map[string]*store.Table[Model]),
		endpointConfigs:              make(map[string]*store.Table[EndpointConfig]),
		endpoints:                    make(map[string]*store.Table[Endpoint]),
		trainingJobs:                 make(map[string]*store.Table[TrainingJob]),
		notebooks:                    make(map[string]*store.Table[NotebookInstance]),
		hpTuningJobs:                 make(map[string]*store.Table[HyperParameterTuningJob]),
		associations:                 make(map[string]*store.Table[Association]),
		trialComponentAssociations:   make(map[string]*store.Table[TrialComponentAssociation]),
		actions:                      make(map[string]*store.Table[Action]),
		artifacts:                    make(map[string]*store.Table[Artifact]),
		contexts:                     make(map[string]*store.Table[Context]),
		algorithms:                   make(map[string]*store.Table[Algorithm]),
		clusters:                     make(map[string]*store.Table[Cluster]),
		modelPackages:                make(map[string]*store.Table[ModelPackage]),
		modelPackageGroups:           make(map[string]*store.Table[ModelPackageGroup]),
		autoMLJobs:                   make(map[string]*store.Table[AutoMLJob]),
		codeRepositories:             make(map[string]*store.Table[CodeRepository]),
		projects:                     make(map[string]*store.Table[Project]),
		spaces:                       make(map[string]*store.Table[Space]),
		smImages:                     make(map[string]*store.Table[SMImage]),
		imageVersions:                make(map[string]map[string]map[int]*ImageVersion),
		imageVersionCounts:           make(map[string]map[string]int),
		compilationJobs:              make(map[string]*store.Table[CompilationJob]),
		monitoringSchedules:          make(map[string]*store.Table[MonitoringSchedule]),
		workteams:                    make(map[string]*store.Table[Workteam]),
		labelingJobs:                 make(map[string]*store.Table[LabelingJob]),
		dataQualityJobDefs:           make(map[string]*store.Table[JobDefinition]),
		modelBiasJobDefs:             make(map[string]*store.Table[JobDefinition]),
		modelQualityJobDefs:          make(map[string]*store.Table[JobDefinition]),
		modelExplainJobDefs:          make(map[string]*store.Table[JobDefinition]),
		monitoringAlerts:             make(map[string]*store.Table[MonitoringAlert]),
		monitoringAlertHistory:       make(map[string][]*MonitoringAlertHistoryEntry),
		monitoringExecutions:         make(map[string]*store.Table[MonitoringExecution]),
		humanTaskUis:                 make(map[string]*store.Table[HumanTaskUI]),
		workforces:                   make(map[string]*store.Table[Workforce]),
		flowDefinitions:              make(map[string]*store.Table[FlowDefinition]),
		appImageConfigs:              make(map[string]*store.Table[AppImageConfig]),
		inferenceExperiments:         make(map[string]*store.Table[InferenceExperiment]),
		mlflowTrackingServers:        make(map[string]*store.Table[MlflowTrackingServer]),
		mlflowApps:                   make(map[string]*store.Table[MlflowApp]),
		modelCards:                   make(map[string]*store.Table[ModelCard]),
		optimizationJobs:             make(map[string]*store.Table[OptimizationJob]),
		studioLifecycleConfigs:       make(map[string]*store.Table[StudioLifecycleConfig]),
		partnerApps:                  make(map[string]*store.Table[PartnerApp]),
		trainingPlans:                make(map[string]*store.Table[TrainingPlan]),
		modelARNIndex:                make(map[string]map[string]string),
		endpointConfigARNIndex:       make(map[string]map[string]string),
		endpointARNIndex:             make(map[string]map[string]string),
		trainingJobARNIndex:          make(map[string]map[string]string),
		notebookARNIndex:             make(map[string]map[string]string),
		hpTuningJobARNIndex:          make(map[string]map[string]string),
		actionARNIndex:               make(map[string]map[string]string),
		contextARNIndex:              make(map[string]map[string]string),
		algorithmARNIndex:            make(map[string]map[string]string),
		clusterARNIndex:              make(map[string]map[string]string),
		modelPackageARNIndex:         make(map[string]map[string]string),
		processingJobARNIndex:        make(map[string]map[string]string),
		transformJobARNIndex:         make(map[string]map[string]string),
		domains:                      make(map[string]*store.Table[Domain]),
		userProfiles:                 make(map[string]*store.Table[UserProfile]),
		apps:                         make(map[string]*store.Table[App]),
		featureGroups:                make(map[string]*store.Table[FeatureGroup]),
		featureRecords:               make(map[string]*store.Table[FeatureRecord]),
		featureMetadata:              make(map[string]*store.Table[FeatureMetadata]),
		pipelines:                    make(map[string]*store.Table[Pipeline]),
		pipelineExecutions:           make(map[string]*store.Table[PipelineExecution]),
		pipelineExecSteps:            make(map[string]*store.Table[PipelineExecutionStep]),
		experiments:                  make(map[string]*store.Table[Experiment]),
		trials:                       make(map[string]*store.Table[Trial]),
		trialComponents:              make(map[string]*store.Table[TrialComponent]),
		notebookLifecycleConfigs:     make(map[string]*store.Table[NotebookInstanceLifecycleConfig]),
		processingJobs:               make(map[string]*store.Table[ProcessingJob]),
		transformJobs:                make(map[string]*store.Table[TransformJob]),
		edgePackagingJobs:            make(map[string]*store.Table[EdgePackagingJob]),
		edgeDeploymentPlans:          make(map[string]*store.Table[EdgeDeploymentPlan]),
		inferenceRecommendationsJobs: make(map[string]*store.Table[InferenceRecommendationsJob]),
		deviceFleets:                 make(map[string]*store.Table[DeviceFleet]),
		devices:                      make(map[string]*store.Table[Device]),
		inferenceComponents:          make(map[string]*store.Table[InferenceComponent]),
		clusterSchedulerConfigs:      make(map[string]*store.Table[ClusterSchedulerConfig]),
		computeQuotas:                make(map[string]*store.Table[ComputeQuota]),
		hubs:                         make(map[string]*store.Table[Hub]),
		hubContents:                  make(map[string]*store.Table[HubContent]),
		accountID:                    accountID,
		region:                       region,
		mu:                           lockmetrics.New("sagemaker"),
		registry:                     store.NewRegistry(),
	}
	b.initTrainingPlanExtMaps()
	b.resetLifecycleContext()

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// ---------------------------------------------------------------------------
// Per-region store helpers — lazy inner-map initialisation.
// Callers must hold b.mu.
// ---------------------------------------------------------------------------

func (b *InMemoryBackend) modelsStore(r string) *store.Table[Model] {
	if b.models[r] == nil {
		b.models[r] = store.Register(b.registry, "models:"+r, store.New(func(v *Model) string { return v.ModelName }))
	}

	return b.models[r]
}

// modelsStoreRO returns the region-scoped models table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelsStoreRO(r string) *store.Table[Model] {
	if v := b.models[r]; v != nil {
		return v
	}

	return store.New(func(v *Model) string { return v.ModelName })
}

func (b *InMemoryBackend) endpointConfigsStore(r string) *store.Table[EndpointConfig] {
	if b.endpointConfigs[r] == nil {
		b.endpointConfigs[r] = store.Register(
			b.registry,
			"endpointConfigs:"+r,
			store.New(func(v *EndpointConfig) string { return v.EndpointConfigName }),
		)
	}

	return b.endpointConfigs[r]
}

// endpointConfigsStoreRO returns the region-scoped endpointConfigs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) endpointConfigsStoreRO(r string) *store.Table[EndpointConfig] {
	if v := b.endpointConfigs[r]; v != nil {
		return v
	}

	return store.New(func(v *EndpointConfig) string { return v.EndpointConfigName })
}

func (b *InMemoryBackend) endpointsStore(r string) *store.Table[Endpoint] {
	if b.endpoints[r] == nil {
		b.endpoints[r] = store.Register(
			b.registry,
			"endpoints:"+r,
			store.New(func(v *Endpoint) string { return v.EndpointName }),
		)
	}

	return b.endpoints[r]
}

// endpointsStoreRO returns the region-scoped endpoints table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) endpointsStoreRO(r string) *store.Table[Endpoint] {
	if v := b.endpoints[r]; v != nil {
		return v
	}

	return store.New(func(v *Endpoint) string { return v.EndpointName })
}

func (b *InMemoryBackend) trainingJobsStore(r string) *store.Table[TrainingJob] {
	if b.trainingJobs[r] == nil {
		b.trainingJobs[r] = store.Register(
			b.registry,
			"trainingJobs:"+r,
			store.New(func(v *TrainingJob) string { return v.TrainingJobName }),
		)
	}

	return b.trainingJobs[r]
}

// trainingJobsStoreRO returns the region-scoped trainingJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trainingJobsStoreRO(r string) *store.Table[TrainingJob] {
	if v := b.trainingJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *TrainingJob) string { return v.TrainingJobName })
}

func (b *InMemoryBackend) notebooksStore(r string) *store.Table[NotebookInstance] {
	if b.notebooks[r] == nil {
		b.notebooks[r] = store.Register(
			b.registry,
			"notebooks:"+r,
			store.New(func(v *NotebookInstance) string { return v.NotebookInstanceName }),
		)
	}

	return b.notebooks[r]
}

// notebooksStoreRO returns the region-scoped notebooks table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) notebooksStoreRO(r string) *store.Table[NotebookInstance] {
	if v := b.notebooks[r]; v != nil {
		return v
	}

	return store.New(func(v *NotebookInstance) string { return v.NotebookInstanceName })
}

func (b *InMemoryBackend) hpTuningJobsStore(r string) *store.Table[HyperParameterTuningJob] {
	if b.hpTuningJobs[r] == nil {
		b.hpTuningJobs[r] = store.Register(
			b.registry,
			"hpTuningJobs:"+r,
			store.New(func(v *HyperParameterTuningJob) string { return v.HyperParameterTuningJobName }),
		)
	}

	return b.hpTuningJobs[r]
}

// hpTuningJobsStoreRO returns the region-scoped hpTuningJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) hpTuningJobsStoreRO(r string) *store.Table[HyperParameterTuningJob] {
	if v := b.hpTuningJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *HyperParameterTuningJob) string { return v.HyperParameterTuningJobName })
}

func (b *InMemoryBackend) associationsStore(r string) *store.Table[Association] {
	if b.associations[r] == nil {
		b.associations[r] = store.Register(
			b.registry,
			"associations:"+r,
			store.New(func(v *Association) string { return associationKey(v.SourceArn, v.DestinationArn) }),
		)
	}

	return b.associations[r]
}

// associationsStoreRO returns the region-scoped associations table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) associationsStoreRO(r string) *store.Table[Association] {
	if v := b.associations[r]; v != nil {
		return v
	}

	return store.New(func(v *Association) string { return associationKey(v.SourceArn, v.DestinationArn) })
}

func (b *InMemoryBackend) trialComponentAssociationsStore(r string) *store.Table[TrialComponentAssociation] {
	if b.trialComponentAssociations[r] == nil {
		b.trialComponentAssociations[r] = store.Register(
			b.registry,
			"trialComponentAssociations:"+r,
			store.New(
				func(v *TrialComponentAssociation) string { return trialComponentKey(v.TrialName, v.TrialComponentName) },
			),
		)
	}

	return b.trialComponentAssociations[r]
}

// trialComponentAssociationsStoreRO returns the region-scoped trialComponentAssociations table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trialComponentAssociationsStoreRO(r string) *store.Table[TrialComponentAssociation] {
	if v := b.trialComponentAssociations[r]; v != nil {
		return v
	}

	return store.New(
		func(v *TrialComponentAssociation) string { return trialComponentKey(v.TrialName, v.TrialComponentName) },
	)
}

func (b *InMemoryBackend) actionsStore(r string) *store.Table[Action] {
	if b.actions[r] == nil {
		b.actions[r] = store.Register(
			b.registry,
			"actions:"+r,
			store.New(func(v *Action) string { return v.ActionName }),
		)
	}

	return b.actions[r]
}

// actionsStoreRO returns the region-scoped actions table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) actionsStoreRO(r string) *store.Table[Action] {
	if v := b.actions[r]; v != nil {
		return v
	}

	return store.New(func(v *Action) string { return v.ActionName })
}

func (b *InMemoryBackend) algorithmsStore(r string) *store.Table[Algorithm] {
	if b.algorithms[r] == nil {
		b.algorithms[r] = store.Register(
			b.registry,
			"algorithms:"+r,
			store.New(func(v *Algorithm) string { return v.AlgorithmName }),
		)
	}

	return b.algorithms[r]
}

// algorithmsStoreRO returns the region-scoped algorithms table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) algorithmsStoreRO(r string) *store.Table[Algorithm] {
	if v := b.algorithms[r]; v != nil {
		return v
	}

	return store.New(func(v *Algorithm) string { return v.AlgorithmName })
}

func (b *InMemoryBackend) clustersStore(r string) *store.Table[Cluster] {
	if b.clusters[r] == nil {
		b.clusters[r] = store.Register(
			b.registry,
			"clusters:"+r,
			store.New(func(v *Cluster) string { return v.ClusterName }),
		)
	}

	return b.clusters[r]
}

// clustersStoreRO returns the region-scoped clusters table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) clustersStoreRO(r string) *store.Table[Cluster] {
	if v := b.clusters[r]; v != nil {
		return v
	}

	return store.New(func(v *Cluster) string { return v.ClusterName })
}

func (b *InMemoryBackend) modelPackagesStore(r string) *store.Table[ModelPackage] {
	if b.modelPackages[r] == nil {
		b.modelPackages[r] = store.Register(
			b.registry,
			"modelPackages:"+r,
			store.New(func(v *ModelPackage) string { return v.ModelPackageArn }),
		)
	}

	return b.modelPackages[r]
}

// modelPackagesStoreRO returns the region-scoped modelPackages table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelPackagesStoreRO(r string) *store.Table[ModelPackage] {
	if v := b.modelPackages[r]; v != nil {
		return v
	}

	return store.New(func(v *ModelPackage) string { return v.ModelPackageArn })
}

func (b *InMemoryBackend) modelPackageGroupsStore(r string) *store.Table[ModelPackageGroup] {
	if b.modelPackageGroups[r] == nil {
		b.modelPackageGroups[r] = store.Register(
			b.registry,
			"modelPackageGroups:"+r,
			store.New(func(v *ModelPackageGroup) string { return v.ModelPackageGroupName }),
		)
	}

	return b.modelPackageGroups[r]
}

// modelPackageGroupsStoreRO returns the region-scoped modelPackageGroups table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelPackageGroupsStoreRO(r string) *store.Table[ModelPackageGroup] {
	if v := b.modelPackageGroups[r]; v != nil {
		return v
	}

	return store.New(func(v *ModelPackageGroup) string { return v.ModelPackageGroupName })
}

func (b *InMemoryBackend) autoMLJobsStore(r string) *store.Table[AutoMLJob] {
	if b.autoMLJobs[r] == nil {
		b.autoMLJobs[r] = store.Register(
			b.registry,
			"autoMLJobs:"+r,
			store.New(func(v *AutoMLJob) string { return v.AutoMLJobName }),
		)
	}

	return b.autoMLJobs[r]
}

// autoMLJobsStoreRO returns the region-scoped autoMLJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) autoMLJobsStoreRO(r string) *store.Table[AutoMLJob] {
	if v := b.autoMLJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *AutoMLJob) string { return v.AutoMLJobName })
}

func (b *InMemoryBackend) codeRepositoriesStore(r string) *store.Table[CodeRepository] {
	if b.codeRepositories[r] == nil {
		b.codeRepositories[r] = store.Register(
			b.registry,
			"codeRepositories:"+r,
			store.New(func(v *CodeRepository) string { return v.CodeRepositoryName }),
		)
	}

	return b.codeRepositories[r]
}

// codeRepositoriesStoreRO returns the region-scoped codeRepositories table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) codeRepositoriesStoreRO(r string) *store.Table[CodeRepository] {
	if v := b.codeRepositories[r]; v != nil {
		return v
	}

	return store.New(func(v *CodeRepository) string { return v.CodeRepositoryName })
}

func (b *InMemoryBackend) projectsStore(r string) *store.Table[Project] {
	if b.projects[r] == nil {
		b.projects[r] = store.Register(
			b.registry,
			"projects:"+r,
			store.New(func(v *Project) string { return v.ProjectName }),
		)
	}

	return b.projects[r]
}

// projectsStoreRO returns the region-scoped projects table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) projectsStoreRO(r string) *store.Table[Project] {
	if v := b.projects[r]; v != nil {
		return v
	}

	return store.New(func(v *Project) string { return v.ProjectName })
}

func (b *InMemoryBackend) spacesStore(r string) *store.Table[Space] {
	if b.spaces[r] == nil {
		b.spaces[r] = store.Register(b.registry, "spaces:"+r, store.New(func(v *Space) string {
			return spaceKey(v.DomainID, v.SpaceName)
		}))
	}

	return b.spaces[r]
}

// spacesStoreRO returns the region-scoped spaces table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) spacesStoreRO(r string) *store.Table[Space] {
	if v := b.spaces[r]; v != nil {
		return v
	}

	return store.New(func(v *Space) string {
		return spaceKey(v.DomainID, v.SpaceName)
	})
}

func (b *InMemoryBackend) smImagesStore(r string) *store.Table[SMImage] {
	if b.smImages[r] == nil {
		b.smImages[r] = store.Register(
			b.registry,
			"smImages:"+r,
			store.New(func(v *SMImage) string { return v.ImageName }),
		)
	}

	return b.smImages[r]
}

// smImagesStoreRO returns the region-scoped smImages table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) smImagesStoreRO(r string) *store.Table[SMImage] {
	if v := b.smImages[r]; v != nil {
		return v
	}

	return store.New(func(v *SMImage) string { return v.ImageName })
}

func (b *InMemoryBackend) imageVersionsStore(r string) map[string]map[int]*ImageVersion {
	if b.imageVersions[r] == nil {
		b.imageVersions[r] = make(map[string]map[int]*ImageVersion)
	}

	return b.imageVersions[r]
}

// imageVersionsStoreRO returns the region-scoped imageVersions table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) imageVersionsStoreRO(r string) map[string]map[int]*ImageVersion {
	if v := b.imageVersions[r]; v != nil {
		return v
	}

	return make(map[string]map[int]*ImageVersion)
}

func (b *InMemoryBackend) imageVersionCountsStore(r string) map[string]int {
	if b.imageVersionCounts[r] == nil {
		b.imageVersionCounts[r] = make(map[string]int)
	}

	return b.imageVersionCounts[r]
}

func (b *InMemoryBackend) compilationJobsStore(r string) *store.Table[CompilationJob] {
	if b.compilationJobs[r] == nil {
		b.compilationJobs[r] = store.Register(
			b.registry,
			"compilationJobs:"+r,
			store.New(func(v *CompilationJob) string { return v.CompilationJobName }),
		)
	}

	return b.compilationJobs[r]
}

// compilationJobsStoreRO returns the region-scoped compilationJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) compilationJobsStoreRO(r string) *store.Table[CompilationJob] {
	if v := b.compilationJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *CompilationJob) string { return v.CompilationJobName })
}

func (b *InMemoryBackend) monitoringSchedulesStore(r string) *store.Table[MonitoringSchedule] {
	if b.monitoringSchedules[r] == nil {
		b.monitoringSchedules[r] = store.Register(
			b.registry,
			"monitoringSchedules:"+r,
			store.New(func(v *MonitoringSchedule) string { return v.MonitoringScheduleName }),
		)
	}

	return b.monitoringSchedules[r]
}

// monitoringSchedulesStoreRO returns the region-scoped monitoringSchedules table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) monitoringSchedulesStoreRO(r string) *store.Table[MonitoringSchedule] {
	if v := b.monitoringSchedules[r]; v != nil {
		return v
	}

	return store.New(func(v *MonitoringSchedule) string { return v.MonitoringScheduleName })
}

func (b *InMemoryBackend) workteamsStore(r string) *store.Table[Workteam] {
	if b.workteams[r] == nil {
		b.workteams[r] = store.Register(
			b.registry,
			"workteams:"+r,
			store.New(func(v *Workteam) string { return v.WorkteamName }),
		)
	}

	return b.workteams[r]
}

// workteamsStoreRO returns the region-scoped workteams table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) workteamsStoreRO(r string) *store.Table[Workteam] {
	if v := b.workteams[r]; v != nil {
		return v
	}

	return store.New(func(v *Workteam) string { return v.WorkteamName })
}

func (b *InMemoryBackend) dataQualityJobDefsStore(r string) *store.Table[JobDefinition] {
	if b.dataQualityJobDefs[r] == nil {
		b.dataQualityJobDefs[r] = store.Register(
			b.registry,
			"dataQualityJobDefs:"+r,
			store.New(func(v *JobDefinition) string { return v.JobDefinitionName }),
		)
	}

	return b.dataQualityJobDefs[r]
}

// dataQualityJobDefsStoreRO returns the region-scoped dataQualityJobDefs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) dataQualityJobDefsStoreRO(r string) *store.Table[JobDefinition] {
	if v := b.dataQualityJobDefs[r]; v != nil {
		return v
	}

	return store.New(func(v *JobDefinition) string { return v.JobDefinitionName })
}

func (b *InMemoryBackend) modelBiasJobDefsStore(r string) *store.Table[JobDefinition] {
	if b.modelBiasJobDefs[r] == nil {
		b.modelBiasJobDefs[r] = store.Register(
			b.registry,
			"modelBiasJobDefs:"+r,
			store.New(func(v *JobDefinition) string { return v.JobDefinitionName }),
		)
	}

	return b.modelBiasJobDefs[r]
}

// modelBiasJobDefsStoreRO returns the region-scoped modelBiasJobDefs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelBiasJobDefsStoreRO(r string) *store.Table[JobDefinition] {
	if v := b.modelBiasJobDefs[r]; v != nil {
		return v
	}

	return store.New(func(v *JobDefinition) string { return v.JobDefinitionName })
}

func (b *InMemoryBackend) modelQualityJobDefsStore(r string) *store.Table[JobDefinition] {
	if b.modelQualityJobDefs[r] == nil {
		b.modelQualityJobDefs[r] = store.Register(
			b.registry,
			"modelQualityJobDefs:"+r,
			store.New(func(v *JobDefinition) string { return v.JobDefinitionName }),
		)
	}

	return b.modelQualityJobDefs[r]
}

// modelQualityJobDefsStoreRO returns the region-scoped modelQualityJobDefs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelQualityJobDefsStoreRO(r string) *store.Table[JobDefinition] {
	if v := b.modelQualityJobDefs[r]; v != nil {
		return v
	}

	return store.New(func(v *JobDefinition) string { return v.JobDefinitionName })
}

func (b *InMemoryBackend) modelExplainJobDefsStore(r string) *store.Table[JobDefinition] {
	if b.modelExplainJobDefs[r] == nil {
		b.modelExplainJobDefs[r] = store.Register(
			b.registry,
			"modelExplainJobDefs:"+r,
			store.New(func(v *JobDefinition) string { return v.JobDefinitionName }),
		)
	}

	return b.modelExplainJobDefs[r]
}

// modelExplainJobDefsStoreRO returns the region-scoped modelExplainJobDefs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelExplainJobDefsStoreRO(r string) *store.Table[JobDefinition] {
	if v := b.modelExplainJobDefs[r]; v != nil {
		return v
	}

	return store.New(func(v *JobDefinition) string { return v.JobDefinitionName })
}

func (b *InMemoryBackend) monitoringExecutionsStore(r string) *store.Table[MonitoringExecution] {
	if b.monitoringExecutions[r] == nil {
		b.monitoringExecutions[r] = store.Register(
			b.registry,
			"monitoringExecutions:"+r,
			store.New(
				func(v *MonitoringExecution) string { return v.MonitoringScheduleName + "|" + v.ProcessingJobArn },
			),
		)
	}

	return b.monitoringExecutions[r]
}

// monitoringExecutionsStoreRO returns the region-scoped monitoringExecutions table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) monitoringExecutionsStoreRO(r string) *store.Table[MonitoringExecution] {
	if v := b.monitoringExecutions[r]; v != nil {
		return v
	}

	return store.New(
		func(v *MonitoringExecution) string { return v.MonitoringScheduleName + "|" + v.ProcessingJobArn },
	)
}

func (b *InMemoryBackend) humanTaskUisStore(r string) *store.Table[HumanTaskUI] {
	if b.humanTaskUis[r] == nil {
		b.humanTaskUis[r] = store.Register(
			b.registry,
			"humanTaskUis:"+r,
			store.New(func(v *HumanTaskUI) string { return v.HumanTaskUIName }),
		)
	}

	return b.humanTaskUis[r]
}

// humanTaskUisStoreRO returns the region-scoped humanTaskUis table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) humanTaskUisStoreRO(r string) *store.Table[HumanTaskUI] {
	if v := b.humanTaskUis[r]; v != nil {
		return v
	}

	return store.New(func(v *HumanTaskUI) string { return v.HumanTaskUIName })
}

func (b *InMemoryBackend) workforcesStore(r string) *store.Table[Workforce] {
	if b.workforces[r] == nil {
		b.workforces[r] = store.Register(
			b.registry,
			"workforces:"+r,
			store.New(func(v *Workforce) string { return v.WorkforceName }),
		)
	}

	return b.workforces[r]
}

// workforcesStoreRO returns the region-scoped workforces table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) workforcesStoreRO(r string) *store.Table[Workforce] {
	if v := b.workforces[r]; v != nil {
		return v
	}

	return store.New(func(v *Workforce) string { return v.WorkforceName })
}

func (b *InMemoryBackend) flowDefinitionsStore(r string) *store.Table[FlowDefinition] {
	if b.flowDefinitions[r] == nil {
		b.flowDefinitions[r] = store.Register(
			b.registry,
			"flowDefinitions:"+r,
			store.New(func(v *FlowDefinition) string { return v.FlowDefinitionName }),
		)
	}

	return b.flowDefinitions[r]
}

// flowDefinitionsStoreRO returns the region-scoped flowDefinitions table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) flowDefinitionsStoreRO(r string) *store.Table[FlowDefinition] {
	if v := b.flowDefinitions[r]; v != nil {
		return v
	}

	return store.New(func(v *FlowDefinition) string { return v.FlowDefinitionName })
}

func (b *InMemoryBackend) appImageConfigsStore(r string) *store.Table[AppImageConfig] {
	if b.appImageConfigs[r] == nil {
		b.appImageConfigs[r] = store.Register(
			b.registry,
			"appImageConfigs:"+r,
			store.New(func(v *AppImageConfig) string { return v.AppImageConfigName }),
		)
	}

	return b.appImageConfigs[r]
}

// appImageConfigsStoreRO returns the region-scoped appImageConfigs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) appImageConfigsStoreRO(r string) *store.Table[AppImageConfig] {
	if v := b.appImageConfigs[r]; v != nil {
		return v
	}

	return store.New(func(v *AppImageConfig) string { return v.AppImageConfigName })
}

func (b *InMemoryBackend) inferenceExperimentsStore(r string) *store.Table[InferenceExperiment] {
	if b.inferenceExperiments[r] == nil {
		b.inferenceExperiments[r] = store.Register(
			b.registry,
			"inferenceExperiments:"+r,
			store.New(func(v *InferenceExperiment) string { return v.Name }),
		)
	}

	return b.inferenceExperiments[r]
}

// inferenceExperimentsStoreRO returns the region-scoped inferenceExperiments table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) inferenceExperimentsStoreRO(r string) *store.Table[InferenceExperiment] {
	if v := b.inferenceExperiments[r]; v != nil {
		return v
	}

	return store.New(func(v *InferenceExperiment) string { return v.Name })
}

func (b *InMemoryBackend) mlflowTrackingServersStore(r string) *store.Table[MlflowTrackingServer] {
	if b.mlflowTrackingServers[r] == nil {
		b.mlflowTrackingServers[r] = store.Register(
			b.registry,
			"mlflowTrackingServers:"+r,
			store.New(func(v *MlflowTrackingServer) string { return v.TrackingServerName }),
		)
	}

	return b.mlflowTrackingServers[r]
}

// mlflowTrackingServersStoreRO returns the region-scoped mlflowTrackingServers table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) mlflowTrackingServersStoreRO(r string) *store.Table[MlflowTrackingServer] {
	if v := b.mlflowTrackingServers[r]; v != nil {
		return v
	}

	return store.New(func(v *MlflowTrackingServer) string { return v.TrackingServerName })
}

func (b *InMemoryBackend) mlflowAppsStore(r string) *store.Table[MlflowApp] {
	if b.mlflowApps[r] == nil {
		b.mlflowApps[r] = store.Register(
			b.registry,
			"mlflowApps:"+r,
			store.New(func(v *MlflowApp) string { return v.Arn }),
		)
	}

	return b.mlflowApps[r]
}

// mlflowAppsStoreRO returns the region-scoped mlflowApps table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) mlflowAppsStoreRO(r string) *store.Table[MlflowApp] {
	if v := b.mlflowApps[r]; v != nil {
		return v
	}

	return store.New(func(v *MlflowApp) string { return v.Arn })
}

func (b *InMemoryBackend) modelCardsStore(r string) *store.Table[ModelCard] {
	if b.modelCards[r] == nil {
		b.modelCards[r] = store.Register(
			b.registry,
			"modelCards:"+r,
			store.New(func(v *ModelCard) string { return v.ModelCardName }),
		)
	}

	return b.modelCards[r]
}

// modelCardsStoreRO returns the region-scoped modelCards table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelCardsStoreRO(r string) *store.Table[ModelCard] {
	if v := b.modelCards[r]; v != nil {
		return v
	}

	return store.New(func(v *ModelCard) string { return v.ModelCardName })
}

func (b *InMemoryBackend) optimizationJobsStore(r string) *store.Table[OptimizationJob] {
	if b.optimizationJobs[r] == nil {
		b.optimizationJobs[r] = store.Register(
			b.registry,
			"optimizationJobs:"+r,
			store.New(func(v *OptimizationJob) string { return v.OptimizationJobName }),
		)
	}

	return b.optimizationJobs[r]
}

// optimizationJobsStoreRO returns the region-scoped optimizationJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) optimizationJobsStoreRO(r string) *store.Table[OptimizationJob] {
	if v := b.optimizationJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *OptimizationJob) string { return v.OptimizationJobName })
}

func (b *InMemoryBackend) studioLifecycleConfigsStore(r string) *store.Table[StudioLifecycleConfig] {
	if b.studioLifecycleConfigs[r] == nil {
		b.studioLifecycleConfigs[r] = store.Register(
			b.registry,
			"studioLifecycleConfigs:"+r,
			store.New(func(v *StudioLifecycleConfig) string { return v.StudioLifecycleConfigName }),
		)
	}

	return b.studioLifecycleConfigs[r]
}

// studioLifecycleConfigsStoreRO returns the region-scoped studioLifecycleConfigs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) studioLifecycleConfigsStoreRO(r string) *store.Table[StudioLifecycleConfig] {
	if v := b.studioLifecycleConfigs[r]; v != nil {
		return v
	}

	return store.New(func(v *StudioLifecycleConfig) string { return v.StudioLifecycleConfigName })
}

func (b *InMemoryBackend) partnerAppsStore(r string) *store.Table[PartnerApp] {
	if b.partnerApps[r] == nil {
		b.partnerApps[r] = store.Register(
			b.registry,
			"partnerApps:"+r,
			store.New(func(v *PartnerApp) string { return v.Arn }),
		)
	}

	return b.partnerApps[r]
}

// partnerAppsStoreRO returns the region-scoped partnerApps table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) partnerAppsStoreRO(r string) *store.Table[PartnerApp] {
	if v := b.partnerApps[r]; v != nil {
		return v
	}

	return store.New(func(v *PartnerApp) string { return v.Arn })
}

func (b *InMemoryBackend) trainingPlansStore(r string) *store.Table[TrainingPlan] {
	if b.trainingPlans[r] == nil {
		b.trainingPlans[r] = store.Register(
			b.registry,
			"trainingPlans:"+r,
			store.New(func(v *TrainingPlan) string { return v.TrainingPlanName }),
		)
	}

	return b.trainingPlans[r]
}

// trainingPlansStoreRO returns the region-scoped trainingPlans table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trainingPlansStoreRO(r string) *store.Table[TrainingPlan] {
	if v := b.trainingPlans[r]; v != nil {
		return v
	}

	return store.New(func(v *TrainingPlan) string { return v.TrainingPlanName })
}

func (b *InMemoryBackend) reservedCapacitiesStore(r string) *store.Table[ReservedCapacity] {
	if b.reservedCapacities[r] == nil {
		b.reservedCapacities[r] = store.Register(
			b.registry,
			"reservedCapacities:"+r,
			store.New(func(v *ReservedCapacity) string { return v.ReservedCapacityArn }),
		)
	}

	return b.reservedCapacities[r]
}

// reservedCapacitiesStoreRO returns the region-scoped reservedCapacities table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) reservedCapacitiesStoreRO(r string) *store.Table[ReservedCapacity] {
	if v := b.reservedCapacities[r]; v != nil {
		return v
	}

	return store.New(func(v *ReservedCapacity) string { return v.ReservedCapacityArn })
}

func (b *InMemoryBackend) trainingPlanExtensionOfferingsStore(r string) *store.Table[pendingTrainingPlanExtension] {
	if b.trainingPlanExtensionOfferings[r] == nil {
		b.trainingPlanExtensionOfferings[r] = store.Register(
			b.registry,
			"trainingPlanExtensionOfferings:"+r,
			store.New(func(v *pendingTrainingPlanExtension) string { return v.ID }),
		)
	}

	return b.trainingPlanExtensionOfferings[r]
}

func (b *InMemoryBackend) modelCardExportJobsStore(r string) *store.Table[ModelCardExportJob] {
	if b.modelCardExportJobs[r] == nil {
		b.modelCardExportJobs[r] = store.Register(
			b.registry,
			"modelCardExportJobs:"+r,
			store.New(func(v *ModelCardExportJob) string { return v.ModelCardExportJobArn }),
		)
	}

	return b.modelCardExportJobs[r]
}

// modelCardExportJobsStoreRO returns the region-scoped modelCardExportJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelCardExportJobsStoreRO(r string) *store.Table[ModelCardExportJob] {
	if v := b.modelCardExportJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *ModelCardExportJob) string { return v.ModelCardExportJobArn })
}

// initTrainingPlanExtMaps (re)initialises the Training Plan / Reserved
// Capacity and ModelCard export job top-level maps. Shared by the
// constructor and Reset to keep both call sites short.
// initTrainingPlanExtMaps initialises the Training Plan / Reserved Capacity /
// ModelCard export job maps, plus the ModelPackageGroup policy / Servicecatalog
// portfolio / Pipeline version state introduced in a later de-stubbing round.
func (b *InMemoryBackend) initTrainingPlanExtMaps() {
	b.reservedCapacities = make(map[string]*store.Table[ReservedCapacity])
	b.trainingPlanExtensionOfferings = make(map[string]*store.Table[pendingTrainingPlanExtension])
	b.modelCardExportJobs = make(map[string]*store.Table[ModelCardExportJob])
	b.pipelineVersions = make(map[string]map[string][]*PipelineVersion)
	b.servicecatalogPortfolioEnabled = make(map[string]bool)
}
func (b *InMemoryBackend) modelARNIndexStore(r string) map[string]string {
	if b.modelARNIndex[r] == nil {
		b.modelARNIndex[r] = make(map[string]string)
	}

	return b.modelARNIndex[r]
}

// modelARNIndexStoreRO returns the region-scoped modelARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelARNIndexStoreRO(r string) map[string]string {
	if v := b.modelARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) endpointConfigARNIndexStore(r string) map[string]string {
	if b.endpointConfigARNIndex[r] == nil {
		b.endpointConfigARNIndex[r] = make(map[string]string)
	}

	return b.endpointConfigARNIndex[r]
}

// endpointConfigARNIndexStoreRO returns the region-scoped endpointConfigARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) endpointConfigARNIndexStoreRO(r string) map[string]string {
	if v := b.endpointConfigARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) endpointARNIndexStore(r string) map[string]string {
	if b.endpointARNIndex[r] == nil {
		b.endpointARNIndex[r] = make(map[string]string)
	}

	return b.endpointARNIndex[r]
}

// endpointARNIndexStoreRO returns the region-scoped endpointARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) endpointARNIndexStoreRO(r string) map[string]string {
	if v := b.endpointARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) trainingJobARNIndexStore(r string) map[string]string {
	if b.trainingJobARNIndex[r] == nil {
		b.trainingJobARNIndex[r] = make(map[string]string)
	}

	return b.trainingJobARNIndex[r]
}

// trainingJobARNIndexStoreRO returns the region-scoped trainingJobARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trainingJobARNIndexStoreRO(r string) map[string]string {
	if v := b.trainingJobARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) notebookARNIndexStore(r string) map[string]string {
	if b.notebookARNIndex[r] == nil {
		b.notebookARNIndex[r] = make(map[string]string)
	}

	return b.notebookARNIndex[r]
}

// notebookARNIndexStoreRO returns the region-scoped notebookARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) notebookARNIndexStoreRO(r string) map[string]string {
	if v := b.notebookARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) hpTuningJobARNIndexStore(r string) map[string]string {
	if b.hpTuningJobARNIndex[r] == nil {
		b.hpTuningJobARNIndex[r] = make(map[string]string)
	}

	return b.hpTuningJobARNIndex[r]
}

// hpTuningJobARNIndexStoreRO returns the region-scoped hpTuningJobARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) hpTuningJobARNIndexStoreRO(r string) map[string]string {
	if v := b.hpTuningJobARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) actionARNIndexStore(r string) map[string]string {
	if b.actionARNIndex[r] == nil {
		b.actionARNIndex[r] = make(map[string]string)
	}

	return b.actionARNIndex[r]
}

// actionARNIndexStoreRO returns the region-scoped actionARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) actionARNIndexStoreRO(r string) map[string]string {
	if v := b.actionARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) algorithmARNIndexStore(r string) map[string]string {
	if b.algorithmARNIndex[r] == nil {
		b.algorithmARNIndex[r] = make(map[string]string)
	}

	return b.algorithmARNIndex[r]
}

// algorithmARNIndexStoreRO returns the region-scoped algorithmARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) algorithmARNIndexStoreRO(r string) map[string]string {
	if v := b.algorithmARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) clusterARNIndexStore(r string) map[string]string {
	if b.clusterARNIndex[r] == nil {
		b.clusterARNIndex[r] = make(map[string]string)
	}

	return b.clusterARNIndex[r]
}

// clusterARNIndexStoreRO returns the region-scoped clusterARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) clusterARNIndexStoreRO(r string) map[string]string {
	if v := b.clusterARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) modelPackageARNIndexStore(r string) map[string]string {
	if b.modelPackageARNIndex[r] == nil {
		b.modelPackageARNIndex[r] = make(map[string]string)
	}

	return b.modelPackageARNIndex[r]
}

// modelPackageARNIndexStoreRO returns the region-scoped modelPackageARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) modelPackageARNIndexStoreRO(r string) map[string]string {
	if v := b.modelPackageARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) processingJobARNIndexStore(r string) map[string]string {
	if b.processingJobARNIndex[r] == nil {
		b.processingJobARNIndex[r] = make(map[string]string)
	}

	return b.processingJobARNIndex[r]
}

// processingJobARNIndexStoreRO returns the region-scoped processingJobARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) processingJobARNIndexStoreRO(r string) map[string]string {
	if v := b.processingJobARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) transformJobARNIndexStore(r string) map[string]string {
	if b.transformJobARNIndex[r] == nil {
		b.transformJobARNIndex[r] = make(map[string]string)
	}

	return b.transformJobARNIndex[r]
}

// transformJobARNIndexStoreRO returns the region-scoped transformJobARNIndex table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) transformJobARNIndexStoreRO(r string) map[string]string {
	if v := b.transformJobARNIndex[r]; v != nil {
		return v
	}

	return make(map[string]string)
}

func (b *InMemoryBackend) domainsStore(r string) *store.Table[Domain] {
	if b.domains[r] == nil {
		b.domains[r] = store.Register(b.registry, "domains:"+r, store.New(func(v *Domain) string { return v.DomainID }))
	}

	return b.domains[r]
}

// domainsStoreRO returns the region-scoped domains table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) domainsStoreRO(r string) *store.Table[Domain] {
	if v := b.domains[r]; v != nil {
		return v
	}

	return store.New(func(v *Domain) string { return v.DomainID })
}

func (b *InMemoryBackend) userProfilesStore(r string) *store.Table[UserProfile] {
	if b.userProfiles[r] == nil {
		b.userProfiles[r] = store.Register(b.registry, "userProfiles:"+r, store.New(func(v *UserProfile) string {
			return userProfileKeyString(userProfileKey{DomainID: v.DomainID, UserProfileName: v.UserProfileName})
		}))
	}

	return b.userProfiles[r]
}

// userProfilesStoreRO returns the region-scoped userProfiles table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) userProfilesStoreRO(r string) *store.Table[UserProfile] {
	if v := b.userProfiles[r]; v != nil {
		return v
	}

	return store.New(func(v *UserProfile) string {
		return userProfileKeyString(userProfileKey{DomainID: v.DomainID, UserProfileName: v.UserProfileName})
	})
}

func (b *InMemoryBackend) appsStore(r string) *store.Table[App] {
	if b.apps[r] == nil {
		b.apps[r] = store.Register(b.registry, "apps:"+r, store.New(func(v *App) string {
			return appKeyString(
				appKey{
					DomainID:        v.DomainID,
					UserProfileName: v.UserProfileName,
					AppType:         v.AppType,
					AppName:         v.AppName,
				},
			)
		}))
	}

	return b.apps[r]
}

// appsStoreRO returns the region-scoped apps table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) appsStoreRO(r string) *store.Table[App] {
	if v := b.apps[r]; v != nil {
		return v
	}

	return store.New(func(v *App) string {
		return appKeyString(
			appKey{
				DomainID:        v.DomainID,
				UserProfileName: v.UserProfileName,
				AppType:         v.AppType,
				AppName:         v.AppName,
			},
		)
	})
}

func (b *InMemoryBackend) featureGroupsStore(r string) *store.Table[FeatureGroup] {
	if b.featureGroups[r] == nil {
		b.featureGroups[r] = store.Register(
			b.registry,
			"featureGroups:"+r,
			store.New(func(v *FeatureGroup) string { return v.FeatureGroupName }),
		)
	}

	return b.featureGroups[r]
}

// featureGroupsStoreRO returns the region-scoped featureGroups table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) featureGroupsStoreRO(r string) *store.Table[FeatureGroup] {
	if v := b.featureGroups[r]; v != nil {
		return v
	}

	return store.New(func(v *FeatureGroup) string { return v.FeatureGroupName })
}

func (b *InMemoryBackend) featureRecordsStore(r string) *store.Table[FeatureRecord] {
	if b.featureRecords[r] == nil {
		b.featureRecords[r] = store.Register(
			b.registry,
			"featureRecords:"+r,
			store.New(func(v *FeatureRecord) string { return v.Key }),
		)
	}

	return b.featureRecords[r]
}

// featureRecordsStoreRO returns the region-scoped featureRecords table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) featureRecordsStoreRO(r string) *store.Table[FeatureRecord] {
	if v := b.featureRecords[r]; v != nil {
		return v
	}

	return store.New(func(v *FeatureRecord) string { return v.Key })
}

func (b *InMemoryBackend) featureMetadataStore(r string) *store.Table[FeatureMetadata] {
	if b.featureMetadata[r] == nil {
		b.featureMetadata[r] = store.Register(
			b.registry,
			"featureMetadata:"+r,
			store.New(func(v *FeatureMetadata) string { return featureMetaKey(v.GroupName, v.FeatureName) }),
		)
	}

	return b.featureMetadata[r]
}

// featureMetadataStoreRO returns the region-scoped featureMetadata table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) featureMetadataStoreRO(r string) *store.Table[FeatureMetadata] {
	if v := b.featureMetadata[r]; v != nil {
		return v
	}

	return store.New(func(v *FeatureMetadata) string { return featureMetaKey(v.GroupName, v.FeatureName) })
}

func (b *InMemoryBackend) pipelinesStore(r string) *store.Table[Pipeline] {
	if b.pipelines[r] == nil {
		b.pipelines[r] = store.Register(
			b.registry,
			"pipelines:"+r,
			store.New(func(v *Pipeline) string { return v.PipelineName }),
		)
	}

	return b.pipelines[r]
}

// pipelinesStoreRO returns the region-scoped pipelines table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) pipelinesStoreRO(r string) *store.Table[Pipeline] {
	if v := b.pipelines[r]; v != nil {
		return v
	}

	return store.New(func(v *Pipeline) string { return v.PipelineName })
}

func (b *InMemoryBackend) pipelineExecutionsStore(r string) *store.Table[PipelineExecution] {
	if b.pipelineExecutions[r] == nil {
		b.pipelineExecutions[r] = store.Register(
			b.registry,
			"pipelineExecutions:"+r,
			store.New(func(v *PipelineExecution) string { return v.PipelineExecutionArn }),
		)
	}

	return b.pipelineExecutions[r]
}

// pipelineExecutionsStoreRO returns the region-scoped pipelineExecutions table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) pipelineExecutionsStoreRO(r string) *store.Table[PipelineExecution] {
	if v := b.pipelineExecutions[r]; v != nil {
		return v
	}

	return store.New(func(v *PipelineExecution) string { return v.PipelineExecutionArn })
}

func (b *InMemoryBackend) pipelineExecStepsStore(r string) *store.Table[PipelineExecutionStep] {
	if b.pipelineExecSteps[r] == nil {
		b.pipelineExecSteps[r] = store.Register(
			b.registry,
			"pipelineExecSteps:"+r,
			store.New(
				func(v *PipelineExecutionStep) string { return pipelineExecutionStepsKey(v.ExecutionArn, v.StepName) },
			),
		)
	}

	return b.pipelineExecSteps[r]
}

// pipelineExecStepsStoreRO returns the region-scoped pipelineExecSteps table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) pipelineExecStepsStoreRO(r string) *store.Table[PipelineExecutionStep] {
	if v := b.pipelineExecSteps[r]; v != nil {
		return v
	}

	return store.New(
		func(v *PipelineExecutionStep) string { return pipelineExecutionStepsKey(v.ExecutionArn, v.StepName) },
	)
}

func (b *InMemoryBackend) experimentsStore(r string) *store.Table[Experiment] {
	if b.experiments[r] == nil {
		b.experiments[r] = store.Register(
			b.registry,
			"experiments:"+r,
			store.New(func(v *Experiment) string { return v.ExperimentName }),
		)
	}

	return b.experiments[r]
}

// experimentsStoreRO returns the region-scoped experiments table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) experimentsStoreRO(r string) *store.Table[Experiment] {
	if v := b.experiments[r]; v != nil {
		return v
	}

	return store.New(func(v *Experiment) string { return v.ExperimentName })
}

func (b *InMemoryBackend) trialsStore(r string) *store.Table[Trial] {
	if b.trials[r] == nil {
		b.trials[r] = store.Register(b.registry, "trials:"+r, store.New(func(v *Trial) string { return v.TrialName }))
	}

	return b.trials[r]
}

// trialsStoreRO returns the region-scoped trials table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trialsStoreRO(r string) *store.Table[Trial] {
	if v := b.trials[r]; v != nil {
		return v
	}

	return store.New(func(v *Trial) string { return v.TrialName })
}

func (b *InMemoryBackend) trialComponentsStore(r string) *store.Table[TrialComponent] {
	if b.trialComponents[r] == nil {
		b.trialComponents[r] = store.Register(
			b.registry,
			"trialComponents:"+r,
			store.New(func(v *TrialComponent) string { return v.TrialComponentName }),
		)
	}

	return b.trialComponents[r]
}

// trialComponentsStoreRO returns the region-scoped trialComponents table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) trialComponentsStoreRO(r string) *store.Table[TrialComponent] {
	if v := b.trialComponents[r]; v != nil {
		return v
	}

	return store.New(func(v *TrialComponent) string { return v.TrialComponentName })
}

func (b *InMemoryBackend) notebookLifecycleConfigsStore(r string) *store.Table[NotebookInstanceLifecycleConfig] {
	if b.notebookLifecycleConfigs[r] == nil {
		b.notebookLifecycleConfigs[r] = store.Register(
			b.registry,
			"notebookLifecycleConfigs:"+r,
			store.New(func(v *NotebookInstanceLifecycleConfig) string { return v.Name }),
		)
	}

	return b.notebookLifecycleConfigs[r]
}

// notebookLifecycleConfigsStoreRO returns the region-scoped notebookLifecycleConfigs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) notebookLifecycleConfigsStoreRO(r string) *store.Table[NotebookInstanceLifecycleConfig] {
	if v := b.notebookLifecycleConfigs[r]; v != nil {
		return v
	}

	return store.New(func(v *NotebookInstanceLifecycleConfig) string { return v.Name })
}

func (b *InMemoryBackend) processingJobsStore(r string) *store.Table[ProcessingJob] {
	if b.processingJobs[r] == nil {
		b.processingJobs[r] = store.Register(
			b.registry,
			"processingJobs:"+r,
			store.New(func(v *ProcessingJob) string { return v.ProcessingJobName }),
		)
	}

	return b.processingJobs[r]
}

// processingJobsStoreRO returns the region-scoped processingJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) processingJobsStoreRO(r string) *store.Table[ProcessingJob] {
	if v := b.processingJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *ProcessingJob) string { return v.ProcessingJobName })
}

func (b *InMemoryBackend) transformJobsStore(r string) *store.Table[TransformJob] {
	if b.transformJobs[r] == nil {
		b.transformJobs[r] = store.Register(
			b.registry,
			"transformJobs:"+r,
			store.New(func(v *TransformJob) string { return v.TransformJobName }),
		)
	}

	return b.transformJobs[r]
}

// transformJobsStoreRO returns the region-scoped transformJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) transformJobsStoreRO(r string) *store.Table[TransformJob] {
	if v := b.transformJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *TransformJob) string { return v.TransformJobName })
}

func (b *InMemoryBackend) edgePackagingJobsStore(r string) *store.Table[EdgePackagingJob] {
	if b.edgePackagingJobs[r] == nil {
		b.edgePackagingJobs[r] = store.Register(
			b.registry,
			"edgePackagingJobs:"+r,
			store.New(func(v *EdgePackagingJob) string { return v.EdgePackagingJobName }),
		)
	}

	return b.edgePackagingJobs[r]
}

// edgePackagingJobsStoreRO returns the region-scoped edgePackagingJobs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) edgePackagingJobsStoreRO(r string) *store.Table[EdgePackagingJob] {
	if v := b.edgePackagingJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *EdgePackagingJob) string { return v.EdgePackagingJobName })
}

func (b *InMemoryBackend) edgeDeploymentPlansStore(r string) *store.Table[EdgeDeploymentPlan] {
	if b.edgeDeploymentPlans[r] == nil {
		b.edgeDeploymentPlans[r] = store.Register(
			b.registry,
			"edgeDeploymentPlans:"+r,
			store.New(func(v *EdgeDeploymentPlan) string { return v.EdgeDeploymentPlanName }),
		)
	}

	return b.edgeDeploymentPlans[r]
}

// edgeDeploymentPlansStoreRO returns the region-scoped edgeDeploymentPlans table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) edgeDeploymentPlansStoreRO(r string) *store.Table[EdgeDeploymentPlan] {
	if v := b.edgeDeploymentPlans[r]; v != nil {
		return v
	}

	return store.New(func(v *EdgeDeploymentPlan) string { return v.EdgeDeploymentPlanName })
}

func (b *InMemoryBackend) inferenceRecommendationsJobsStore(r string) *store.Table[InferenceRecommendationsJob] {
	if b.inferenceRecommendationsJobs[r] == nil {
		b.inferenceRecommendationsJobs[r] = store.Register(
			b.registry,
			"inferenceRecommendationsJobs:"+r,
			store.New(func(v *InferenceRecommendationsJob) string { return v.JobName }),
		)
	}

	return b.inferenceRecommendationsJobs[r]
}

// inferenceRecommendationsJobsStoreRO returns the region-scoped
// inferenceRecommendationsJobs table for r without mutating the outer map.
// Safe to call while holding only b.mu.RLock(): if the region has not been
// observed yet, it returns a fresh, unregistered, empty view instead of
// lazily creating (and persisting) an entry.
func (b *InMemoryBackend) inferenceRecommendationsJobsStoreRO(r string) *store.Table[InferenceRecommendationsJob] {
	if v := b.inferenceRecommendationsJobs[r]; v != nil {
		return v
	}

	return store.New(func(v *InferenceRecommendationsJob) string { return v.JobName })
}

func (b *InMemoryBackend) deviceFleetsStore(r string) *store.Table[DeviceFleet] {
	if b.deviceFleets[r] == nil {
		b.deviceFleets[r] = store.Register(
			b.registry,
			"deviceFleets:"+r,
			store.New(func(v *DeviceFleet) string { return v.DeviceFleetName }),
		)
	}

	return b.deviceFleets[r]
}

// deviceFleetsStoreRO returns the region-scoped deviceFleets table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) deviceFleetsStoreRO(r string) *store.Table[DeviceFleet] {
	if v := b.deviceFleets[r]; v != nil {
		return v
	}

	return store.New(func(v *DeviceFleet) string { return v.DeviceFleetName })
}

func (b *InMemoryBackend) devicesStore(r string) *store.Table[Device] {
	if b.devices[r] == nil {
		b.devices[r] = store.Register(b.registry, "devices:"+r, store.New(func(v *Device) string {
			return deviceKeyString(deviceKey{fleetName: v.DeviceFleetName, deviceName: v.DeviceName})
		}))
	}

	return b.devices[r]
}

// devicesStoreRO returns the region-scoped devices table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) devicesStoreRO(r string) *store.Table[Device] {
	if v := b.devices[r]; v != nil {
		return v
	}

	return store.New(func(v *Device) string {
		return deviceKeyString(deviceKey{fleetName: v.DeviceFleetName, deviceName: v.DeviceName})
	})
}

func (b *InMemoryBackend) inferenceComponentsStore(r string) *store.Table[InferenceComponent] {
	if b.inferenceComponents[r] == nil {
		b.inferenceComponents[r] = store.Register(
			b.registry,
			"inferenceComponents:"+r,
			store.New(func(v *InferenceComponent) string { return v.InferenceComponentName }),
		)
	}

	return b.inferenceComponents[r]
}

// inferenceComponentsStoreRO returns the region-scoped inferenceComponents table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) inferenceComponentsStoreRO(r string) *store.Table[InferenceComponent] {
	if v := b.inferenceComponents[r]; v != nil {
		return v
	}

	return store.New(func(v *InferenceComponent) string { return v.InferenceComponentName })
}

func (b *InMemoryBackend) clusterSchedulerConfigsStore(r string) *store.Table[ClusterSchedulerConfig] {
	if b.clusterSchedulerConfigs[r] == nil {
		b.clusterSchedulerConfigs[r] = store.Register(
			b.registry,
			"clusterSchedulerConfigs:"+r,
			store.New(func(v *ClusterSchedulerConfig) string { return v.ClusterSchedulerConfigName }),
		)
	}

	return b.clusterSchedulerConfigs[r]
}

// clusterSchedulerConfigsStoreRO returns the region-scoped clusterSchedulerConfigs table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) clusterSchedulerConfigsStoreRO(r string) *store.Table[ClusterSchedulerConfig] {
	if v := b.clusterSchedulerConfigs[r]; v != nil {
		return v
	}

	return store.New(func(v *ClusterSchedulerConfig) string { return v.ClusterSchedulerConfigName })
}

func (b *InMemoryBackend) computeQuotasStore(r string) *store.Table[ComputeQuota] {
	if b.computeQuotas[r] == nil {
		b.computeQuotas[r] = store.Register(
			b.registry,
			"computeQuotas:"+r,
			store.New(func(v *ComputeQuota) string { return v.ComputeQuotaName }),
		)
	}

	return b.computeQuotas[r]
}

// computeQuotasStoreRO returns the region-scoped computeQuotas table for r without mutating
// the outer map. Safe to call while holding only b.mu.RLock(): if the region
// has not been observed yet, it returns a fresh, unregistered, empty view
// instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) computeQuotasStoreRO(r string) *store.Table[ComputeQuota] {
	if v := b.computeQuotas[r]; v != nil {
		return v
	}

	return store.New(func(v *ComputeQuota) string { return v.ComputeQuotaName })
}

// Reset reinitialises all maps to empty, clearing all stored resources.
//
//nolint:funlen // Reset must reinitialise all maps; splitting would obscure the invariant
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Fresh registry: every xxxStore(r) helper below lazily re-registers a new
	// per-region store.Table under the same "field:region" name on next use,
	// which would panic against the old registry (duplicate name) since the
	// old one is never otherwise cleared.
	b.registry = store.NewRegistry()

	b.models = make(map[string]*store.Table[Model])
	b.endpointConfigs = make(map[string]*store.Table[EndpointConfig])
	b.endpoints = make(map[string]*store.Table[Endpoint])
	b.trainingJobs = make(map[string]*store.Table[TrainingJob])
	b.notebooks = make(map[string]*store.Table[NotebookInstance])
	b.hpTuningJobs = make(map[string]*store.Table[HyperParameterTuningJob])
	b.associations = make(map[string]*store.Table[Association])
	b.trialComponentAssociations = make(map[string]*store.Table[TrialComponentAssociation])
	b.actions = make(map[string]*store.Table[Action])
	b.artifacts = make(map[string]*store.Table[Artifact])
	b.contexts = make(map[string]*store.Table[Context])
	b.algorithms = make(map[string]*store.Table[Algorithm])
	b.clusters = make(map[string]*store.Table[Cluster])
	b.modelPackages = make(map[string]*store.Table[ModelPackage])
	b.modelPackageGroups = make(map[string]*store.Table[ModelPackageGroup])
	b.autoMLJobs = make(map[string]*store.Table[AutoMLJob])
	b.codeRepositories = make(map[string]*store.Table[CodeRepository])
	b.projects = make(map[string]*store.Table[Project])
	b.spaces = make(map[string]*store.Table[Space])
	b.smImages = make(map[string]*store.Table[SMImage])
	b.imageVersions = make(map[string]map[string]map[int]*ImageVersion)
	b.imageVersionCounts = make(map[string]map[string]int)
	b.compilationJobs = make(map[string]*store.Table[CompilationJob])
	b.monitoringSchedules = make(map[string]*store.Table[MonitoringSchedule])
	b.workteams = make(map[string]*store.Table[Workteam])
	b.labelingJobs = make(map[string]*store.Table[LabelingJob])
	b.dataQualityJobDefs = make(map[string]*store.Table[JobDefinition])
	b.modelBiasJobDefs = make(map[string]*store.Table[JobDefinition])
	b.modelQualityJobDefs = make(map[string]*store.Table[JobDefinition])
	b.modelExplainJobDefs = make(map[string]*store.Table[JobDefinition])
	b.monitoringAlerts = make(map[string]*store.Table[MonitoringAlert])
	b.monitoringAlertHistory = make(map[string][]*MonitoringAlertHistoryEntry)
	b.monitoringExecutions = make(map[string]*store.Table[MonitoringExecution])
	b.humanTaskUis = make(map[string]*store.Table[HumanTaskUI])
	b.workforces = make(map[string]*store.Table[Workforce])
	b.flowDefinitions = make(map[string]*store.Table[FlowDefinition])
	b.appImageConfigs = make(map[string]*store.Table[AppImageConfig])
	b.inferenceExperiments = make(map[string]*store.Table[InferenceExperiment])
	b.mlflowTrackingServers = make(map[string]*store.Table[MlflowTrackingServer])
	b.mlflowApps = make(map[string]*store.Table[MlflowApp])
	b.modelCards = make(map[string]*store.Table[ModelCard])
	b.optimizationJobs = make(map[string]*store.Table[OptimizationJob])
	b.studioLifecycleConfigs = make(map[string]*store.Table[StudioLifecycleConfig])
	b.partnerApps = make(map[string]*store.Table[PartnerApp])
	b.trainingPlans = make(map[string]*store.Table[TrainingPlan])
	b.initTrainingPlanExtMaps()
	b.modelARNIndex = make(map[string]map[string]string)
	b.endpointConfigARNIndex = make(map[string]map[string]string)
	b.endpointARNIndex = make(map[string]map[string]string)
	b.trainingJobARNIndex = make(map[string]map[string]string)
	b.notebookARNIndex = make(map[string]map[string]string)
	b.hpTuningJobARNIndex = make(map[string]map[string]string)
	b.actionARNIndex = make(map[string]map[string]string)
	b.contextARNIndex = make(map[string]map[string]string)
	b.algorithmARNIndex = make(map[string]map[string]string)
	b.clusterARNIndex = make(map[string]map[string]string)
	b.modelPackageARNIndex = make(map[string]map[string]string)
	b.processingJobARNIndex = make(map[string]map[string]string)
	b.transformJobARNIndex = make(map[string]map[string]string)
	b.domains = make(map[string]*store.Table[Domain])
	b.userProfiles = make(map[string]*store.Table[UserProfile])
	b.apps = make(map[string]*store.Table[App])
	b.featureGroups = make(map[string]*store.Table[FeatureGroup])
	b.featureRecords = make(map[string]*store.Table[FeatureRecord])
	b.featureMetadata = make(map[string]*store.Table[FeatureMetadata])
	b.pipelines = make(map[string]*store.Table[Pipeline])
	b.pipelineExecutions = make(map[string]*store.Table[PipelineExecution])
	b.pipelineExecSteps = make(map[string]*store.Table[PipelineExecutionStep])
	b.experiments = make(map[string]*store.Table[Experiment])
	b.trials = make(map[string]*store.Table[Trial])
	b.trialComponents = make(map[string]*store.Table[TrialComponent])
	b.notebookLifecycleConfigs = make(map[string]*store.Table[NotebookInstanceLifecycleConfig])
	b.processingJobs = make(map[string]*store.Table[ProcessingJob])
	b.transformJobs = make(map[string]*store.Table[TransformJob])
	b.edgePackagingJobs = make(map[string]*store.Table[EdgePackagingJob])
	b.edgeDeploymentPlans = make(map[string]*store.Table[EdgeDeploymentPlan])
	b.inferenceRecommendationsJobs = make(map[string]*store.Table[InferenceRecommendationsJob])
	b.deviceFleets = make(map[string]*store.Table[DeviceFleet])
	b.devices = make(map[string]*store.Table[Device])
	b.inferenceComponents = make(map[string]*store.Table[InferenceComponent])
	b.clusterSchedulerConfigs = make(map[string]*store.Table[ClusterSchedulerConfig])
	b.computeQuotas = make(map[string]*store.Table[ComputeQuota])
	b.hubs = make(map[string]*store.Table[Hub])
	b.hubContents = make(map[string]*store.Table[HubContent])
	// Cancel pending goroutines and start fresh lifecycle context.
	b.resetLifecycleContext()
}

// CreateModel creates a new SageMaker model.
func (b *InMemoryBackend) CreateModel(
	ctx context.Context,
	name string,
	executionRoleARN string,
	primaryContainer *ContainerDefinition,
	containers []ContainerDefinition,
	tags map[string]string,
) (*Model, error) {
	b.mu.Lock("CreateModel")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	models := b.modelsStore(region)

	if _, ok := models.Get(name); ok {
		return nil, fmt.Errorf("%w: model %s already exists", ErrModelAlreadyExists, name)
	}

	modelARN := arn.Build("sagemaker", region, b.accountID, "model/"+name)

	var storedPrimaryContainer *ContainerDefinition

	if primaryContainer != nil {
		pc := cloneContainer(*primaryContainer)
		storedPrimaryContainer = &pc
	}

	storedContainers := make([]ContainerDefinition, len(containers))

	for i, c := range containers {
		storedContainers[i] = cloneContainer(c)
	}

	m := &Model{
		ModelName:        name,
		ModelARN:         modelARN,
		ExecutionRoleARN: executionRoleARN,
		PrimaryContainer: storedPrimaryContainer,
		Containers:       storedContainers,
		CreationTime:     time.Now(),
		Tags:             mergeTags(nil, tags),
	}
	models.Put(m)
	b.modelARNIndexStore(region)[modelARN] = name

	return cloneModel(m), nil
}

// DescribeModel returns a model by name.
func (b *InMemoryBackend) DescribeModel(ctx context.Context, name string) (*Model, error) {
	b.mu.RLock("DescribeModel")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	m, ok := b.modelsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: could not find model %q", ErrModelNotFound, name)
	}

	return cloneModel(m), nil
}

// ListModels returns models sorted by name, with optional pagination.
func (b *InMemoryBackend) ListModels(ctx context.Context, nextToken string) ([]*Model, string) {
	b.mu.RLock("ListModels")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.modelsStoreRO(region), nextToken, cloneModel,
		func(a, b *Model) bool { return a.ModelName < b.ModelName })
}

// DeleteModel deletes a model by name.
func (b *InMemoryBackend) DeleteModel(ctx context.Context, name string) error {
	b.mu.Lock("DeleteModel")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	models := b.modelsStore(region)

	m, ok := models.Get(name)
	if !ok {
		return fmt.Errorf("%w: could not find model %q", ErrModelNotFound, name)
	}

	arnIndex := b.modelARNIndexStore(region)
	delete(arnIndex, m.ModelARN)
	models.Delete(name)

	return nil
}

// SetModelExtras sets optional fields on an existing model that were not included
// in the original CreateModel signature (VpcConfig, EnableNetworkIsolation, InferenceExecutionConfig).
func (b *InMemoryBackend) SetModelExtras(
	ctx context.Context,
	name string,
	vpcConfig *VpcConfig,
	enableNetworkIsolation bool,
	inferenceExecConfig *InferenceExecutionConfig,
) error {
	b.mu.Lock("SetModelExtras")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	m, ok := b.modelsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: could not find model %q", ErrModelNotFound, name)
	}

	if vpcConfig != nil {
		vpc := *vpcConfig
		vpc.SecurityGroupIDs = append([]string(nil), vpcConfig.SecurityGroupIDs...)
		vpc.Subnets = append([]string(nil), vpcConfig.Subnets...)
		m.VpcConfig = &vpc
	}

	m.EnableNetworkIsolation = enableNetworkIsolation

	if inferenceExecConfig != nil {
		iec := *inferenceExecConfig
		m.InferenceExecutionConfig = &iec
	}

	return nil
}

// CreateEndpointConfig creates a new SageMaker endpoint configuration.
func (b *InMemoryBackend) CreateEndpointConfig(
	ctx context.Context,
	name string,
	productionVariants []ProductionVariant,
	tags map[string]string,
) (*EndpointConfig, error) {
	b.mu.Lock("CreateEndpointConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ecStore := b.endpointConfigsStore(region)

	if _, ok := ecStore.Get(name); ok {
		return nil, fmt.Errorf(
			"%w: endpoint config %s already exists",
			ErrEndpointConfigAlreadyExists,
			name,
		)
	}

	configARN := arn.Build("sagemaker", region, b.accountID, "endpoint-config/"+name)

	storedVariants := make([]ProductionVariant, len(productionVariants))
	copy(storedVariants, productionVariants)

	ec := &EndpointConfig{
		EndpointConfigName: name,
		EndpointConfigARN:  configARN,
		ProductionVariants: storedVariants,
		CreationTime:       time.Now(),
		Tags:               mergeTags(nil, tags),
	}
	ecStore.Put(ec)
	b.endpointConfigARNIndexStore(region)[configARN] = name

	return cloneEndpointConfig(ec), nil
}

// DescribeEndpointConfig returns an endpoint config by name.
func (b *InMemoryBackend) DescribeEndpointConfig(ctx context.Context, name string) (*EndpointConfig, error) {
	b.mu.RLock("DescribeEndpointConfig")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	ec, ok := b.endpointConfigsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf(
			"%w: could not find endpoint configuration %q",
			ErrEndpointConfigNotFound,
			name,
		)
	}

	return cloneEndpointConfig(ec), nil
}

// ListEndpointConfigs returns endpoint configurations sorted by name, with optional pagination.
func (b *InMemoryBackend) ListEndpointConfigs(ctx context.Context, nextToken string) ([]*EndpointConfig, string) {
	b.mu.RLock("ListEndpointConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.endpointConfigsStoreRO(region), nextToken, cloneEndpointConfig,
		func(a, b *EndpointConfig) bool { return a.EndpointConfigName < b.EndpointConfigName })
}

// DeleteEndpointConfig deletes an endpoint configuration by name.
func (b *InMemoryBackend) DeleteEndpointConfig(ctx context.Context, name string) error {
	b.mu.Lock("DeleteEndpointConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ecStore := b.endpointConfigsStore(region)

	ec, ok := ecStore.Get(name)
	if !ok {
		return fmt.Errorf(
			"%w: could not find endpoint configuration %q",
			ErrEndpointConfigNotFound,
			name,
		)
	}

	arnIndex := b.endpointConfigARNIndexStore(region)
	delete(arnIndex, ec.EndpointConfigARN)
	ecStore.Delete(name)

	return nil
}

// SetEndpointConfigExtras sets optional fields on an existing endpoint config that were not
// included in the original CreateEndpointConfig signature.
func (b *InMemoryBackend) SetEndpointConfigExtras(
	ctx context.Context,
	name string,
	dataCaptureConfig *DataCaptureConfig,
	asyncInferenceConfig *AsyncInferenceConfig,
	vpcConfig *VpcConfig,
	executionRoleArn string,
	kmsKeyID string,
	shadowProductionVariants []ProductionVariant,
	enableNetworkIsolation bool,
) error {
	b.mu.Lock("SetEndpointConfigExtras")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ec, ok := b.endpointConfigsStore(region).Get(name)
	if !ok {
		return fmt.Errorf(
			"%w: could not find endpoint configuration %q",
			ErrEndpointConfigNotFound,
			name,
		)
	}

	if dataCaptureConfig != nil {
		dcc := *dataCaptureConfig
		ec.DataCaptureConfig = &dcc
	}

	if asyncInferenceConfig != nil {
		aic := *asyncInferenceConfig
		ec.AsyncInferenceConfig = &aic
	}

	if vpcConfig != nil {
		vpc := *vpcConfig
		vpc.SecurityGroupIDs = append([]string(nil), vpcConfig.SecurityGroupIDs...)
		vpc.Subnets = append([]string(nil), vpcConfig.Subnets...)
		ec.VpcConfig = &vpc
	}

	ec.ExecutionRoleArn = executionRoleArn
	ec.KmsKeyID = kmsKeyID
	ec.EnableNetworkIsolation = enableNetworkIsolation

	if len(shadowProductionVariants) > 0 {
		stored := make([]ProductionVariant, len(shadowProductionVariants))
		for i, pv := range shadowProductionVariants {
			stored[i] = cloneProductionVariant(pv)
		}
		ec.ShadowProductionVariants = stored
	}

	return nil
}

// AddTags adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) AddTags(ctx context.Context, resourceARN string, tags map[string]string) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name, ok := b.modelARNIndexStore(region)[resourceARN]; ok {
		m := tableGet(b.modelsStore(region), name)
		m.Tags = mergeTags(m.Tags, tags)

		return nil
	}

	if name, ok := b.endpointConfigARNIndexStore(region)[resourceARN]; ok {
		ec := tableGet(b.endpointConfigsStore(region), name)
		ec.Tags = mergeTags(ec.Tags, tags)

		return nil
	}

	if name, ok := b.actionARNIndexStore(region)[resourceARN]; ok {
		a := tableGet(b.actionsStore(region), name)
		a.Tags = mergeTags(a.Tags, tags)

		return nil
	}

	if name, ok := b.algorithmARNIndexStore(region)[resourceARN]; ok {
		al := tableGet(b.algorithmsStore(region), name)
		al.Tags = mergeTags(al.Tags, tags)

		return nil
	}

	if _, ok := b.modelPackageARNIndexStore(region)[resourceARN]; ok {
		mp := tableGet(b.modelPackagesStore(region), resourceARN)
		mp.Tags = mergeTags(mp.Tags, tags)

		return nil
	}

	if name, ok := b.endpointARNIndexStore(region)[resourceARN]; ok {
		ep := tableGet(b.endpointsStore(region), name)
		ep.Tags = mergeTags(ep.Tags, tags)

		return nil
	}

	if name, ok := b.trainingJobARNIndexStore(region)[resourceARN]; ok {
		tj := tableGet(b.trainingJobsStore(region), name)
		tj.Tags = mergeTags(tj.Tags, tags)

		return nil
	}

	if name, ok := b.notebookARNIndexStore(region)[resourceARN]; ok {
		nb := tableGet(b.notebooksStore(region), name)
		nb.Tags = mergeTags(nb.Tags, tags)

		return nil
	}

	if name, ok := b.hpTuningJobARNIndexStore(region)[resourceARN]; ok {
		j := tableGet(b.hpTuningJobsStore(region), name)
		j.Tags = mergeTags(j.Tags, tags)

		return nil
	}

	if name, ok := b.processingJobARNIndexStore(region)[resourceARN]; ok {
		if pj, found := b.processingJobsStore(region).Get(name); found {
			pj.Tags = mergeTags(pj.Tags, tags)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrValidation, resourceARN)
}

// ListTags returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTags(ctx context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	tagMap := b.findTagMapLocked(resourceARN, region)
	if tagMap == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrValidation, resourceARN)
	}

	result := make(map[string]string, len(*tagMap))
	maps.Copy(result, *tagMap)

	return result, nil
}

// findTagMapLocked returns a pointer to the tags map for a resource identified by ARN.
// Must be called with b.mu held. Returns nil if the resource is not found.
func (b *InMemoryBackend) findTagMapLocked(resourceARN string, region string) *map[string]string {
	if name, ok := b.modelARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.modelsStoreRO(region), name).Tags
	}

	if name, ok := b.endpointConfigARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.endpointConfigsStoreRO(region), name).Tags
	}

	if name, ok := b.actionARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.actionsStoreRO(region), name).Tags
	}

	if name, ok := b.algorithmARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.algorithmsStoreRO(region), name).Tags
	}

	if _, ok := b.modelPackageARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.modelPackagesStoreRO(region), resourceARN).Tags
	}

	if name, ok := b.endpointARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.endpointsStoreRO(region), name).Tags
	}

	if name, ok := b.trainingJobARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.trainingJobsStoreRO(region), name).Tags
	}

	if name, ok := b.notebookARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.notebooksStoreRO(region), name).Tags
	}

	if name, ok := b.hpTuningJobARNIndexStoreRO(region)[resourceARN]; ok {
		return &tableGet(b.hpTuningJobsStoreRO(region), name).Tags
	}

	if tags := b.findTagMapIndexedExtraLocked(resourceARN, region); tags != nil {
		return tags
	}

	return b.findTagMapStatefulLocked(resourceARN, region)
}

// findTagMapIndexedExtraLocked handles the remaining ARN-indexed resource
// kinds (processing jobs, transform jobs, clusters). Separated from
// findTagMapLocked to keep it within cyclomatic-complexity limits.
func (b *InMemoryBackend) findTagMapIndexedExtraLocked(resourceARN string, region string) *map[string]string {
	if name, ok := b.processingJobARNIndexStoreRO(region)[resourceARN]; ok {
		if pj, found := b.processingJobsStoreRO(region).Get(name); found {
			return &pj.Tags
		}
	}

	if name, ok := b.transformJobARNIndexStoreRO(region)[resourceARN]; ok {
		if tj, found := b.transformJobsStoreRO(region).Get(name); found {
			return &tj.Tags
		}
	}

	if name, ok := b.clusterARNIndexStoreRO(region)[resourceARN]; ok {
		if c, found := b.clustersStoreRO(region).Get(name); found {
			return &c.Tags
		}
	}

	return nil
}

// findTagMapStatefulLocked handles tag lookups for stateful resources (domains,
// featureGroups, pipelines, experiments, trials, trialComponents). Separated
// to keep findTagMapLocked within cognitive-complexity limits.
func (b *InMemoryBackend) findTagMapStatefulLocked(resourceARN string, region string) *map[string]string {
	for _, d := range b.domainsStoreRO(region).All() {
		if d.DomainArn == resourceARN {
			return &d.Tags
		}
	}
	for _, fg := range b.featureGroupsStoreRO(region).All() {
		if fg.FeatureGroupArn == resourceARN {
			return &fg.Tags
		}
	}
	for _, p := range b.pipelinesStoreRO(region).All() {
		if p.PipelineArn == resourceARN {
			return &p.Tags
		}
	}
	for _, e := range b.experimentsStoreRO(region).All() {
		if e.ExperimentArn == resourceARN {
			return &e.Tags
		}
	}
	for _, t := range b.trialsStoreRO(region).All() {
		if t.TrialArn == resourceARN {
			return &t.Tags
		}
	}
	for _, tc := range b.trialComponentsStoreRO(region).All() {
		if tc.TrialComponentArn == resourceARN {
			return &tc.Tags
		}
	}

	return nil
}

// DeleteTags removes tag keys from a resource identified by ARN.
func (b *InMemoryBackend) DeleteTags(ctx context.Context, resourceARN string, tagKeys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	tags := b.findTagMapLocked(resourceARN, region)
	if tags == nil {
		return fmt.Errorf("%w: resource %s not found", ErrValidation, resourceARN)
	}

	for _, k := range tagKeys {
		delete(*tags, k)
	}

	return nil
}

// mergeTags merges new tags into existing ones, returning a new map.
func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}

// parseNextToken parses a pagination token (integer offset) into a slice index.
func parseNextToken(token string) int {
	if token == "" {
		return 0
	}

	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// associationKey returns the map key for an association.
func associationKey(sourceArn, destinationArn string) string {
	return sourceArn + "|" + destinationArn
}

// trialComponentKey returns the map key for a trial-component association.
func trialComponentKey(trialName, componentName string) string {
	return trialName + "|" + componentName
}

// AddAssociation creates an association between a source and destination entity in the ML lineage graph.
func (b *InMemoryBackend) AddAssociation(
	ctx context.Context,
	sourceArn, destinationArn, associationType string,
	tags map[string]string,
) (*Association, error) {
	b.mu.Lock("AddAssociation")
	defer b.mu.Unlock()

	if sourceArn == "" {
		return nil, fmt.Errorf("%w: SourceArn is required", ErrValidation)
	}

	if destinationArn == "" {
		return nil, fmt.Errorf("%w: DestinationArn is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	assocStore := b.associationsStore(region)

	key := associationKey(sourceArn, destinationArn)
	if _, ok := assocStore.Get(key); ok {
		return nil, fmt.Errorf(
			"%w: association between %s and %s already exists",
			ErrAssociationAlreadyExists,
			sourceArn,
			destinationArn,
		)
	}

	assocARN := arn.Build(
		"sagemaker",
		region,
		b.accountID,
		fmt.Sprintf("association/%s/%s", sourceArn, destinationArn),
	)

	a := &Association{
		SourceArn:       sourceArn,
		DestinationArn:  destinationArn,
		AssociationType: associationType,
		AssociationArn:  assocARN,
		CreationTime:    time.Now(),
		Tags:            mergeTags(nil, tags),
	}
	assocStore.Put(a)

	return cloneAssociation(a), nil
}

// AssociateTrialComponent associates a trial component with a trial.
func (b *InMemoryBackend) AssociateTrialComponent(
	ctx context.Context,
	trialName, trialComponentName string,
) (*TrialComponentAssociation, error) {
	b.mu.Lock("AssociateTrialComponent")
	defer b.mu.Unlock()

	if trialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", ErrValidation)
	}

	if trialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	tcaStore := b.trialComponentAssociationsStore(region)

	key := trialComponentKey(trialName, trialComponentName)
	if _, ok := tcaStore.Get(key); ok {
		return nil, fmt.Errorf("%w: trial component %s is already associated with trial %s",
			ErrAssociationAlreadyExists, trialComponentName, trialName)
	}

	trialArn := arn.Build("sagemaker", region, b.accountID, "experiment-trial/"+trialName)
	componentArn := arn.Build(
		"sagemaker",
		region,
		b.accountID,
		"experiment-trial-component/"+trialComponentName,
	)

	assoc := &TrialComponentAssociation{
		TrialName:          trialName,
		TrialComponentName: trialComponentName,
		TrialArn:           trialArn,
		TrialComponentArn:  componentArn,
		CreationTime:       time.Now(),
	}
	tcaStore.Put(assoc)

	return cloneTrialComponentAssociation(assoc), nil
}

// ensureClusterLocked looks up a cluster by name (must be called with lock held).
func (b *InMemoryBackend) ensureClusterLocked(ctx context.Context, clusterName string) (*Cluster, error) {
	region := getRegion(ctx, b.region)

	c, ok := b.clustersStore(region).Get(clusterName)
	if !ok {
		return nil, fmt.Errorf("%w: cluster %q not found", ErrClusterNotFound, clusterName)
	}

	return c, nil
}

// AddClusterInternal adds a cluster directly for seeding tests.
func (b *InMemoryBackend) AddClusterInternal(ctx context.Context, clusterName string) *Cluster {
	b.mu.Lock("AddClusterInternal")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	clusterARN := arn.Build("sagemaker", region, b.accountID, "cluster/"+clusterName)
	c := &Cluster{
		ClusterName:   clusterName,
		ClusterArn:    clusterARN,
		ClusterStatus: clusterStatusInService,
		Nodes:         make(map[string]*ClusterNode),
		CreationTime:  time.Now(),
	}
	b.clustersStore(region).Put(c)
	b.clusterARNIndexStore(region)[clusterARN] = clusterName

	return cloneCluster(c)
}

// AddActionInternal adds an action directly for seeding tests.
func (b *InMemoryBackend) AddActionInternal(ctx context.Context, name, actionType string) *Action {
	b.mu.Lock("AddActionInternal")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	actionARN := arn.Build("sagemaker", region, b.accountID, "action/"+name)
	now := time.Now()
	a := &Action{
		ActionName:       name,
		ActionArn:        actionARN,
		ActionType:       actionType,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             make(map[string]string),
	}
	b.actionsStore(region).Put(a)
	b.actionARNIndexStore(region)[actionARN] = name

	return cloneAction(a)
}

// AddAlgorithmInternal adds an algorithm directly for seeding tests.
func (b *InMemoryBackend) AddAlgorithmInternal(ctx context.Context, name string) *Algorithm {
	b.mu.Lock("AddAlgorithmInternal")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	algorithmARN := arn.Build("sagemaker", region, b.accountID, "algorithm/"+name)
	al := &Algorithm{
		AlgorithmName:   name,
		AlgorithmArn:    algorithmARN,
		AlgorithmStatus: algorithmStatusCompleted,
		CreationTime:    time.Now(),
		Tags:            make(map[string]string),
	}
	b.algorithmsStore(region).Put(al)
	b.algorithmARNIndexStore(region)[algorithmARN] = al.AlgorithmName

	return cloneAlgorithm(al)
}

// AttachClusterNodeVolume attaches a volume to a cluster node.
func (b *InMemoryBackend) AttachClusterNodeVolume(
	ctx context.Context,
	clusterName, nodeID string,
	volume ClusterNodeVolume,
) (string, string, error) {
	b.mu.Lock("AttachClusterNodeVolume")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(ctx, clusterName)
	if err != nil {
		return "", "", err
	}

	if nodeID == "" {
		return "", "", fmt.Errorf("%w: NodeId is required", ErrValidation)
	}

	node, ok := c.Nodes[nodeID]
	if !ok {
		node = &ClusterNode{
			NodeID:     nodeID,
			NodeStatus: statusRunning,
		}
		c.Nodes[nodeID] = node
	}

	node.Volumes = append(node.Volumes, volume)

	return c.ClusterArn, nodeID, nil
}

// BatchAddClusterNodes adds multiple nodes to a cluster.
// Returns clusterArn and a slice of nodeIDs that failed to add.
func (b *InMemoryBackend) BatchAddClusterNodes(
	ctx context.Context,
	clusterName string,
	nodeConfigs []ClusterNode,
) (string, []string, error) {
	b.mu.Lock("BatchAddClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(ctx, clusterName)
	if err != nil {
		return "", nil, err
	}

	var failures []string

	for i := range nodeConfigs {
		node := &nodeConfigs[i]
		if node.NodeID == "" {
			node.NodeID = fmt.Sprintf("node-%d", len(c.Nodes)+1)
		}

		if node.NodeStatus == "" {
			node.NodeStatus = statusRunning
		}

		if _, exists := c.Nodes[node.NodeID]; exists {
			failures = append(failures, node.NodeID)

			continue
		}

		nodeCopy := *node
		c.Nodes[node.NodeID] = &nodeCopy
	}

	return c.ClusterArn, failures, nil
}

// BatchDeleteClusterNodes removes multiple nodes from a cluster.
// Returns clusterArn, a slice of nodeIDs with errors, and a slice of successfully deleted nodeIDs.
func (b *InMemoryBackend) BatchDeleteClusterNodes(
	ctx context.Context,
	clusterName string,
	nodeIDs []string,
) (string, []string, []string, error) {
	b.mu.Lock("BatchDeleteClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(ctx, clusterName)
	if err != nil {
		return "", nil, nil, err
	}

	var errored, successful []string

	for _, nodeID := range nodeIDs {
		if _, ok := c.Nodes[nodeID]; !ok {
			errored = append(errored, nodeID)

			continue
		}

		delete(c.Nodes, nodeID)
		successful = append(successful, nodeID)
	}

	return c.ClusterArn, errored, successful, nil
}

// ModelPackageBatchResult holds the result of describing a single model package in a batch.
type ModelPackageBatchResult struct {
	ModelPackage *ModelPackage
	ErrorCode    string
	ErrorMessage string
}

// BatchDescribeModelPackage returns descriptions of multiple model packages by ARN.
func (b *InMemoryBackend) BatchDescribeModelPackage(
	ctx context.Context,
	modelPackageArns []string,
) map[string]ModelPackageBatchResult {
	b.mu.RLock("BatchDescribeModelPackage")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	mpStore := b.modelPackagesStoreRO(region)

	results := make(map[string]ModelPackageBatchResult, len(modelPackageArns))

	for _, arnStr := range modelPackageArns {
		mp, ok := mpStore.Get(arnStr)
		if !ok {
			results[arnStr] = ModelPackageBatchResult{
				ErrorCode:    "ValidationException",
				ErrorMessage: fmt.Sprintf("model package %q not found", arnStr),
			}

			continue
		}

		results[arnStr] = ModelPackageBatchResult{
			ModelPackage: cloneModelPackage(mp),
		}
	}

	return results
}

// BatchRebootClusterNodes reboots multiple nodes in a cluster.
// Returns clusterArn, a slice of failed nodeIDs, and successful nodeIDs.
func (b *InMemoryBackend) BatchRebootClusterNodes(
	ctx context.Context,
	clusterName string,
	nodeIDs []string,
) (string, []string, []string, error) {
	b.mu.Lock("BatchRebootClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(ctx, clusterName)
	if err != nil {
		return "", nil, nil, err
	}

	var failures, successful []string

	for _, nodeID := range nodeIDs {
		if _, ok := c.Nodes[nodeID]; !ok {
			failures = append(failures, nodeID)

			continue
		}

		successful = append(successful, nodeID)
	}

	return c.ClusterArn, failures, successful, nil
}

// BatchReplaceClusterNodes replaces multiple nodes in a cluster.
// Returns clusterArn and a slice of nodeIDs that failed to replace.
func (b *InMemoryBackend) BatchReplaceClusterNodes(
	ctx context.Context,
	clusterName string,
	nodes []ClusterNode,
) (string, []string, error) {
	b.mu.Lock("BatchReplaceClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(ctx, clusterName)
	if err != nil {
		return "", nil, err
	}

	var failures []string

	for i := range nodes {
		node := &nodes[i]
		if node.NodeID == "" {
			failures = append(failures, "")

			continue
		}

		if _, ok := c.Nodes[node.NodeID]; !ok {
			failures = append(failures, node.NodeID)

			continue
		}

		nodeCopy := *node
		nodeCopy.NodeStatus = statusRunning
		c.Nodes[node.NodeID] = &nodeCopy
	}

	return c.ClusterArn, failures, nil
}

// CreateAction creates a SageMaker ML lineage action.
func (b *InMemoryBackend) CreateAction(
	ctx context.Context,
	name, actionType, description, status string,
	source ActionSource,
	properties map[string]string,
	tags map[string]string,
) (*Action, error) {
	b.mu.Lock("CreateAction")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ActionName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	actionsStore := b.actionsStore(region)

	if _, ok := actionsStore.Get(name); ok {
		return nil, fmt.Errorf("%w: action %q already exists", ErrActionAlreadyExists, name)
	}

	actionARN := arn.Build("sagemaker", region, b.accountID, "action/"+name)
	now := time.Now()

	a := &Action{
		ActionName:       name,
		ActionArn:        actionARN,
		ActionType:       actionType,
		Description:      description,
		Status:           status,
		Source:           source,
		Properties:       maps.Clone(properties),
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	actionsStore.Put(a)
	b.actionARNIndexStore(region)[actionARN] = name

	return cloneAction(a), nil
}

// CreateAlgorithmOptions holds the optional fields accepted by CreateAlgorithm.
type CreateAlgorithmOptions struct {
	Tags                    map[string]string
	AlgorithmName           string
	AlgorithmDescription    string
	TrainingSpecification   json.RawMessage
	InferenceSpecification  json.RawMessage
	ValidationSpecification json.RawMessage
	CertifyForMarketplace   bool
}

// CreateAlgorithm creates a SageMaker algorithm specification.
func (b *InMemoryBackend) CreateAlgorithm(ctx context.Context, opts CreateAlgorithmOptions) (*Algorithm, error) {
	b.mu.Lock("CreateAlgorithm")
	defer b.mu.Unlock()

	if opts.AlgorithmName == "" {
		return nil, fmt.Errorf("%w: AlgorithmName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	algoStore := b.algorithmsStore(region)

	if _, ok := algoStore.Get(opts.AlgorithmName); ok {
		return nil, fmt.Errorf("%w: algorithm %q already exists", ErrAlgorithmAlreadyExists, opts.AlgorithmName)
	}

	algorithmARN := arn.Build("sagemaker", region, b.accountID, "algorithm/"+opts.AlgorithmName)

	statusDetails := AlgorithmStatusDetails{
		ImageScanStatuses:  []AlgorithmStatusItem{},
		ValidationStatuses: []AlgorithmStatusItem{},
	}

	al := &Algorithm{
		AlgorithmName:           opts.AlgorithmName,
		AlgorithmArn:            algorithmARN,
		AlgorithmDescription:    opts.AlgorithmDescription,
		AlgorithmStatus:         algorithmStatusCompleted,
		AlgorithmStatusDetails:  statusDetails,
		TrainingSpecification:   opts.TrainingSpecification,
		InferenceSpecification:  opts.InferenceSpecification,
		ValidationSpecification: opts.ValidationSpecification,
		CertifyForMarketplace:   opts.CertifyForMarketplace,
		Tags:                    mergeTags(nil, opts.Tags),
		CreationTime:            time.Now(),
	}
	algoStore.Put(al)
	b.algorithmARNIndexStore(region)[algorithmARN] = opts.AlgorithmName

	return cloneAlgorithm(al), nil
}

// DescribeAlgorithm returns an algorithm by name.
func (b *InMemoryBackend) DescribeAlgorithm(ctx context.Context, name string) (*Algorithm, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeAlgorithm")
	defer b.mu.RUnlock()

	al, ok := b.algorithmsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: algorithm %q not found", ErrAlgorithmNotFound, name)
	}

	return cloneAlgorithm(al), nil
}

// DeleteAlgorithm removes an algorithm by name.
func (b *InMemoryBackend) DeleteAlgorithm(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteAlgorithm")
	defer b.mu.Unlock()

	store := b.algorithmsStore(region)

	al, ok := store.Get(name)
	if !ok {
		return fmt.Errorf("%w: algorithm %q not found", ErrAlgorithmNotFound, name)
	}

	store.Delete(name)
	delete(b.algorithmARNIndexStore(region), al.AlgorithmArn)

	return nil
}

// ListAlgorithms returns a page of algorithms, ordered by name.
func (b *InMemoryBackend) ListAlgorithms(ctx context.Context, nextToken string) ([]*Algorithm, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAlgorithms")
	defer b.mu.RUnlock()

	return sagemakerListKeyPaged(
		b.algorithmsStoreRO(region),
		nextToken,
		cloneAlgorithm,
		func(v *Algorithm) string { return v.AlgorithmName },
	)
}

// AddModelPackageInternal adds a model package directly for testing.
func (b *InMemoryBackend) AddModelPackageInternal(ctx context.Context, mp *ModelPackage) {
	b.mu.Lock("AddModelPackageInternal")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.modelPackagesStore(region).Put(mp)
	b.modelPackageARNIndexStore(region)[mp.ModelPackageArn] = mp.ModelPackageArn
}

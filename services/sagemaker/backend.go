package sagemaker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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

// Algorithm represents a SageMaker algorithm specification.
type Algorithm struct {
	CreationTime         time.Time         `json:"CreationTime"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	AlgorithmName        string            `json:"AlgorithmName"`
	AlgorithmArn         string            `json:"AlgorithmArn"`
	AlgorithmDescription string            `json:"AlgorithmDescription,omitempty"`
	AlgorithmStatus      string            `json:"AlgorithmStatus"`
}

// cloneAlgorithm returns a deep copy of al.
func cloneAlgorithm(al *Algorithm) *Algorithm {
	cp := *al
	cp.Tags = maps.Clone(al.Tags)

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
	models                       map[string]map[string]*Model
	endpointConfigs              map[string]map[string]*EndpointConfig
	endpoints                    map[string]map[string]*Endpoint
	trainingJobs                 map[string]map[string]*TrainingJob
	notebooks                    map[string]map[string]*NotebookInstance
	hpTuningJobs                 map[string]map[string]*HyperParameterTuningJob
	associations                 map[string]map[string]*Association
	trialComponentAssociations   map[string]map[string]*TrialComponentAssociation
	actions                      map[string]map[string]*Action
	artifacts                    map[string]map[string]*Artifact // region -> ArtifactArn -> Artifact
	contexts                     map[string]map[string]*Context  // region -> ContextName -> Context
	algorithms                   map[string]map[string]*Algorithm
	clusters                     map[string]map[string]*Cluster
	modelPackages                map[string]map[string]*ModelPackage
	modelPackageGroups           map[string]map[string]*ModelPackageGroup
	autoMLJobs                   map[string]map[string]*AutoMLJob
	codeRepositories             map[string]map[string]*CodeRepository
	projects                     map[string]map[string]*Project
	spaces                       map[string]map[string]*Space
	smImages                     map[string]map[string]*SMImage
	imageVersions                map[string]map[string]map[int]*ImageVersion // region → imageName → version → ImageVersion
	imageVersionCounts           map[string]map[string]int                   // region → imageName → latest version number
	compilationJobs              map[string]map[string]*CompilationJob
	monitoringSchedules          map[string]map[string]*MonitoringSchedule
	workteams                    map[string]map[string]*Workteam
	dataQualityJobDefs           map[string]map[string]*JobDefinition
	modelBiasJobDefs             map[string]map[string]*JobDefinition
	modelQualityJobDefs          map[string]map[string]*JobDefinition
	modelExplainJobDefs          map[string]map[string]*JobDefinition
	humanTaskUis                 map[string]map[string]*HumanTaskUI
	workforces                   map[string]map[string]*Workforce
	flowDefinitions              map[string]map[string]*FlowDefinition
	appImageConfigs              map[string]map[string]*AppImageConfig
	inferenceExperiments         map[string]map[string]*InferenceExperiment
	mlflowTrackingServers        map[string]map[string]*MlflowTrackingServer
	modelCards                   map[string]map[string]*ModelCard
	optimizationJobs             map[string]map[string]*OptimizationJob
	studioLifecycleConfigs       map[string]map[string]*StudioLifecycleConfig
	partnerApps                  map[string]map[string]*PartnerApp
	trainingPlans                map[string]map[string]*TrainingPlan
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
	domains                      map[string]map[string]*Domain
	userProfiles                 map[string]map[userProfileKey]*UserProfile
	apps                         map[string]map[appKey]*App
	featureGroups                map[string]map[string]*FeatureGroup
	featureRecords               map[string]map[string]*FeatureRecord
	featureMetadata              map[string]map[string]*FeatureMetadata
	pipelines                    map[string]map[string]*Pipeline
	pipelineExecutions           map[string]map[string]*PipelineExecution
	pipelineExecSteps            map[string]map[string]*PipelineExecutionStep
	experiments                  map[string]map[string]*Experiment
	trials                       map[string]map[string]*Trial
	trialComponents              map[string]map[string]*TrialComponent
	notebookLifecycleConfigs     map[string]map[string]*NotebookInstanceLifecycleConfig
	processingJobs               map[string]map[string]*ProcessingJob
	transformJobs                map[string]map[string]*TransformJob
	edgePackagingJobs            map[string]map[string]*EdgePackagingJob
	inferenceRecommendationsJobs map[string]map[string]*InferenceRecommendationsJob
	deviceFleets                 map[string]map[string]*DeviceFleet
	devices                      map[string]map[deviceKey]*Device
	inferenceComponents          map[string]map[string]*InferenceComponent
	clusterSchedulerConfigs      map[string]map[string]*ClusterSchedulerConfig
	computeQuotas                map[string]map[string]*ComputeQuota
	lifecycleParent              context.Context
	lifecycleCtx                 context.Context
	lifecycleCancel              context.CancelFunc
	mu                           *lockmetrics.RWMutex
	accountID                    string
	region                       string
	wg                           sync.WaitGroup
}

// NewInMemoryBackend creates a new in-memory SageMaker backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new in-memory SageMaker backend whose
// lifecycle goroutines (status-transition simulators) are children of svcCtx, so
// they are cancelled when the service shuts down rather than leaking. If svcCtx is
// nil, [context.Background] is used.
func NewInMemoryBackendWithContext(
	svcCtx context.Context,
	accountID, region string,
) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	b := &InMemoryBackend{
		lifecycleParent:              svcCtx,
		models:                       make(map[string]map[string]*Model),
		endpointConfigs:              make(map[string]map[string]*EndpointConfig),
		endpoints:                    make(map[string]map[string]*Endpoint),
		trainingJobs:                 make(map[string]map[string]*TrainingJob),
		notebooks:                    make(map[string]map[string]*NotebookInstance),
		hpTuningJobs:                 make(map[string]map[string]*HyperParameterTuningJob),
		associations:                 make(map[string]map[string]*Association),
		trialComponentAssociations:   make(map[string]map[string]*TrialComponentAssociation),
		actions:                      make(map[string]map[string]*Action),
		artifacts:                    make(map[string]map[string]*Artifact),
		contexts:                     make(map[string]map[string]*Context),
		algorithms:                   make(map[string]map[string]*Algorithm),
		clusters:                     make(map[string]map[string]*Cluster),
		modelPackages:                make(map[string]map[string]*ModelPackage),
		modelPackageGroups:           make(map[string]map[string]*ModelPackageGroup),
		autoMLJobs:                   make(map[string]map[string]*AutoMLJob),
		codeRepositories:             make(map[string]map[string]*CodeRepository),
		projects:                     make(map[string]map[string]*Project),
		spaces:                       make(map[string]map[string]*Space),
		smImages:                     make(map[string]map[string]*SMImage),
		imageVersions:                make(map[string]map[string]map[int]*ImageVersion),
		imageVersionCounts:           make(map[string]map[string]int),
		compilationJobs:              make(map[string]map[string]*CompilationJob),
		monitoringSchedules:          make(map[string]map[string]*MonitoringSchedule),
		workteams:                    make(map[string]map[string]*Workteam),
		dataQualityJobDefs:           make(map[string]map[string]*JobDefinition),
		modelBiasJobDefs:             make(map[string]map[string]*JobDefinition),
		modelQualityJobDefs:          make(map[string]map[string]*JobDefinition),
		modelExplainJobDefs:          make(map[string]map[string]*JobDefinition),
		humanTaskUis:                 make(map[string]map[string]*HumanTaskUI),
		workforces:                   make(map[string]map[string]*Workforce),
		flowDefinitions:              make(map[string]map[string]*FlowDefinition),
		appImageConfigs:              make(map[string]map[string]*AppImageConfig),
		inferenceExperiments:         make(map[string]map[string]*InferenceExperiment),
		mlflowTrackingServers:        make(map[string]map[string]*MlflowTrackingServer),
		modelCards:                   make(map[string]map[string]*ModelCard),
		optimizationJobs:             make(map[string]map[string]*OptimizationJob),
		studioLifecycleConfigs:       make(map[string]map[string]*StudioLifecycleConfig),
		partnerApps:                  make(map[string]map[string]*PartnerApp),
		trainingPlans:                make(map[string]map[string]*TrainingPlan),
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
		domains:                      make(map[string]map[string]*Domain),
		userProfiles:                 make(map[string]map[userProfileKey]*UserProfile),
		apps:                         make(map[string]map[appKey]*App),
		featureGroups:                make(map[string]map[string]*FeatureGroup),
		featureRecords:               make(map[string]map[string]*FeatureRecord),
		featureMetadata:              make(map[string]map[string]*FeatureMetadata),
		pipelines:                    make(map[string]map[string]*Pipeline),
		pipelineExecutions:           make(map[string]map[string]*PipelineExecution),
		pipelineExecSteps:            make(map[string]map[string]*PipelineExecutionStep),
		experiments:                  make(map[string]map[string]*Experiment),
		trials:                       make(map[string]map[string]*Trial),
		trialComponents:              make(map[string]map[string]*TrialComponent),
		notebookLifecycleConfigs:     make(map[string]map[string]*NotebookInstanceLifecycleConfig),
		processingJobs:               make(map[string]map[string]*ProcessingJob),
		transformJobs:                make(map[string]map[string]*TransformJob),
		edgePackagingJobs:            make(map[string]map[string]*EdgePackagingJob),
		inferenceRecommendationsJobs: make(map[string]map[string]*InferenceRecommendationsJob),
		deviceFleets:                 make(map[string]map[string]*DeviceFleet),
		devices:                      make(map[string]map[deviceKey]*Device),
		inferenceComponents:          make(map[string]map[string]*InferenceComponent),
		clusterSchedulerConfigs:      make(map[string]map[string]*ClusterSchedulerConfig),
		computeQuotas:                make(map[string]map[string]*ComputeQuota),
		accountID:                    accountID,
		region:                       region,
		mu:                           lockmetrics.New("sagemaker"),
	}
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

func (b *InMemoryBackend) modelsStore(r string) map[string]*Model {
	if b.models[r] == nil {
		b.models[r] = make(map[string]*Model)
	}

	return b.models[r]
}
func (b *InMemoryBackend) endpointConfigsStore(r string) map[string]*EndpointConfig {
	if b.endpointConfigs[r] == nil {
		b.endpointConfigs[r] = make(map[string]*EndpointConfig)
	}

	return b.endpointConfigs[r]
}
func (b *InMemoryBackend) endpointsStore(r string) map[string]*Endpoint {
	if b.endpoints[r] == nil {
		b.endpoints[r] = make(map[string]*Endpoint)
	}

	return b.endpoints[r]
}
func (b *InMemoryBackend) trainingJobsStore(r string) map[string]*TrainingJob {
	if b.trainingJobs[r] == nil {
		b.trainingJobs[r] = make(map[string]*TrainingJob)
	}

	return b.trainingJobs[r]
}
func (b *InMemoryBackend) notebooksStore(r string) map[string]*NotebookInstance {
	if b.notebooks[r] == nil {
		b.notebooks[r] = make(map[string]*NotebookInstance)
	}

	return b.notebooks[r]
}
func (b *InMemoryBackend) hpTuningJobsStore(r string) map[string]*HyperParameterTuningJob {
	if b.hpTuningJobs[r] == nil {
		b.hpTuningJobs[r] = make(map[string]*HyperParameterTuningJob)
	}

	return b.hpTuningJobs[r]
}
func (b *InMemoryBackend) associationsStore(r string) map[string]*Association {
	if b.associations[r] == nil {
		b.associations[r] = make(map[string]*Association)
	}

	return b.associations[r]
}
func (b *InMemoryBackend) trialComponentAssociationsStore(r string) map[string]*TrialComponentAssociation {
	if b.trialComponentAssociations[r] == nil {
		b.trialComponentAssociations[r] = make(map[string]*TrialComponentAssociation)
	}

	return b.trialComponentAssociations[r]
}
func (b *InMemoryBackend) actionsStore(r string) map[string]*Action {
	if b.actions[r] == nil {
		b.actions[r] = make(map[string]*Action)
	}

	return b.actions[r]
}
func (b *InMemoryBackend) algorithmsStore(r string) map[string]*Algorithm {
	if b.algorithms[r] == nil {
		b.algorithms[r] = make(map[string]*Algorithm)
	}

	return b.algorithms[r]
}
func (b *InMemoryBackend) clustersStore(r string) map[string]*Cluster {
	if b.clusters[r] == nil {
		b.clusters[r] = make(map[string]*Cluster)
	}

	return b.clusters[r]
}
func (b *InMemoryBackend) modelPackagesStore(r string) map[string]*ModelPackage {
	if b.modelPackages[r] == nil {
		b.modelPackages[r] = make(map[string]*ModelPackage)
	}

	return b.modelPackages[r]
}
func (b *InMemoryBackend) modelPackageGroupsStore(r string) map[string]*ModelPackageGroup {
	if b.modelPackageGroups[r] == nil {
		b.modelPackageGroups[r] = make(map[string]*ModelPackageGroup)
	}

	return b.modelPackageGroups[r]
}
func (b *InMemoryBackend) autoMLJobsStore(r string) map[string]*AutoMLJob {
	if b.autoMLJobs[r] == nil {
		b.autoMLJobs[r] = make(map[string]*AutoMLJob)
	}

	return b.autoMLJobs[r]
}
func (b *InMemoryBackend) codeRepositoriesStore(r string) map[string]*CodeRepository {
	if b.codeRepositories[r] == nil {
		b.codeRepositories[r] = make(map[string]*CodeRepository)
	}

	return b.codeRepositories[r]
}
func (b *InMemoryBackend) projectsStore(r string) map[string]*Project {
	if b.projects[r] == nil {
		b.projects[r] = make(map[string]*Project)
	}

	return b.projects[r]
}
func (b *InMemoryBackend) spacesStore(r string) map[string]*Space {
	if b.spaces[r] == nil {
		b.spaces[r] = make(map[string]*Space)
	}

	return b.spaces[r]
}
func (b *InMemoryBackend) smImagesStore(r string) map[string]*SMImage {
	if b.smImages[r] == nil {
		b.smImages[r] = make(map[string]*SMImage)
	}

	return b.smImages[r]
}
func (b *InMemoryBackend) imageVersionsStore(r string) map[string]map[int]*ImageVersion {
	if b.imageVersions[r] == nil {
		b.imageVersions[r] = make(map[string]map[int]*ImageVersion)
	}

	return b.imageVersions[r]
}
func (b *InMemoryBackend) imageVersionCountsStore(r string) map[string]int {
	if b.imageVersionCounts[r] == nil {
		b.imageVersionCounts[r] = make(map[string]int)
	}

	return b.imageVersionCounts[r]
}
func (b *InMemoryBackend) compilationJobsStore(r string) map[string]*CompilationJob {
	if b.compilationJobs[r] == nil {
		b.compilationJobs[r] = make(map[string]*CompilationJob)
	}

	return b.compilationJobs[r]
}
func (b *InMemoryBackend) monitoringSchedulesStore(r string) map[string]*MonitoringSchedule {
	if b.monitoringSchedules[r] == nil {
		b.monitoringSchedules[r] = make(map[string]*MonitoringSchedule)
	}

	return b.monitoringSchedules[r]
}
func (b *InMemoryBackend) workteamsStore(r string) map[string]*Workteam {
	if b.workteams[r] == nil {
		b.workteams[r] = make(map[string]*Workteam)
	}

	return b.workteams[r]
}
func (b *InMemoryBackend) dataQualityJobDefsStore(r string) map[string]*JobDefinition {
	if b.dataQualityJobDefs[r] == nil {
		b.dataQualityJobDefs[r] = make(map[string]*JobDefinition)
	}

	return b.dataQualityJobDefs[r]
}
func (b *InMemoryBackend) modelBiasJobDefsStore(r string) map[string]*JobDefinition {
	if b.modelBiasJobDefs[r] == nil {
		b.modelBiasJobDefs[r] = make(map[string]*JobDefinition)
	}

	return b.modelBiasJobDefs[r]
}
func (b *InMemoryBackend) modelQualityJobDefsStore(r string) map[string]*JobDefinition {
	if b.modelQualityJobDefs[r] == nil {
		b.modelQualityJobDefs[r] = make(map[string]*JobDefinition)
	}

	return b.modelQualityJobDefs[r]
}
func (b *InMemoryBackend) modelExplainJobDefsStore(r string) map[string]*JobDefinition {
	if b.modelExplainJobDefs[r] == nil {
		b.modelExplainJobDefs[r] = make(map[string]*JobDefinition)
	}

	return b.modelExplainJobDefs[r]
}
func (b *InMemoryBackend) humanTaskUisStore(r string) map[string]*HumanTaskUI {
	if b.humanTaskUis[r] == nil {
		b.humanTaskUis[r] = make(map[string]*HumanTaskUI)
	}

	return b.humanTaskUis[r]
}
func (b *InMemoryBackend) workforcesStore(r string) map[string]*Workforce {
	if b.workforces[r] == nil {
		b.workforces[r] = make(map[string]*Workforce)
	}

	return b.workforces[r]
}
func (b *InMemoryBackend) flowDefinitionsStore(r string) map[string]*FlowDefinition {
	if b.flowDefinitions[r] == nil {
		b.flowDefinitions[r] = make(map[string]*FlowDefinition)
	}

	return b.flowDefinitions[r]
}
func (b *InMemoryBackend) appImageConfigsStore(r string) map[string]*AppImageConfig {
	if b.appImageConfigs[r] == nil {
		b.appImageConfigs[r] = make(map[string]*AppImageConfig)
	}

	return b.appImageConfigs[r]
}
func (b *InMemoryBackend) inferenceExperimentsStore(r string) map[string]*InferenceExperiment {
	if b.inferenceExperiments[r] == nil {
		b.inferenceExperiments[r] = make(map[string]*InferenceExperiment)
	}

	return b.inferenceExperiments[r]
}
func (b *InMemoryBackend) mlflowTrackingServersStore(r string) map[string]*MlflowTrackingServer {
	if b.mlflowTrackingServers[r] == nil {
		b.mlflowTrackingServers[r] = make(map[string]*MlflowTrackingServer)
	}

	return b.mlflowTrackingServers[r]
}
func (b *InMemoryBackend) modelCardsStore(r string) map[string]*ModelCard {
	if b.modelCards[r] == nil {
		b.modelCards[r] = make(map[string]*ModelCard)
	}

	return b.modelCards[r]
}
func (b *InMemoryBackend) optimizationJobsStore(r string) map[string]*OptimizationJob {
	if b.optimizationJobs[r] == nil {
		b.optimizationJobs[r] = make(map[string]*OptimizationJob)
	}

	return b.optimizationJobs[r]
}
func (b *InMemoryBackend) studioLifecycleConfigsStore(r string) map[string]*StudioLifecycleConfig {
	if b.studioLifecycleConfigs[r] == nil {
		b.studioLifecycleConfigs[r] = make(map[string]*StudioLifecycleConfig)
	}

	return b.studioLifecycleConfigs[r]
}
func (b *InMemoryBackend) partnerAppsStore(r string) map[string]*PartnerApp {
	if b.partnerApps[r] == nil {
		b.partnerApps[r] = make(map[string]*PartnerApp)
	}

	return b.partnerApps[r]
}
func (b *InMemoryBackend) trainingPlansStore(r string) map[string]*TrainingPlan {
	if b.trainingPlans[r] == nil {
		b.trainingPlans[r] = make(map[string]*TrainingPlan)
	}

	return b.trainingPlans[r]
}
func (b *InMemoryBackend) modelARNIndexStore(r string) map[string]string {
	if b.modelARNIndex[r] == nil {
		b.modelARNIndex[r] = make(map[string]string)
	}

	return b.modelARNIndex[r]
}
func (b *InMemoryBackend) endpointConfigARNIndexStore(r string) map[string]string {
	if b.endpointConfigARNIndex[r] == nil {
		b.endpointConfigARNIndex[r] = make(map[string]string)
	}

	return b.endpointConfigARNIndex[r]
}
func (b *InMemoryBackend) endpointARNIndexStore(r string) map[string]string {
	if b.endpointARNIndex[r] == nil {
		b.endpointARNIndex[r] = make(map[string]string)
	}

	return b.endpointARNIndex[r]
}
func (b *InMemoryBackend) trainingJobARNIndexStore(r string) map[string]string {
	if b.trainingJobARNIndex[r] == nil {
		b.trainingJobARNIndex[r] = make(map[string]string)
	}

	return b.trainingJobARNIndex[r]
}
func (b *InMemoryBackend) notebookARNIndexStore(r string) map[string]string {
	if b.notebookARNIndex[r] == nil {
		b.notebookARNIndex[r] = make(map[string]string)
	}

	return b.notebookARNIndex[r]
}
func (b *InMemoryBackend) hpTuningJobARNIndexStore(r string) map[string]string {
	if b.hpTuningJobARNIndex[r] == nil {
		b.hpTuningJobARNIndex[r] = make(map[string]string)
	}

	return b.hpTuningJobARNIndex[r]
}
func (b *InMemoryBackend) actionARNIndexStore(r string) map[string]string {
	if b.actionARNIndex[r] == nil {
		b.actionARNIndex[r] = make(map[string]string)
	}

	return b.actionARNIndex[r]
}
func (b *InMemoryBackend) algorithmARNIndexStore(r string) map[string]string {
	if b.algorithmARNIndex[r] == nil {
		b.algorithmARNIndex[r] = make(map[string]string)
	}

	return b.algorithmARNIndex[r]
}
func (b *InMemoryBackend) clusterARNIndexStore(r string) map[string]string {
	if b.clusterARNIndex[r] == nil {
		b.clusterARNIndex[r] = make(map[string]string)
	}

	return b.clusterARNIndex[r]
}
func (b *InMemoryBackend) modelPackageARNIndexStore(r string) map[string]string {
	if b.modelPackageARNIndex[r] == nil {
		b.modelPackageARNIndex[r] = make(map[string]string)
	}

	return b.modelPackageARNIndex[r]
}
func (b *InMemoryBackend) processingJobARNIndexStore(r string) map[string]string {
	if b.processingJobARNIndex[r] == nil {
		b.processingJobARNIndex[r] = make(map[string]string)
	}

	return b.processingJobARNIndex[r]
}
func (b *InMemoryBackend) transformJobARNIndexStore(r string) map[string]string {
	if b.transformJobARNIndex[r] == nil {
		b.transformJobARNIndex[r] = make(map[string]string)
	}

	return b.transformJobARNIndex[r]
}
func (b *InMemoryBackend) domainsStore(r string) map[string]*Domain {
	if b.domains[r] == nil {
		b.domains[r] = make(map[string]*Domain)
	}

	return b.domains[r]
}
func (b *InMemoryBackend) userProfilesStore(r string) map[userProfileKey]*UserProfile {
	if b.userProfiles[r] == nil {
		b.userProfiles[r] = make(map[userProfileKey]*UserProfile)
	}

	return b.userProfiles[r]
}
func (b *InMemoryBackend) appsStore(r string) map[appKey]*App {
	if b.apps[r] == nil {
		b.apps[r] = make(map[appKey]*App)
	}

	return b.apps[r]
}
func (b *InMemoryBackend) featureGroupsStore(r string) map[string]*FeatureGroup {
	if b.featureGroups[r] == nil {
		b.featureGroups[r] = make(map[string]*FeatureGroup)
	}

	return b.featureGroups[r]
}
func (b *InMemoryBackend) featureRecordsStore(r string) map[string]*FeatureRecord {
	if b.featureRecords[r] == nil {
		b.featureRecords[r] = make(map[string]*FeatureRecord)
	}

	return b.featureRecords[r]
}
func (b *InMemoryBackend) featureMetadataStore(r string) map[string]*FeatureMetadata {
	if b.featureMetadata[r] == nil {
		b.featureMetadata[r] = make(map[string]*FeatureMetadata)
	}

	return b.featureMetadata[r]
}
func (b *InMemoryBackend) pipelinesStore(r string) map[string]*Pipeline {
	if b.pipelines[r] == nil {
		b.pipelines[r] = make(map[string]*Pipeline)
	}

	return b.pipelines[r]
}
func (b *InMemoryBackend) pipelineExecutionsStore(r string) map[string]*PipelineExecution {
	if b.pipelineExecutions[r] == nil {
		b.pipelineExecutions[r] = make(map[string]*PipelineExecution)
	}

	return b.pipelineExecutions[r]
}
func (b *InMemoryBackend) pipelineExecStepsStore(r string) map[string]*PipelineExecutionStep {
	if b.pipelineExecSteps[r] == nil {
		b.pipelineExecSteps[r] = make(map[string]*PipelineExecutionStep)
	}

	return b.pipelineExecSteps[r]
}
func (b *InMemoryBackend) experimentsStore(r string) map[string]*Experiment {
	if b.experiments[r] == nil {
		b.experiments[r] = make(map[string]*Experiment)
	}

	return b.experiments[r]
}
func (b *InMemoryBackend) trialsStore(r string) map[string]*Trial {
	if b.trials[r] == nil {
		b.trials[r] = make(map[string]*Trial)
	}

	return b.trials[r]
}
func (b *InMemoryBackend) trialComponentsStore(r string) map[string]*TrialComponent {
	if b.trialComponents[r] == nil {
		b.trialComponents[r] = make(map[string]*TrialComponent)
	}

	return b.trialComponents[r]
}
func (b *InMemoryBackend) notebookLifecycleConfigsStore(r string) map[string]*NotebookInstanceLifecycleConfig {
	if b.notebookLifecycleConfigs[r] == nil {
		b.notebookLifecycleConfigs[r] = make(map[string]*NotebookInstanceLifecycleConfig)
	}

	return b.notebookLifecycleConfigs[r]
}
func (b *InMemoryBackend) processingJobsStore(r string) map[string]*ProcessingJob {
	if b.processingJobs[r] == nil {
		b.processingJobs[r] = make(map[string]*ProcessingJob)
	}

	return b.processingJobs[r]
}
func (b *InMemoryBackend) transformJobsStore(r string) map[string]*TransformJob {
	if b.transformJobs[r] == nil {
		b.transformJobs[r] = make(map[string]*TransformJob)
	}

	return b.transformJobs[r]
}
func (b *InMemoryBackend) edgePackagingJobsStore(r string) map[string]*EdgePackagingJob {
	if b.edgePackagingJobs[r] == nil {
		b.edgePackagingJobs[r] = make(map[string]*EdgePackagingJob)
	}

	return b.edgePackagingJobs[r]
}
func (b *InMemoryBackend) inferenceRecommendationsJobsStore(r string) map[string]*InferenceRecommendationsJob {
	if b.inferenceRecommendationsJobs[r] == nil {
		b.inferenceRecommendationsJobs[r] = make(map[string]*InferenceRecommendationsJob)
	}

	return b.inferenceRecommendationsJobs[r]
}
func (b *InMemoryBackend) deviceFleetsStore(r string) map[string]*DeviceFleet {
	if b.deviceFleets[r] == nil {
		b.deviceFleets[r] = make(map[string]*DeviceFleet)
	}

	return b.deviceFleets[r]
}
func (b *InMemoryBackend) devicesStore(r string) map[deviceKey]*Device {
	if b.devices[r] == nil {
		b.devices[r] = make(map[deviceKey]*Device)
	}

	return b.devices[r]
}
func (b *InMemoryBackend) inferenceComponentsStore(r string) map[string]*InferenceComponent {
	if b.inferenceComponents[r] == nil {
		b.inferenceComponents[r] = make(map[string]*InferenceComponent)
	}

	return b.inferenceComponents[r]
}
func (b *InMemoryBackend) clusterSchedulerConfigsStore(r string) map[string]*ClusterSchedulerConfig {
	if b.clusterSchedulerConfigs[r] == nil {
		b.clusterSchedulerConfigs[r] = make(map[string]*ClusterSchedulerConfig)
	}

	return b.clusterSchedulerConfigs[r]
}
func (b *InMemoryBackend) computeQuotasStore(r string) map[string]*ComputeQuota {
	if b.computeQuotas[r] == nil {
		b.computeQuotas[r] = make(map[string]*ComputeQuota)
	}

	return b.computeQuotas[r]
}

// Reset reinitialises all maps to empty, clearing all stored resources.
//
//nolint:funlen // Reset must reinitialise all maps; splitting would obscure the invariant
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.models = make(map[string]map[string]*Model)
	b.endpointConfigs = make(map[string]map[string]*EndpointConfig)
	b.endpoints = make(map[string]map[string]*Endpoint)
	b.trainingJobs = make(map[string]map[string]*TrainingJob)
	b.notebooks = make(map[string]map[string]*NotebookInstance)
	b.hpTuningJobs = make(map[string]map[string]*HyperParameterTuningJob)
	b.associations = make(map[string]map[string]*Association)
	b.trialComponentAssociations = make(map[string]map[string]*TrialComponentAssociation)
	b.actions = make(map[string]map[string]*Action)
	b.artifacts = make(map[string]map[string]*Artifact)
	b.contexts = make(map[string]map[string]*Context)
	b.algorithms = make(map[string]map[string]*Algorithm)
	b.clusters = make(map[string]map[string]*Cluster)
	b.modelPackages = make(map[string]map[string]*ModelPackage)
	b.modelPackageGroups = make(map[string]map[string]*ModelPackageGroup)
	b.autoMLJobs = make(map[string]map[string]*AutoMLJob)
	b.codeRepositories = make(map[string]map[string]*CodeRepository)
	b.projects = make(map[string]map[string]*Project)
	b.spaces = make(map[string]map[string]*Space)
	b.smImages = make(map[string]map[string]*SMImage)
	b.imageVersions = make(map[string]map[string]map[int]*ImageVersion)
	b.imageVersionCounts = make(map[string]map[string]int)
	b.compilationJobs = make(map[string]map[string]*CompilationJob)
	b.monitoringSchedules = make(map[string]map[string]*MonitoringSchedule)
	b.workteams = make(map[string]map[string]*Workteam)
	b.dataQualityJobDefs = make(map[string]map[string]*JobDefinition)
	b.modelBiasJobDefs = make(map[string]map[string]*JobDefinition)
	b.modelQualityJobDefs = make(map[string]map[string]*JobDefinition)
	b.modelExplainJobDefs = make(map[string]map[string]*JobDefinition)
	b.humanTaskUis = make(map[string]map[string]*HumanTaskUI)
	b.workforces = make(map[string]map[string]*Workforce)
	b.flowDefinitions = make(map[string]map[string]*FlowDefinition)
	b.appImageConfigs = make(map[string]map[string]*AppImageConfig)
	b.inferenceExperiments = make(map[string]map[string]*InferenceExperiment)
	b.mlflowTrackingServers = make(map[string]map[string]*MlflowTrackingServer)
	b.modelCards = make(map[string]map[string]*ModelCard)
	b.optimizationJobs = make(map[string]map[string]*OptimizationJob)
	b.studioLifecycleConfigs = make(map[string]map[string]*StudioLifecycleConfig)
	b.partnerApps = make(map[string]map[string]*PartnerApp)
	b.trainingPlans = make(map[string]map[string]*TrainingPlan)
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
	b.domains = make(map[string]map[string]*Domain)
	b.userProfiles = make(map[string]map[userProfileKey]*UserProfile)
	b.apps = make(map[string]map[appKey]*App)
	b.featureGroups = make(map[string]map[string]*FeatureGroup)
	b.featureRecords = make(map[string]map[string]*FeatureRecord)
	b.featureMetadata = make(map[string]map[string]*FeatureMetadata)
	b.pipelines = make(map[string]map[string]*Pipeline)
	b.pipelineExecutions = make(map[string]map[string]*PipelineExecution)
	b.pipelineExecSteps = make(map[string]map[string]*PipelineExecutionStep)
	b.experiments = make(map[string]map[string]*Experiment)
	b.trials = make(map[string]map[string]*Trial)
	b.trialComponents = make(map[string]map[string]*TrialComponent)
	b.notebookLifecycleConfigs = make(map[string]map[string]*NotebookInstanceLifecycleConfig)
	b.processingJobs = make(map[string]map[string]*ProcessingJob)
	b.transformJobs = make(map[string]map[string]*TransformJob)
	b.edgePackagingJobs = make(map[string]map[string]*EdgePackagingJob)
	b.inferenceRecommendationsJobs = make(map[string]map[string]*InferenceRecommendationsJob)
	b.deviceFleets = make(map[string]map[string]*DeviceFleet)
	b.devices = make(map[string]map[deviceKey]*Device)
	b.inferenceComponents = make(map[string]map[string]*InferenceComponent)
	b.clusterSchedulerConfigs = make(map[string]map[string]*ClusterSchedulerConfig)
	b.computeQuotas = make(map[string]map[string]*ComputeQuota)
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

	if _, ok := models[name]; ok {
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
	models[name] = m
	b.modelARNIndexStore(region)[modelARN] = name

	return cloneModel(m), nil
}

// DescribeModel returns a model by name.
func (b *InMemoryBackend) DescribeModel(ctx context.Context, name string) (*Model, error) {
	b.mu.RLock("DescribeModel")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	m, ok := b.modelsStore(region)[name]
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

	return sagemakerListPaged(b.modelsStore(region), nextToken, cloneModel,
		func(a, b *Model) bool { return a.ModelName < b.ModelName })
}

// DeleteModel deletes a model by name.
func (b *InMemoryBackend) DeleteModel(ctx context.Context, name string) error {
	b.mu.Lock("DeleteModel")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	models := b.modelsStore(region)

	m, ok := models[name]
	if !ok {
		return fmt.Errorf("%w: could not find model %q", ErrModelNotFound, name)
	}

	arnIndex := b.modelARNIndexStore(region)
	delete(arnIndex, m.ModelARN)
	delete(models, name)

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

	m, ok := b.modelsStore(region)[name]
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

	if _, ok := ecStore[name]; ok {
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
	ecStore[name] = ec
	b.endpointConfigARNIndexStore(region)[configARN] = name

	return cloneEndpointConfig(ec), nil
}

// DescribeEndpointConfig returns an endpoint config by name.
func (b *InMemoryBackend) DescribeEndpointConfig(ctx context.Context, name string) (*EndpointConfig, error) {
	b.mu.RLock("DescribeEndpointConfig")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	ec, ok := b.endpointConfigsStore(region)[name]
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

	return sagemakerListPaged(b.endpointConfigsStore(region), nextToken, cloneEndpointConfig,
		func(a, b *EndpointConfig) bool { return a.EndpointConfigName < b.EndpointConfigName })
}

// DeleteEndpointConfig deletes an endpoint configuration by name.
func (b *InMemoryBackend) DeleteEndpointConfig(ctx context.Context, name string) error {
	b.mu.Lock("DeleteEndpointConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ecStore := b.endpointConfigsStore(region)

	ec, ok := ecStore[name]
	if !ok {
		return fmt.Errorf(
			"%w: could not find endpoint configuration %q",
			ErrEndpointConfigNotFound,
			name,
		)
	}

	arnIndex := b.endpointConfigARNIndexStore(region)
	delete(arnIndex, ec.EndpointConfigARN)
	delete(ecStore, name)

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

	ec, ok := b.endpointConfigsStore(region)[name]
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
		m := b.modelsStore(region)[name]
		m.Tags = mergeTags(m.Tags, tags)

		return nil
	}

	if name, ok := b.endpointConfigARNIndexStore(region)[resourceARN]; ok {
		ec := b.endpointConfigsStore(region)[name]
		ec.Tags = mergeTags(ec.Tags, tags)

		return nil
	}

	if name, ok := b.actionARNIndexStore(region)[resourceARN]; ok {
		a := b.actionsStore(region)[name]
		a.Tags = mergeTags(a.Tags, tags)

		return nil
	}

	if name, ok := b.algorithmARNIndexStore(region)[resourceARN]; ok {
		al := b.algorithmsStore(region)[name]
		al.Tags = mergeTags(al.Tags, tags)

		return nil
	}

	if _, ok := b.modelPackageARNIndexStore(region)[resourceARN]; ok {
		mp := b.modelPackagesStore(region)[resourceARN]
		mp.Tags = mergeTags(mp.Tags, tags)

		return nil
	}

	if name, ok := b.endpointARNIndexStore(region)[resourceARN]; ok {
		ep := b.endpointsStore(region)[name]
		ep.Tags = mergeTags(ep.Tags, tags)

		return nil
	}

	if name, ok := b.trainingJobARNIndexStore(region)[resourceARN]; ok {
		tj := b.trainingJobsStore(region)[name]
		tj.Tags = mergeTags(tj.Tags, tags)

		return nil
	}

	if name, ok := b.notebookARNIndexStore(region)[resourceARN]; ok {
		nb := b.notebooksStore(region)[name]
		nb.Tags = mergeTags(nb.Tags, tags)

		return nil
	}

	if name, ok := b.hpTuningJobARNIndexStore(region)[resourceARN]; ok {
		j := b.hpTuningJobsStore(region)[name]
		j.Tags = mergeTags(j.Tags, tags)

		return nil
	}

	if name, ok := b.processingJobARNIndexStore(region)[resourceARN]; ok {
		if pj, found := b.processingJobsStore(region)[name]; found {
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
	if name, ok := b.modelARNIndexStore(region)[resourceARN]; ok {
		return &b.modelsStore(region)[name].Tags
	}

	if name, ok := b.endpointConfigARNIndexStore(region)[resourceARN]; ok {
		return &b.endpointConfigsStore(region)[name].Tags
	}

	if name, ok := b.actionARNIndexStore(region)[resourceARN]; ok {
		return &b.actionsStore(region)[name].Tags
	}

	if name, ok := b.algorithmARNIndexStore(region)[resourceARN]; ok {
		return &b.algorithmsStore(region)[name].Tags
	}

	if _, ok := b.modelPackageARNIndexStore(region)[resourceARN]; ok {
		return &b.modelPackagesStore(region)[resourceARN].Tags
	}

	if name, ok := b.endpointARNIndexStore(region)[resourceARN]; ok {
		return &b.endpointsStore(region)[name].Tags
	}

	if name, ok := b.trainingJobARNIndexStore(region)[resourceARN]; ok {
		return &b.trainingJobsStore(region)[name].Tags
	}

	if name, ok := b.notebookARNIndexStore(region)[resourceARN]; ok {
		return &b.notebooksStore(region)[name].Tags
	}

	if name, ok := b.hpTuningJobARNIndexStore(region)[resourceARN]; ok {
		return &b.hpTuningJobsStore(region)[name].Tags
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
	if name, ok := b.processingJobARNIndexStore(region)[resourceARN]; ok {
		if pj, found := b.processingJobsStore(region)[name]; found {
			return &pj.Tags
		}
	}

	if name, ok := b.transformJobARNIndexStore(region)[resourceARN]; ok {
		if tj, found := b.transformJobsStore(region)[name]; found {
			return &tj.Tags
		}
	}

	if name, ok := b.clusterARNIndexStore(region)[resourceARN]; ok {
		if c, found := b.clustersStore(region)[name]; found {
			return &c.Tags
		}
	}

	return nil
}

// findTagMapStatefulLocked handles tag lookups for stateful resources (domains,
// featureGroups, pipelines, experiments, trials, trialComponents). Separated
// to keep findTagMapLocked within cognitive-complexity limits.
func (b *InMemoryBackend) findTagMapStatefulLocked(resourceARN string, region string) *map[string]string {
	for _, d := range b.domainsStore(region) {
		if d.DomainArn == resourceARN {
			return &d.Tags
		}
	}
	for _, fg := range b.featureGroupsStore(region) {
		if fg.FeatureGroupArn == resourceARN {
			return &fg.Tags
		}
	}
	for _, p := range b.pipelinesStore(region) {
		if p.PipelineArn == resourceARN {
			return &p.Tags
		}
	}
	for _, e := range b.experimentsStore(region) {
		if e.ExperimentArn == resourceARN {
			return &e.Tags
		}
	}
	for _, t := range b.trialsStore(region) {
		if t.TrialArn == resourceARN {
			return &t.Tags
		}
	}
	for _, tc := range b.trialComponentsStore(region) {
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
	if _, ok := assocStore[key]; ok {
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
	assocStore[key] = a

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
	if _, ok := tcaStore[key]; ok {
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
	tcaStore[key] = assoc

	return cloneTrialComponentAssociation(assoc), nil
}

// ensureClusterLocked looks up a cluster by name (must be called with lock held).
func (b *InMemoryBackend) ensureClusterLocked(ctx context.Context, clusterName string) (*Cluster, error) {
	region := getRegion(ctx, b.region)

	c, ok := b.clustersStore(region)[clusterName]
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
	b.clustersStore(region)[clusterName] = c
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
	b.actionsStore(region)[name] = a
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
	b.algorithmsStore(region)[name] = al
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
	mpStore := b.modelPackagesStore(region)

	results := make(map[string]ModelPackageBatchResult, len(modelPackageArns))

	for _, arnStr := range modelPackageArns {
		mp, ok := mpStore[arnStr]
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

	if _, ok := actionsStore[name]; ok {
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
	actionsStore[name] = a
	b.actionARNIndexStore(region)[actionARN] = name

	return cloneAction(a), nil
}

// CreateAlgorithm creates a SageMaker algorithm specification.
func (b *InMemoryBackend) CreateAlgorithm(
	ctx context.Context,
	name, description string,
	tags map[string]string,
) (*Algorithm, error) {
	b.mu.Lock("CreateAlgorithm")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: AlgorithmName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	algoStore := b.algorithmsStore(region)

	if _, ok := algoStore[name]; ok {
		return nil, fmt.Errorf("%w: algorithm %q already exists", ErrAlgorithmAlreadyExists, name)
	}

	algorithmARN := arn.Build("sagemaker", region, b.accountID, "algorithm/"+name)

	al := &Algorithm{
		AlgorithmName:        name,
		AlgorithmArn:         algorithmARN,
		AlgorithmDescription: description,
		AlgorithmStatus:      algorithmStatusCompleted,
		Tags:                 mergeTags(nil, tags),
		CreationTime:         time.Now(),
	}
	algoStore[name] = al
	b.algorithmARNIndexStore(region)[algorithmARN] = name

	return cloneAlgorithm(al), nil
}

// AddModelPackageInternal adds a model package directly for testing.
func (b *InMemoryBackend) AddModelPackageInternal(ctx context.Context, mp *ModelPackage) {
	b.mu.Lock("AddModelPackageInternal")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.modelPackagesStore(region)[mp.ModelPackageArn] = mp
	b.modelPackageARNIndexStore(region)[mp.ModelPackageArn] = mp.ModelPackageArn
}

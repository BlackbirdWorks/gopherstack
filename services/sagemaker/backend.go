package sagemaker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// generateID returns a random hex string of the given byte length (output chars = 2*n).
func generateID(n int) string {
	b := make([]byte, n)
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
	// ErrModelPackageNotFound is returned when a model package does not exist.
	ErrModelPackageNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrValidation is returned for invalid input parameters.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
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
	CreationTime time.Time         `json:"CreationTime"`
	Tags         map[string]string `json:"Tags,omitempty"`
	Properties   map[string]string `json:"Properties,omitempty"`
	Source       ActionSource      `json:"Source"`
	ActionName   string            `json:"ActionName"`
	ActionArn    string            `json:"ActionArn"`
	ActionType   string            `json:"ActionType"`
	Description  string            `json:"Description,omitempty"`
	Status       string            `json:"Status,omitempty"`
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

	return &cp
}

// ClusterNodeVolume represents a volume attached to a cluster node.
type ClusterNodeVolume struct {
	VolumeName string `json:"VolumeName"`
	SizeInGB   int32  `json:"SizeInGB,omitempty"`
}

// ClusterNode represents a node in a SageMaker cluster.
type ClusterNode struct {
	NodeID       string              `json:"NodeId"`
	InstanceType string              `json:"InstanceType,omitempty"`
	NodeStatus   string              `json:"NodeStatus"`
	Volumes      []ClusterNodeVolume `json:"Volumes,omitempty"`
}

// Cluster represents a SageMaker HyperPod cluster.
type Cluster struct {
	CreationTime  time.Time               `json:"CreationTime"`
	Nodes         map[string]*ClusterNode `json:"-"`
	ClusterArn    string                  `json:"ClusterArn"`
	ClusterName   string                  `json:"ClusterName"`
	ClusterStatus string                  `json:"ClusterStatus"`
}

// ModelPackage represents a SageMaker model package.
type ModelPackage struct {
	CreationTime            time.Time         `json:"CreationTime"`
	Tags                    map[string]string `json:"Tags,omitempty"`
	ModelPackageName        string            `json:"ModelPackageName"`
	ModelPackageArn         string            `json:"ModelPackageArn"`
	ModelPackageGroupName   string            `json:"ModelPackageGroupName,omitempty"`
	ModelPackageStatus      string            `json:"ModelPackageStatus"`
	ModelPackageDescription string            `json:"ModelPackageDescription,omitempty"`
}

// cloneModelPackage returns a deep copy of mp.
func cloneModelPackage(mp *ModelPackage) *ModelPackage {
	cp := *mp
	cp.Tags = maps.Clone(mp.Tags)

	return &cp
}

// InMemoryBackend is an in-memory store for SageMaker resources.
type InMemoryBackend struct {
	models                     map[string]*Model
	endpointConfigs            map[string]*EndpointConfig
	endpoints                  map[string]*Endpoint                        // key: endpointName
	trainingJobs               map[string]*TrainingJob                     // key: jobName
	notebooks                  map[string]*NotebookInstance                // key: instanceName
	hpTuningJobs               map[string]*HyperParameterTuningJob         // key: jobName
	associations               map[string]*Association                     // key: sourceArn+"|"+destinationArn
	trialComponentAssociations map[string]*TrialComponentAssociation       // key: trialName+"|"+componentName
	actions                    map[string]*Action                          // key: actionName
	algorithms                 map[string]*Algorithm                       // key: algorithmName
	clusters                   map[string]*Cluster                         // key: clusterName
	modelPackages              map[string]*ModelPackage                    // key: modelPackageArn
	modelARNIndex              map[string]string                           // ARN → model name
	endpointConfigARNIndex     map[string]string                           // ARN → endpoint config name
	endpointARNIndex           map[string]string                           // ARN → endpoint name
	trainingJobARNIndex        map[string]string                           // ARN → training job name
	notebookARNIndex           map[string]string                           // ARN → notebook instance name
	hpTuningJobARNIndex        map[string]string                           // ARN → HP tuning job name
	actionARNIndex             map[string]string                           // ARN → action name
	algorithmARNIndex          map[string]string                           // ARN → algorithm name
	clusterARNIndex            map[string]string                           // ARN → cluster name
	modelPackageARNIndex       map[string]string                           // ARN → model package ARN
	domains                    map[string]*Domain                          // key: domainID
	userProfiles               map[userProfileKey]*UserProfile             // key: domainID+name
	apps                       map[appKey]*App                             // key: domainID+userProfile+appType+appName
	featureGroups              map[string]*FeatureGroup                    // key: featureGroupName
	featureRecords             map[string]*FeatureRecord                   // key: groupName|recordID
	featureMetadata            map[string]*FeatureMetadata                 // key: groupName/featureName
	pipelines                  map[string]*Pipeline                        // key: pipelineName
	pipelineExecutions         map[string]*PipelineExecution               // key: executionArn
	pipelineExecSteps          map[string]*PipelineExecutionStep           // key: execArn|stepName
	experiments                map[string]*Experiment                      // key: experimentName
	trials                     map[string]*Trial                           // key: trialName
	trialComponents            map[string]*TrialComponent                  // key: trialComponentName
	notebookLifecycleConfigs   map[string]*NotebookInstanceLifecycleConfig // key: configName
	processingJobs             map[string]*ProcessingJob                   // key: jobName
	processingJobARNIndex      map[string]string                           // ARN → job name
	lifecycleCtx               context.Context
	lifecycleCancel            context.CancelFunc
	mu                         *lockmetrics.RWMutex
	accountID                  string
	region                     string
}

// NewInMemoryBackend creates a new in-memory SageMaker backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		models:                     make(map[string]*Model),
		endpointConfigs:            make(map[string]*EndpointConfig),
		endpoints:                  make(map[string]*Endpoint),
		trainingJobs:               make(map[string]*TrainingJob),
		notebooks:                  make(map[string]*NotebookInstance),
		hpTuningJobs:               make(map[string]*HyperParameterTuningJob),
		associations:               make(map[string]*Association),
		trialComponentAssociations: make(map[string]*TrialComponentAssociation),
		actions:                    make(map[string]*Action),
		algorithms:                 make(map[string]*Algorithm),
		clusters:                   make(map[string]*Cluster),
		modelPackages:              make(map[string]*ModelPackage),
		modelARNIndex:              make(map[string]string),
		endpointConfigARNIndex:     make(map[string]string),
		endpointARNIndex:           make(map[string]string),
		trainingJobARNIndex:        make(map[string]string),
		notebookARNIndex:           make(map[string]string),
		hpTuningJobARNIndex:        make(map[string]string),
		actionARNIndex:             make(map[string]string),
		algorithmARNIndex:          make(map[string]string),
		clusterARNIndex:            make(map[string]string),
		modelPackageARNIndex:       make(map[string]string),
		domains:                    make(map[string]*Domain),
		userProfiles:               make(map[userProfileKey]*UserProfile),
		apps:                       make(map[appKey]*App),
		featureGroups:              make(map[string]*FeatureGroup),
		featureRecords:             make(map[string]*FeatureRecord),
		featureMetadata:            make(map[string]*FeatureMetadata),
		pipelines:                  make(map[string]*Pipeline),
		pipelineExecutions:         make(map[string]*PipelineExecution),
		pipelineExecSteps:          make(map[string]*PipelineExecutionStep),
		experiments:                make(map[string]*Experiment),
		trials:                     make(map[string]*Trial),
		trialComponents:            make(map[string]*TrialComponent),
		notebookLifecycleConfigs:   make(map[string]*NotebookInstanceLifecycleConfig),
		processingJobs:             make(map[string]*ProcessingJob),
		processingJobARNIndex:      make(map[string]string),
		accountID:                  accountID,
		region:                     region,
		mu:                         lockmetrics.New("sagemaker"),
	}
	b.resetLifecycleContext()

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset reinitialises all maps to empty, clearing all stored resources.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.models = make(map[string]*Model)
	b.endpointConfigs = make(map[string]*EndpointConfig)
	b.endpoints = make(map[string]*Endpoint)
	b.trainingJobs = make(map[string]*TrainingJob)
	b.notebooks = make(map[string]*NotebookInstance)
	b.hpTuningJobs = make(map[string]*HyperParameterTuningJob)
	b.associations = make(map[string]*Association)
	b.trialComponentAssociations = make(map[string]*TrialComponentAssociation)
	b.actions = make(map[string]*Action)
	b.algorithms = make(map[string]*Algorithm)
	b.clusters = make(map[string]*Cluster)
	b.modelPackages = make(map[string]*ModelPackage)
	b.modelARNIndex = make(map[string]string)
	b.endpointConfigARNIndex = make(map[string]string)
	b.endpointARNIndex = make(map[string]string)
	b.trainingJobARNIndex = make(map[string]string)
	b.notebookARNIndex = make(map[string]string)
	b.hpTuningJobARNIndex = make(map[string]string)
	b.actionARNIndex = make(map[string]string)
	b.algorithmARNIndex = make(map[string]string)
	b.clusterARNIndex = make(map[string]string)
	b.modelPackageARNIndex = make(map[string]string)
	b.domains = make(map[string]*Domain)
	b.userProfiles = make(map[userProfileKey]*UserProfile)
	b.apps = make(map[appKey]*App)
	b.featureGroups = make(map[string]*FeatureGroup)
	b.featureRecords = make(map[string]*FeatureRecord)
	b.featureMetadata = make(map[string]*FeatureMetadata)
	b.pipelines = make(map[string]*Pipeline)
	b.pipelineExecutions = make(map[string]*PipelineExecution)
	b.pipelineExecSteps = make(map[string]*PipelineExecutionStep)
	b.experiments = make(map[string]*Experiment)
	b.trials = make(map[string]*Trial)
	b.trialComponents = make(map[string]*TrialComponent)
	b.notebookLifecycleConfigs = make(map[string]*NotebookInstanceLifecycleConfig)
	b.processingJobs = make(map[string]*ProcessingJob)
	b.processingJobARNIndex = make(map[string]string)
	// Cancel pending goroutines and start fresh lifecycle context.
	b.resetLifecycleContext()
}

// CreateModel creates a new SageMaker model.
func (b *InMemoryBackend) CreateModel(
	name string,
	executionRoleARN string,
	primaryContainer *ContainerDefinition,
	containers []ContainerDefinition,
	tags map[string]string,
) (*Model, error) {
	b.mu.Lock("CreateModel")
	defer b.mu.Unlock()

	if _, ok := b.models[name]; ok {
		return nil, fmt.Errorf("%w: model %s already exists", ErrModelAlreadyExists, name)
	}

	modelARN := arn.Build("sagemaker", b.region, b.accountID, "model/"+name)

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
	b.models[name] = m
	b.modelARNIndex[modelARN] = name

	return cloneModel(m), nil
}

// DescribeModel returns a model by name.
func (b *InMemoryBackend) DescribeModel(name string) (*Model, error) {
	b.mu.RLock("DescribeModel")
	defer b.mu.RUnlock()

	m, ok := b.models[name]
	if !ok {
		return nil, fmt.Errorf("%w: could not find model %q", ErrModelNotFound, name)
	}

	return cloneModel(m), nil
}

// ListModels returns models sorted by name, with optional pagination.
func (b *InMemoryBackend) ListModels(nextToken string) ([]*Model, string) {
	b.mu.RLock("ListModels")
	defer b.mu.RUnlock()

	list := make([]*Model, 0, len(b.models))

	for _, m := range b.models {
		list = append(list, cloneModel(m))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ModelName < list[j].ModelName
	})

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*Model{}, ""
	}
	end := startIdx + sagemakerDefaultPageSize
	var outToken string
	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// DeleteModel deletes a model by name.
func (b *InMemoryBackend) DeleteModel(name string) error {
	b.mu.Lock("DeleteModel")
	defer b.mu.Unlock()

	m, ok := b.models[name]
	if !ok {
		return fmt.Errorf("%w: could not find model %q", ErrModelNotFound, name)
	}

	delete(b.modelARNIndex, m.ModelARN)
	delete(b.models, name)

	return nil
}

// SetModelExtras sets optional fields on an existing model that were not included
// in the original CreateModel signature (VpcConfig, EnableNetworkIsolation, InferenceExecutionConfig).
func (b *InMemoryBackend) SetModelExtras(
	name string,
	vpcConfig *VpcConfig,
	enableNetworkIsolation bool,
	inferenceExecConfig *InferenceExecutionConfig,
) error {
	b.mu.Lock("SetModelExtras")
	defer b.mu.Unlock()

	m, ok := b.models[name]
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
	name string,
	productionVariants []ProductionVariant,
	tags map[string]string,
) (*EndpointConfig, error) {
	b.mu.Lock("CreateEndpointConfig")
	defer b.mu.Unlock()

	if _, ok := b.endpointConfigs[name]; ok {
		return nil, fmt.Errorf(
			"%w: endpoint config %s already exists",
			ErrEndpointConfigAlreadyExists,
			name,
		)
	}

	configARN := arn.Build("sagemaker", b.region, b.accountID, "endpoint-config/"+name)

	storedVariants := make([]ProductionVariant, len(productionVariants))
	copy(storedVariants, productionVariants)

	ec := &EndpointConfig{
		EndpointConfigName: name,
		EndpointConfigARN:  configARN,
		ProductionVariants: storedVariants,
		CreationTime:       time.Now(),
		Tags:               mergeTags(nil, tags),
	}
	b.endpointConfigs[name] = ec
	b.endpointConfigARNIndex[configARN] = name

	return cloneEndpointConfig(ec), nil
}

// DescribeEndpointConfig returns an endpoint config by name.
func (b *InMemoryBackend) DescribeEndpointConfig(name string) (*EndpointConfig, error) {
	b.mu.RLock("DescribeEndpointConfig")
	defer b.mu.RUnlock()

	ec, ok := b.endpointConfigs[name]
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
func (b *InMemoryBackend) ListEndpointConfigs(nextToken string) ([]*EndpointConfig, string) {
	b.mu.RLock("ListEndpointConfigs")
	defer b.mu.RUnlock()

	list := make([]*EndpointConfig, 0, len(b.endpointConfigs))

	for _, ec := range b.endpointConfigs {
		list = append(list, cloneEndpointConfig(ec))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].EndpointConfigName < list[j].EndpointConfigName
	})

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*EndpointConfig{}, ""
	}
	end := startIdx + sagemakerDefaultPageSize
	var outToken string
	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// DeleteEndpointConfig deletes an endpoint configuration by name.
func (b *InMemoryBackend) DeleteEndpointConfig(name string) error {
	b.mu.Lock("DeleteEndpointConfig")
	defer b.mu.Unlock()

	ec, ok := b.endpointConfigs[name]
	if !ok {
		return fmt.Errorf(
			"%w: could not find endpoint configuration %q",
			ErrEndpointConfigNotFound,
			name,
		)
	}

	delete(b.endpointConfigARNIndex, ec.EndpointConfigARN)
	delete(b.endpointConfigs, name)

	return nil
}

// SetEndpointConfigExtras sets optional fields on an existing endpoint config that were not
// included in the original CreateEndpointConfig signature.
func (b *InMemoryBackend) SetEndpointConfigExtras(
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

	ec, ok := b.endpointConfigs[name]
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
func (b *InMemoryBackend) AddTags(resourceARN string, tags map[string]string) error {
	b.mu.Lock("AddTags")
	defer b.mu.Unlock()

	if name, ok := b.modelARNIndex[resourceARN]; ok {
		m := b.models[name]
		m.Tags = mergeTags(m.Tags, tags)

		return nil
	}

	if name, ok := b.endpointConfigARNIndex[resourceARN]; ok {
		ec := b.endpointConfigs[name]
		ec.Tags = mergeTags(ec.Tags, tags)

		return nil
	}

	if name, ok := b.actionARNIndex[resourceARN]; ok {
		a := b.actions[name]
		a.Tags = mergeTags(a.Tags, tags)

		return nil
	}

	if name, ok := b.algorithmARNIndex[resourceARN]; ok {
		al := b.algorithms[name]
		al.Tags = mergeTags(al.Tags, tags)

		return nil
	}

	if _, ok := b.modelPackageARNIndex[resourceARN]; ok {
		mp := b.modelPackages[resourceARN]
		mp.Tags = mergeTags(mp.Tags, tags)

		return nil
	}

	if name, ok := b.endpointARNIndex[resourceARN]; ok {
		ep := b.endpoints[name]
		ep.Tags = mergeTags(ep.Tags, tags)

		return nil
	}

	if name, ok := b.trainingJobARNIndex[resourceARN]; ok {
		tj := b.trainingJobs[name]
		tj.Tags = mergeTags(tj.Tags, tags)

		return nil
	}

	if name, ok := b.notebookARNIndex[resourceARN]; ok {
		nb := b.notebooks[name]
		nb.Tags = mergeTags(nb.Tags, tags)

		return nil
	}

	if name, ok := b.hpTuningJobARNIndex[resourceARN]; ok {
		j := b.hpTuningJobs[name]
		j.Tags = mergeTags(j.Tags, tags)

		return nil
	}

	if name, ok := b.processingJobARNIndex[resourceARN]; ok {
		if pj, found := b.processingJobs[name]; found {
			pj.Tags = mergeTags(pj.Tags, tags)

			return nil
		}
	}

	return fmt.Errorf("%w: resource %s not found", ErrValidation, resourceARN)
}

// ListTags returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	tagMap := b.findTagMapLocked(resourceARN)
	if tagMap == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrValidation, resourceARN)
	}

	result := make(map[string]string, len(*tagMap))
	maps.Copy(result, *tagMap)

	return result, nil
}

// findTagMapLocked returns a pointer to the tags map for a resource identified by ARN.
// Must be called with b.mu held. Returns nil if the resource is not found.
func (b *InMemoryBackend) findTagMapLocked(resourceARN string) *map[string]string {
	if name, ok := b.modelARNIndex[resourceARN]; ok {
		return &b.models[name].Tags
	}

	if name, ok := b.endpointConfigARNIndex[resourceARN]; ok {
		return &b.endpointConfigs[name].Tags
	}

	if name, ok := b.actionARNIndex[resourceARN]; ok {
		return &b.actions[name].Tags
	}

	if name, ok := b.algorithmARNIndex[resourceARN]; ok {
		return &b.algorithms[name].Tags
	}

	if _, ok := b.modelPackageARNIndex[resourceARN]; ok {
		return &b.modelPackages[resourceARN].Tags
	}

	if name, ok := b.endpointARNIndex[resourceARN]; ok {
		return &b.endpoints[name].Tags
	}

	if name, ok := b.trainingJobARNIndex[resourceARN]; ok {
		return &b.trainingJobs[name].Tags
	}

	if name, ok := b.notebookARNIndex[resourceARN]; ok {
		return &b.notebooks[name].Tags
	}

	if name, ok := b.hpTuningJobARNIndex[resourceARN]; ok {
		return &b.hpTuningJobs[name].Tags
	}

	if name, ok := b.processingJobARNIndex[resourceARN]; ok {
		if pj, found := b.processingJobs[name]; found {
			return &pj.Tags
		}
	}

	return nil
}

// DeleteTags removes tag keys from a resource identified by ARN.
func (b *InMemoryBackend) DeleteTags(resourceARN string, tagKeys []string) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	tags := b.findTagMapLocked(resourceARN)
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

	key := associationKey(sourceArn, destinationArn)
	if _, ok := b.associations[key]; ok {
		return nil, fmt.Errorf(
			"%w: association between %s and %s already exists",
			ErrAssociationAlreadyExists,
			sourceArn,
			destinationArn,
		)
	}

	assocARN := arn.Build(
		"sagemaker",
		b.region,
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
	b.associations[key] = a

	return cloneAssociation(a), nil
}

// AssociateTrialComponent associates a trial component with a trial.
func (b *InMemoryBackend) AssociateTrialComponent(
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

	key := trialComponentKey(trialName, trialComponentName)
	if _, ok := b.trialComponentAssociations[key]; ok {
		return nil, fmt.Errorf("%w: trial component %s is already associated with trial %s",
			ErrAssociationAlreadyExists, trialComponentName, trialName)
	}

	trialArn := arn.Build("sagemaker", b.region, b.accountID, "experiment-trial/"+trialName)
	componentArn := arn.Build(
		"sagemaker",
		b.region,
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
	b.trialComponentAssociations[key] = assoc

	return cloneTrialComponentAssociation(assoc), nil
}

// ensureClusterLocked looks up a cluster by name (must be called with lock held).
func (b *InMemoryBackend) ensureClusterLocked(clusterName string) (*Cluster, error) {
	c, ok := b.clusters[clusterName]
	if !ok {
		return nil, fmt.Errorf("%w: cluster %q not found", ErrClusterNotFound, clusterName)
	}

	return c, nil
}

// AddClusterInternal adds a cluster directly for seeding tests.
func (b *InMemoryBackend) AddClusterInternal(clusterName string) *Cluster {
	b.mu.Lock("AddClusterInternal")
	defer b.mu.Unlock()

	clusterARN := arn.Build("sagemaker", b.region, b.accountID, "cluster/"+clusterName)
	c := &Cluster{
		ClusterName:   clusterName,
		ClusterArn:    clusterARN,
		ClusterStatus: clusterStatusInService,
		Nodes:         make(map[string]*ClusterNode),
		CreationTime:  time.Now(),
	}
	b.clusters[clusterName] = c
	b.clusterARNIndex[clusterARN] = clusterName

	return cloneCluster(c)
}

// AddActionInternal adds an action directly for seeding tests.
func (b *InMemoryBackend) AddActionInternal(name, actionType string) *Action {
	b.mu.Lock("AddActionInternal")
	defer b.mu.Unlock()

	actionARN := arn.Build("sagemaker", b.region, b.accountID, "action/"+name)
	a := &Action{
		ActionName:   name,
		ActionArn:    actionARN,
		ActionType:   actionType,
		CreationTime: time.Now(),
		Tags:         make(map[string]string),
	}
	b.actions[name] = a
	b.actionARNIndex[actionARN] = name

	return cloneAction(a)
}

// AddAlgorithmInternal adds an algorithm directly for seeding tests.
func (b *InMemoryBackend) AddAlgorithmInternal(name string) *Algorithm {
	b.mu.Lock("AddAlgorithmInternal")
	defer b.mu.Unlock()

	algorithmARN := arn.Build("sagemaker", b.region, b.accountID, "algorithm/"+name)
	al := &Algorithm{
		AlgorithmName:   name,
		AlgorithmArn:    algorithmARN,
		AlgorithmStatus: algorithmStatusCompleted,
		CreationTime:    time.Now(),
		Tags:            make(map[string]string),
	}
	b.algorithms[name] = al
	b.algorithmARNIndex[algorithmARN] = al.AlgorithmName

	return cloneAlgorithm(al)
}

// AttachClusterNodeVolume attaches a volume to a cluster node.
func (b *InMemoryBackend) AttachClusterNodeVolume(
	clusterName, nodeID string,
	volume ClusterNodeVolume,
) (string, string, error) {
	b.mu.Lock("AttachClusterNodeVolume")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(clusterName)
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
	clusterName string,
	nodeConfigs []ClusterNode,
) (string, []string, error) {
	b.mu.Lock("BatchAddClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(clusterName)
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
	clusterName string,
	nodeIDs []string,
) (string, []string, []string, error) {
	b.mu.Lock("BatchDeleteClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(clusterName)
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
	modelPackageArns []string,
) map[string]ModelPackageBatchResult {
	b.mu.RLock("BatchDescribeModelPackage")
	defer b.mu.RUnlock()

	results := make(map[string]ModelPackageBatchResult, len(modelPackageArns))

	for _, arnStr := range modelPackageArns {
		mp, ok := b.modelPackages[arnStr]
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
	clusterName string,
	nodeIDs []string,
) (string, []string, []string, error) {
	b.mu.Lock("BatchRebootClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(clusterName)
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
	clusterName string,
	nodes []ClusterNode,
) (string, []string, error) {
	b.mu.Lock("BatchReplaceClusterNodes")
	defer b.mu.Unlock()

	c, err := b.ensureClusterLocked(clusterName)
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

	if _, ok := b.actions[name]; ok {
		return nil, fmt.Errorf("%w: action %q already exists", ErrActionAlreadyExists, name)
	}

	actionARN := arn.Build("sagemaker", b.region, b.accountID, "action/"+name)

	a := &Action{
		ActionName:   name,
		ActionArn:    actionARN,
		ActionType:   actionType,
		Description:  description,
		Status:       status,
		Source:       source,
		Properties:   maps.Clone(properties),
		Tags:         mergeTags(nil, tags),
		CreationTime: time.Now(),
	}
	b.actions[name] = a
	b.actionARNIndex[actionARN] = name

	return cloneAction(a), nil
}

// CreateAlgorithm creates a SageMaker algorithm specification.
func (b *InMemoryBackend) CreateAlgorithm(
	name, description string,
	tags map[string]string,
) (*Algorithm, error) {
	b.mu.Lock("CreateAlgorithm")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: AlgorithmName is required", ErrValidation)
	}

	if _, ok := b.algorithms[name]; ok {
		return nil, fmt.Errorf("%w: algorithm %q already exists", ErrAlgorithmAlreadyExists, name)
	}

	algorithmARN := arn.Build("sagemaker", b.region, b.accountID, "algorithm/"+name)

	al := &Algorithm{
		AlgorithmName:        name,
		AlgorithmArn:         algorithmARN,
		AlgorithmDescription: description,
		AlgorithmStatus:      algorithmStatusCompleted,
		Tags:                 mergeTags(nil, tags),
		CreationTime:         time.Now(),
	}
	b.algorithms[name] = al
	b.algorithmARNIndex[algorithmARN] = name

	return cloneAlgorithm(al), nil
}

// AddModelPackageInternal adds a model package directly for testing.
func (b *InMemoryBackend) AddModelPackageInternal(mp *ModelPackage) {
	b.mu.Lock("AddModelPackageInternal")
	defer b.mu.Unlock()

	b.modelPackages[mp.ModelPackageArn] = mp
	b.modelPackageARNIndex[mp.ModelPackageArn] = mp.ModelPackageArn
}

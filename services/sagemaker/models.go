package sagemaker

import (
	"encoding/json"
	"maps"
	"time"
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
//
// ExplainerConfig/MetricsConfig are carried as opaque json.RawMessage
// passthrough rather than fully-typed structs, the same convention as
// ModelPackage's deep union/config fields — every field a client actually
// sends round-trips exactly.
type EndpointConfig struct {
	CreationTime             time.Time             `json:"CreationTime"`
	Tags                     map[string]string     `json:"Tags,omitempty"`
	VpcConfig                *VpcConfig            `json:"VpcConfig,omitempty"`
	DataCaptureConfig        *DataCaptureConfig    `json:"DataCaptureConfig,omitempty"`
	AsyncInferenceConfig     *AsyncInferenceConfig `json:"AsyncInferenceConfig,omitempty"`
	ExplainerConfig          json.RawMessage       `json:"ExplainerConfig,omitempty"`
	MetricsConfig            json.RawMessage       `json:"MetricsConfig,omitempty"`
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
	cp.ExplainerConfig = append(json.RawMessage(nil), ec.ExplainerConfig...)
	cp.MetricsConfig = append(json.RawMessage(nil), ec.MetricsConfig...)

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
	CreationTime       time.Time           `json:"CreationTime"`
	LastModifiedTime   time.Time           `json:"LastModifiedTime"`
	Tags               map[string]string   `json:"Tags,omitempty"`
	Properties         map[string]string   `json:"Properties,omitempty"`
	MetadataProperties *MetadataProperties `json:"MetadataProperties,omitempty"`
	Source             ActionSource        `json:"Source"`
	ActionName         string              `json:"ActionName"`
	ActionArn          string              `json:"ActionArn"`
	ActionType         string              `json:"ActionType"`
	Description        string              `json:"Description,omitempty"`
	Status             string              `json:"Status,omitempty"`
}

// cloneAction returns a deep copy of a.
func cloneAction(a *Action) *Action {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)
	cp.Properties = maps.Clone(a.Properties)

	if a.MetadataProperties != nil {
		mp := *a.MetadataProperties
		cp.MetadataProperties = &mp
	}

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

	if c.VpcConfig != nil {
		vc := *c.VpcConfig
		vc.SecurityGroupIDs = append([]string(nil), c.VpcConfig.SecurityGroupIDs...)
		vc.Subnets = append([]string(nil), c.VpcConfig.Subnets...)
		cp.VpcConfig = &vc
	}

	if c.AutoScaling != nil {
		as := *c.AutoScaling
		cp.AutoScaling = &as
	}

	if c.Orchestrator != nil {
		o := *c.Orchestrator
		if c.Orchestrator.Eks != nil {
			eks := *c.Orchestrator.Eks
			o.Eks = &eks
		}
		if c.Orchestrator.Slurm != nil {
			slurm := *c.Orchestrator.Slurm
			o.Slurm = &slurm
		}
		cp.Orchestrator = &o
	}

	if c.TieredStorageConfig != nil {
		tsc := *c.TieredStorageConfig
		cp.TieredStorageConfig = &tsc
	}

	cp.RestrictedInstanceGroups = make([]ClusterRestrictedInstanceGroup, len(c.RestrictedInstanceGroups))
	for i, ig := range c.RestrictedInstanceGroups {
		cp.RestrictedInstanceGroups[i] = cloneClusterRestrictedInstanceGroup(ig)
	}

	cp.RestrictedInstanceGroupsConfig = cloneRestrictedInstanceGroupsConfig(c.RestrictedInstanceGroupsConfig)

	return &cp
}

// cloneClusterEnvironmentConfig returns a deep copy of e.
func cloneClusterEnvironmentConfig(e *ClusterEnvironmentConfig) *ClusterEnvironmentConfig {
	if e == nil {
		return nil
	}

	cp := *e
	if e.FSxLustreConfig != nil {
		flc := *e.FSxLustreConfig
		cp.FSxLustreConfig = &flc
	}

	return &cp
}

// cloneClusterInstanceStorageConfig returns a deep copy of s.
func cloneClusterInstanceStorageConfig(s ClusterInstanceStorageConfig) ClusterInstanceStorageConfig {
	if s.EbsVolumeConfig != nil {
		v := *s.EbsVolumeConfig
		s.EbsVolumeConfig = &v
	}

	if s.FsxLustreConfig != nil {
		v := *s.FsxLustreConfig
		s.FsxLustreConfig = &v
	}

	if s.FsxOpenZfsConfig != nil {
		v := *s.FsxOpenZfsConfig
		s.FsxOpenZfsConfig = &v
	}

	return s
}

// cloneScheduledUpdateConfig returns a deep copy of s.
func cloneScheduledUpdateConfig(s *ScheduledUpdateConfig) *ScheduledUpdateConfig {
	if s == nil {
		return nil
	}

	cp := *s
	cp.DeploymentConfig = cloneDeploymentConfiguration(s.DeploymentConfig)

	return &cp
}

// cloneDeploymentConfiguration returns a deep copy of dc.
func cloneDeploymentConfiguration(dc *DeploymentConfiguration) *DeploymentConfiguration {
	if dc == nil {
		return nil
	}

	cp := *dc
	cp.AutoRollbackConfiguration = append([]AlarmDetails(nil), dc.AutoRollbackConfiguration...)
	cp.RollingUpdatePolicy = cloneRollingDeploymentPolicy(dc.RollingUpdatePolicy)

	return &cp
}

// cloneRollingDeploymentPolicy returns a deep copy of rp.
func cloneRollingDeploymentPolicy(rp *RollingDeploymentPolicy) *RollingDeploymentPolicy {
	if rp == nil {
		return nil
	}

	cp := *rp

	if rp.MaximumBatchSize != nil {
		v := *rp.MaximumBatchSize
		cp.MaximumBatchSize = &v
	}

	if rp.RollbackMaximumBatchSize != nil {
		v := *rp.RollbackMaximumBatchSize
		cp.RollbackMaximumBatchSize = &v
	}

	return &cp
}

// cloneClusterRestrictedInstanceGroup returns a deep copy of ig.
func cloneClusterRestrictedInstanceGroup(ig ClusterRestrictedInstanceGroup) ClusterRestrictedInstanceGroup {
	ig.EnvironmentConfig = cloneClusterEnvironmentConfig(ig.EnvironmentConfig)
	ig.ScheduledUpdateConfig = cloneScheduledUpdateConfig(ig.ScheduledUpdateConfig)

	if ig.InstanceStorageConfigs != nil {
		cp := make([]ClusterInstanceStorageConfig, len(ig.InstanceStorageConfigs))
		for i, s := range ig.InstanceStorageConfigs {
			cp[i] = cloneClusterInstanceStorageConfig(s)
		}

		ig.InstanceStorageConfigs = cp
	}

	return ig
}

// cloneRestrictedInstanceGroupsConfig returns a deep copy of cfg.
func cloneRestrictedInstanceGroupsConfig(
	cfg *ClusterRestrictedInstanceGroupsConfig,
) *ClusterRestrictedInstanceGroupsConfig {
	if cfg == nil {
		return nil
	}

	cp := *cfg

	if cfg.SharedEnvironmentConfig != nil {
		sec := *cfg.SharedEnvironmentConfig
		if cfg.SharedEnvironmentConfig.FSxLustreConfig != nil {
			flc := *cfg.SharedEnvironmentConfig.FSxLustreConfig
			sec.FSxLustreConfig = &flc
		}

		cp.SharedEnvironmentConfig = &sec
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
	CreationTime      time.Time           `json:"CreationTime"`
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

// ClusterAutoScalingConfig mirrors types.ClusterAutoScalingConfig
// (api_op_CreateCluster.go:41-43, types/types.go:4492, sagemaker@v1.263.2).
// Status is not stored: it is synthesized as InService on read, the same
// convention this service already uses for instance-group status
// (instanceGroupStatusInService), since this emulator does not model
// asynchronous autoscaler provisioning.
type ClusterAutoScalingConfig struct {
	Mode           string `json:"Mode"`
	AutoScalerType string `json:"AutoScalerType,omitempty"`
}

// ClusterOrchestratorEksConfig mirrors types.ClusterOrchestratorEksConfig
// (types/types.go:5470, sagemaker@v1.263.2).
type ClusterOrchestratorEksConfig struct {
	ClusterArn string `json:"ClusterArn"`
}

// ClusterOrchestratorSlurmConfig mirrors types.ClusterOrchestratorSlurmConfig
// (types/types.go:5483, sagemaker@v1.263.2).
type ClusterOrchestratorSlurmConfig struct {
	SlurmConfigStrategy string `json:"SlurmConfigStrategy,omitempty"`
}

// ClusterOrchestrator mirrors types.ClusterOrchestrator (types/types.go:5456,
// sagemaker@v1.263.2). Despite AWS's docs stating "you must provide exactly
// one orchestrator configuration: either Eks or Slurm", botocore's
// sagemaker/2017-07-24 service-2.json models ClusterOrchestrator as a plain
// "structure" (not "union"), and serializers.go:27593-27612 emit Eks/Slurm as
// two independent optional keys — so this is a struct with a
// runtime-validated business rule, not a discriminated wire union.
type ClusterOrchestrator struct {
	Eks   *ClusterOrchestratorEksConfig   `json:"Eks,omitempty"`
	Slurm *ClusterOrchestratorSlurmConfig `json:"Slurm,omitempty"`
}

// ClusterTieredStorageConfig mirrors types.ClusterTieredStorageConfig
// (types/types.go:5847, sagemaker@v1.263.2).
type ClusterTieredStorageConfig struct {
	Mode                               string `json:"Mode"`
	InstanceMemoryAllocationPercentage int32  `json:"InstanceMemoryAllocationPercentage,omitempty"`
}

// FSxLustreConfig mirrors types.FSxLustreConfig (types/types.go:9152,
// sagemaker@v1.263.2).
type FSxLustreConfig struct {
	PerUnitStorageThroughput int32 `json:"PerUnitStorageThroughput,omitempty"`
	SizeInGiB                int32 `json:"SizeInGiB,omitempty"`
}

// ClusterEnvironmentConfig mirrors types.EnvironmentConfig (types/types.go:
// 8395, sagemaker@v1.263.2) -- the request-side shape of a restricted
// instance group's environment configuration.
type ClusterEnvironmentConfig struct {
	FSxLustreConfig *FSxLustreConfig `json:"FSxLustreConfig,omitempty"`
}

// ClusterEnvironmentConfigDetails mirrors types.EnvironmentConfigDetails
// (types/types.go:8405, sagemaker@v1.263.2) -- the response-side shape.
// S3OutputPath is a response-only field this emulator has no data source for
// (no restricted-instance-group output-location tracking exists) -- left
// unset, a disclosed no-op, rather than fabricated.
type ClusterEnvironmentConfigDetails struct {
	FSxLustreConfig *FSxLustreConfig `json:"FSxLustreConfig,omitempty"`
	S3OutputPath    string           `json:"S3OutputPath,omitempty"`
}

// ClusterEbsVolumeConfig mirrors types.ClusterEbsVolumeConfig (types/types.go:
// 4548, sagemaker@v1.263.2).
type ClusterEbsVolumeConfig struct {
	VolumeKmsKeyID string `json:"VolumeKmsKeyId,omitempty"`
	VolumeSizeInGB int32  `json:"VolumeSizeInGB,omitempty"`
	RootVolume     bool   `json:"RootVolume,omitempty"`
}

// ClusterFsxLustreConfig mirrors types.ClusterFsxLustreConfig (types/types.go:
// 4683, sagemaker@v1.263.2).
type ClusterFsxLustreConfig struct {
	DNSName   string `json:"DnsName"`
	MountName string `json:"MountName"`
	MountPath string `json:"MountPath,omitempty"`
}

// ClusterFsxOpenZfsConfig mirrors types.ClusterFsxOpenZfsConfig
// (types/types.go:4704, sagemaker@v1.263.2).
type ClusterFsxOpenZfsConfig struct {
	DNSName   string `json:"DnsName"`
	MountPath string `json:"MountPath,omitempty"`
}

// ClusterInstanceStorageConfig mirrors types.ClusterInstanceStorageConfig
// (types/types.go:5107, sagemaker@v1.263.2): a genuine discriminated union,
// confirmed by its isClusterInstanceStorageConfig() marker interface with
// three member wrapper types -- unlike ClusterOrchestrator in this same
// service, which reads like a union in prose but is a plain struct.
// serializers.go:27404-27428 confirms each member serializes as a single-key
// object ("EbsVolumeConfig"/"FsxLustreConfig"/"FsxOpenZfsConfig"). Modeled
// the way this repo already models real smithy unions (bedrock's
// EvaluationConfig, services/bedrock/models.go:271-274): one pointer field
// per member, tagged with its wire key, dispatched by encoding/json's own
// field-presence matching rather than a Go interface -- mirroring
// smithy-go's single-key-object mechanism directly.
// validateClusterInstanceStorageConfigLocked enforces exactly one member set.
type ClusterInstanceStorageConfig struct {
	EbsVolumeConfig  *ClusterEbsVolumeConfig  `json:"EbsVolumeConfig,omitempty"`
	FsxLustreConfig  *ClusterFsxLustreConfig  `json:"FsxLustreConfig,omitempty"`
	FsxOpenZfsConfig *ClusterFsxOpenZfsConfig `json:"FsxOpenZfsConfig,omitempty"`
}

// CapacitySizeConfig mirrors types.CapacitySizeConfig (types/types.go:3824,
// sagemaker@v1.263.2).
type CapacitySizeConfig struct {
	Type  string `json:"Type"`
	Value int32  `json:"Value"`
}

// AlarmDetails mirrors types.AlarmDetails (types/types.go:841,
// sagemaker@v1.263.2).
type AlarmDetails struct {
	AlarmName string `json:"AlarmName"`
}

// RollingDeploymentPolicy mirrors types.RollingDeploymentPolicy
// (types/types.go:20006, sagemaker@v1.263.2).
type RollingDeploymentPolicy struct {
	MaximumBatchSize         *CapacitySizeConfig `json:"MaximumBatchSize,omitempty"`
	RollbackMaximumBatchSize *CapacitySizeConfig `json:"RollbackMaximumBatchSize,omitempty"`
}

// DeploymentConfiguration mirrors types.DeploymentConfiguration
// (types/types.go:7106, sagemaker@v1.263.2).
type DeploymentConfiguration struct {
	RollingUpdatePolicy       *RollingDeploymentPolicy `json:"RollingUpdatePolicy,omitempty"`
	AutoRollbackConfiguration []AlarmDetails           `json:"AutoRollbackConfiguration,omitempty"`
	WaitIntervalInSeconds     int32                    `json:"WaitIntervalInSeconds,omitempty"`
}

// ScheduledUpdateConfig mirrors types.ScheduledUpdateConfig (types/types.go:
// 20564, sagemaker@v1.263.2). It is the same Go SDK type on both the
// restricted instance group's request shape and its ClusterRestrictedInstanceGroupDetails
// response shape (types/types.go:5550), so no separate Details variant is
// needed here, mirroring Orchestrator/TieredStorageConfig's own
// request/response type sharing (see ClusterOrchestrator's doc comment).
type ScheduledUpdateConfig struct {
	DeploymentConfig   *DeploymentConfiguration `json:"DeploymentConfig,omitempty"`
	ScheduleExpression string                   `json:"ScheduleExpression"`
}

// ClusterRestrictedInstanceGroup represents a restricted instance group
// specification/details for a SageMaker HyperPod cluster (a merged view of
// the AWS ClusterRestrictedInstanceGroupSpecification and
// ClusterRestrictedInstanceGroupDetails shapes, types/types.go:5514,:5622 --
// the same convention ClusterInstanceGroup already uses for regular instance
// groups). OnStartDeepHealthChecks/OverrideVpcConfig/ThreadsPerCore/
// TrainingPlanArn are not modeled, consistent with ClusterInstanceGroup
// already omitting those same fields for regular instance groups.
type ClusterRestrictedInstanceGroup struct {
	EnvironmentConfig      *ClusterEnvironmentConfig      `json:"EnvironmentConfig,omitempty"`
	ScheduledUpdateConfig  *ScheduledUpdateConfig         `json:"ScheduledUpdateConfig,omitempty"`
	InstanceGroupName      string                         `json:"InstanceGroupName"`
	ExecutionRole          string                         `json:"ExecutionRole,omitempty"`
	InstanceType           string                         `json:"InstanceType,omitempty"`
	InstanceStorageConfigs []ClusterInstanceStorageConfig `json:"InstanceStorageConfigs,omitempty"`
	InstanceCount          int32                          `json:"InstanceCount,omitempty"`
}

// ClusterSharedEnvironmentConfig mirrors types.ClusterSharedEnvironmentConfig
// (types/types.go:5727, sagemaker@v1.263.2). FSxLustreConfig and
// FSxLustreDeletionPolicy are both "This member is required" on the real
// type -- validateRestrictedInstanceGroupsConfigLocked enforces their
// presence. FSxLustreDeletionPolicy is stored/echoed as an opaque string,
// the same not-validated-against-the-enum convention NodeProvisioningMode
// already uses.
type ClusterSharedEnvironmentConfig struct {
	FSxLustreConfig         *FSxLustreConfig `json:"FSxLustreConfig,omitempty"`
	FSxLustreDeletionPolicy string           `json:"FSxLustreDeletionPolicy,omitempty"`
}

// ClusterRestrictedInstanceGroupsConfig mirrors
// types.ClusterRestrictedInstanceGroupsConfig (types/types.go:5598,
// sagemaker@v1.263.2). SharedEnvironmentConfig is "This member is required"
// on the real type.
type ClusterRestrictedInstanceGroupsConfig struct {
	SharedEnvironmentConfig *ClusterSharedEnvironmentConfig `json:"SharedEnvironmentConfig,omitempty"`
}

// Cluster represents a SageMaker HyperPod cluster.
type Cluster struct {
	CreationTime                   time.Time                              `json:"CreationTime"`
	Nodes                          map[string]*ClusterNode                `json:"-"`
	Tags                           map[string]string                      `json:"Tags,omitempty"`
	VpcConfig                      *VpcConfig                             `json:"VpcConfig,omitempty"`
	AutoScaling                    *ClusterAutoScalingConfig              `json:"AutoScaling,omitempty"`
	Orchestrator                   *ClusterOrchestrator                   `json:"Orchestrator,omitempty"`
	TieredStorageConfig            *ClusterTieredStorageConfig            `json:"TieredStorageConfig,omitempty"`
	RestrictedInstanceGroupsConfig *ClusterRestrictedInstanceGroupsConfig `json:"RestrictedInstanceGroupsConfig,omitempty"`
	ClusterArn                     string                                 `json:"ClusterArn"`
	ClusterName                    string                                 `json:"ClusterName"`
	ClusterStatus                  string                                 `json:"ClusterStatus"`
	NodeRecovery                   string                                 `json:"NodeRecovery,omitempty"`
	ClusterRole                    string                                 `json:"ClusterRole,omitempty"`
	NodeProvisioningMode           string                                 `json:"NodeProvisioningMode,omitempty"`
	InstanceGroups                 []ClusterInstanceGroup                 `json:"InstanceGroups,omitempty"`
	RestrictedInstanceGroups       []ClusterRestrictedInstanceGroup       `json:"RestrictedInstanceGroups,omitempty"`
}

// ModelPackageStatusItem mirrors AWS's ModelPackageStatusItem: the outcome of
// one validation or image-scan check run against a model package.
type ModelPackageStatusItem struct {
	Name          string `json:"Name"`
	Status        string `json:"Status"`
	FailureReason string `json:"FailureReason,omitempty"`
}

// ModelPackageStatusDetails mirrors AWS's ModelPackageStatusDetails: the
// overall validation/scan status of a model package. ValidationStatuses is
// "This member is required" in the real DescribeModelPackageOutput — it must
// always be emitted (as an empty list when no validation profiles ran),
// never omitted.
type ModelPackageStatusDetails struct {
	ValidationStatuses []ModelPackageStatusItem `json:"ValidationStatuses"`
	ImageScanStatuses  []ModelPackageStatusItem `json:"ImageScanStatuses,omitempty"`
}

// ModelPackage represents a SageMaker model package.
//
// InferenceSpecification/SourceAlgorithmSpecification/ValidationSpecification/
// DriftCheckBaselines/ModelMetrics/ModelCard/ModelLifeCycle/MetadataProperties/
// SecurityConfig/AdditionalInferenceSpecifications are carried as opaque
// json.RawMessage passthrough rather than fully-typed structs — same
// convention as ai_workload_configs.go's WorkloadSpec/DatasetConfig — since
// these are deeply-nested union/config shapes out of this pass's budget;
// every field a client actually sends round-trips exactly.
type ModelPackage struct {
	CreationTime                      time.Time                 `json:"CreationTime"`
	LastModifiedTime                  time.Time                 `json:"LastModifiedTime,omitzero"`
	Tags                              map[string]string         `json:"Tags,omitempty"`
	CustomerMetadataProperties        map[string]string         `json:"CustomerMetadataProperties,omitempty"`
	InferenceSpecification            json.RawMessage           `json:"InferenceSpecification,omitempty"`
	SourceAlgorithmSpecification      json.RawMessage           `json:"SourceAlgorithmSpecification,omitempty"`
	ValidationSpecification           json.RawMessage           `json:"ValidationSpecification,omitempty"`
	DriftCheckBaselines               json.RawMessage           `json:"DriftCheckBaselines,omitempty"`
	ModelMetrics                      json.RawMessage           `json:"ModelMetrics,omitempty"`
	ModelCard                         json.RawMessage           `json:"ModelCard,omitempty"`
	ModelLifeCycle                    json.RawMessage           `json:"ModelLifeCycle,omitempty"`
	MetadataProperties                json.RawMessage           `json:"MetadataProperties,omitempty"`
	SecurityConfig                    json.RawMessage           `json:"SecurityConfig,omitempty"`
	AdditionalInferenceSpecifications json.RawMessage           `json:"AdditionalInferenceSpecifications,omitempty"`
	ModelPackageName                  string                    `json:"ModelPackageName"`
	ModelPackageArn                   string                    `json:"ModelPackageArn"`
	ModelPackageGroupName             string                    `json:"ModelPackageGroupName,omitempty"`
	ModelPackageStatus                string                    `json:"ModelPackageStatus"`
	ModelApprovalStatus               string                    `json:"ModelApprovalStatus,omitempty"`
	ApprovalDescription               string                    `json:"ApprovalDescription,omitempty"`
	ModelPackageDescription           string                    `json:"ModelPackageDescription,omitempty"`
	Domain                            string                    `json:"Domain,omitempty"`
	ManagedStorageType                string                    `json:"ManagedStorageType,omitempty"`
	ModelPackageRegistrationType      string                    `json:"ModelPackageRegistrationType,omitempty"`
	SamplePayloadURL                  string                    `json:"SamplePayloadUrl,omitempty"`
	SkipModelValidation               string                    `json:"SkipModelValidation,omitempty"`
	SourceURI                         string                    `json:"SourceUri,omitempty"`
	Task                              string                    `json:"Task,omitempty"`
	ModelPackageStatusDetails         ModelPackageStatusDetails `json:"ModelPackageStatusDetails"`
	CertifyForMarketplace             bool                      `json:"CertifyForMarketplace,omitempty"`
}

// cloneModelPackage returns a deep copy of mp.
func cloneModelPackage(mp *ModelPackage) *ModelPackage {
	cp := *mp
	cp.Tags = maps.Clone(mp.Tags)
	cp.CustomerMetadataProperties = maps.Clone(mp.CustomerMetadataProperties)
	cp.ModelPackageStatusDetails.ValidationStatuses = append(
		[]ModelPackageStatusItem{}, mp.ModelPackageStatusDetails.ValidationStatuses...,
	)
	cp.ModelPackageStatusDetails.ImageScanStatuses = append(
		[]ModelPackageStatusItem{}, mp.ModelPackageStatusDetails.ImageScanStatuses...,
	)
	cp.InferenceSpecification = append(json.RawMessage(nil), mp.InferenceSpecification...)
	cp.SourceAlgorithmSpecification = append(json.RawMessage(nil), mp.SourceAlgorithmSpecification...)
	cp.ValidationSpecification = append(json.RawMessage(nil), mp.ValidationSpecification...)
	cp.DriftCheckBaselines = append(json.RawMessage(nil), mp.DriftCheckBaselines...)
	cp.ModelMetrics = append(json.RawMessage(nil), mp.ModelMetrics...)
	cp.ModelCard = append(json.RawMessage(nil), mp.ModelCard...)
	cp.ModelLifeCycle = append(json.RawMessage(nil), mp.ModelLifeCycle...)
	cp.MetadataProperties = append(json.RawMessage(nil), mp.MetadataProperties...)
	cp.SecurityConfig = append(json.RawMessage(nil), mp.SecurityConfig...)
	cp.AdditionalInferenceSpecifications = append(json.RawMessage(nil), mp.AdditionalInferenceSpecifications...)

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeModelPackage.
func (mp *ModelPackage) MarshalJSON() ([]byte, error) {
	type alias ModelPackage

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime,omitempty"`
	}{
		alias:        (*alias)(mp),
		CreationTime: epochSeconds(mp.CreationTime),
	}
	if !mp.LastModifiedTime.IsZero() {
		aux.LastModifiedTime = epochSeconds(mp.LastModifiedTime)
	}

	return json.Marshal(aux)
}

// UnmarshalJSON is the inverse of [ModelPackage.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (mp *ModelPackage) UnmarshalJSON(data []byte) error {
	type alias ModelPackage

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(mp)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	mp.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	if aux.LastModifiedTime != 0 {
		mp.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)
	}

	return nil
}

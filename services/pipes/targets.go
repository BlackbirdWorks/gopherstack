package pipes

import (
	"maps"
)

// AwsVpcConfiguration is the VPC network configuration for ECS tasks.
type AwsVpcConfiguration struct {
	AssignPublicIP string   `json:"AssignPublicIp,omitempty"`
	Subnets        []string `json:"Subnets,omitempty"`
	SecurityGroups []string `json:"SecurityGroups,omitempty"`
}

// NetworkConfiguration wraps VPC configuration for ECS task targets.
type NetworkConfiguration struct {
	AwsvpcConfiguration *AwsVpcConfiguration `json:"AwsvpcConfiguration,omitempty"`
}

// CapacityProviderStrategyItem is a single entry in an ECS capacity provider strategy.
type CapacityProviderStrategyItem struct {
	CapacityProvider string `json:"CapacityProvider,omitempty"`
	Weight           int    `json:"Weight,omitempty"`
	Base             int    `json:"Base,omitempty"`
}

// PlacementConstraint is a constraint for ECS task placement.
type PlacementConstraint struct {
	Expression string `json:"Expression,omitempty"`
	Type       string `json:"Type,omitempty"`
}

// PlacementStrategy is a placement strategy rule for ECS tasks.
type PlacementStrategy struct {
	Field string `json:"Field,omitempty"`
	Type  string `json:"Type,omitempty"`
}

// EcsEnvironmentVariable is a name/value pair overriding an ECS container's environment.
type EcsEnvironmentVariable struct {
	Name  string `json:"Name,omitempty"`
	Value string `json:"Value,omitempty"`
}

// EcsEnvironmentFile references an S3 object containing environment variables for an ECS container.
type EcsEnvironmentFile struct {
	Type  string `json:"Type,omitempty"`
	Value string `json:"Value,omitempty"`
}

// EcsResourceRequirement is a resource type/value pair for an ECS container override.
type EcsResourceRequirement struct {
	Type  string `json:"Type,omitempty"`
	Value string `json:"Value,omitempty"`
}

// EcsContainerOverride holds per-container override values for an ECS task execution.
type EcsContainerOverride struct {
	CPU                  *int                     `json:"Cpu,omitempty"`
	Memory               *int                     `json:"Memory,omitempty"`
	MemoryReservation    *int                     `json:"MemoryReservation,omitempty"`
	Name                 string                   `json:"Name,omitempty"`
	Command              []string                 `json:"Command,omitempty"`
	Environment          []EcsEnvironmentVariable `json:"Environment,omitempty"`
	EnvironmentFiles     []EcsEnvironmentFile     `json:"EnvironmentFiles,omitempty"`
	ResourceRequirements []EcsResourceRequirement `json:"ResourceRequirements,omitempty"`
}

// EcsEphemeralStorage overrides the ephemeral storage size for an ECS task.
type EcsEphemeralStorage struct {
	SizeInGiB int `json:"SizeInGiB,omitempty"`
}

// EcsInferenceAcceleratorOverride overrides an Elastic Inference accelerator for an ECS task.
type EcsInferenceAcceleratorOverride struct {
	DeviceName string `json:"DeviceName,omitempty"`
	DeviceType string `json:"DeviceType,omitempty"`
}

// EcsTaskOverride holds override values for an ECS task execution.
type EcsTaskOverride struct {
	EphemeralStorage              *EcsEphemeralStorage              `json:"EphemeralStorage,omitempty"`
	TaskRoleArn                   string                            `json:"TaskRoleArn,omitempty"`
	ExecutionRoleArn              string                            `json:"ExecutionRoleArn,omitempty"`
	CPU                           string                            `json:"Cpu,omitempty"`
	Memory                        string                            `json:"Memory,omitempty"`
	ContainerOverrides            []EcsContainerOverride            `json:"ContainerOverrides,omitempty"`
	InferenceAcceleratorOverrides []EcsInferenceAcceleratorOverride `json:"InferenceAcceleratorOverrides,omitempty"`
}

// BatchJobDependency represents a dependency between Batch jobs.
type BatchJobDependency struct {
	JobID string `json:"JobId,omitempty"`
	Type  string `json:"Type,omitempty"`
}

// BatchEnvironmentVariable is a name/value pair overriding a Batch container's environment.
type BatchEnvironmentVariable struct {
	Name  string `json:"Name,omitempty"`
	Value string `json:"Value,omitempty"`
}

// BatchResourceRequirement is a resource type/value pair for a Batch container override.
type BatchResourceRequirement struct {
	Type  string `json:"Type,omitempty"`
	Value string `json:"Value,omitempty"`
}

// BatchContainerOverrides holds container override values for a Batch job.
type BatchContainerOverrides struct {
	Environment          []BatchEnvironmentVariable `json:"Environment,omitempty"`
	InstanceType         string                     `json:"InstanceType,omitempty"`
	Command              []string                   `json:"Command,omitempty"`
	ResourceRequirements []BatchResourceRequirement `json:"ResourceRequirements,omitempty"`
}

// LambdaFunctionParameters holds Lambda-specific target configuration.
type LambdaFunctionParameters struct {
	InvocationType string `json:"InvocationType,omitempty"`
}

// StepFunctionTargetParameters holds Step Functions target configuration.
type StepFunctionTargetParameters struct {
	InvocationType string `json:"InvocationType,omitempty"`
}

// SQSTargetParameters holds SQS-specific target configuration.
type SQSTargetParameters struct {
	MessageGroupID         string `json:"MessageGroupId,omitempty"`
	MessageDeduplicationID string `json:"MessageDeduplicationId,omitempty"`
}

// KinesisStreamTargetParameters holds Kinesis stream target configuration.
type KinesisStreamTargetParameters struct {
	PartitionKey string `json:"PartitionKey,omitempty"`
}

// CloudWatchLogsTargetParameters holds CloudWatch Logs target configuration.
type CloudWatchLogsTargetParameters struct {
	LogStreamName string `json:"LogStreamName,omitempty"`
	Timestamp     string `json:"Timestamp,omitempty"`
}

// EBEventBusTargetParameters holds EventBridge event bus target configuration.
type EBEventBusTargetParameters struct {
	DetailType string   `json:"DetailType,omitempty"`
	EndpointID string   `json:"EndpointId,omitempty"`
	Source     string   `json:"Source,omitempty"`
	Time       string   `json:"Time,omitempty"`
	Resources  []string `json:"Resources,omitempty"`
}

// RedshiftDataTargetParameters holds Redshift Data API target configuration.
type RedshiftDataTargetParameters struct {
	Database         string   `json:"Database,omitempty"`
	DBUser           string   `json:"DbUser,omitempty"`
	SecretManagerArn string   `json:"SecretManagerArn,omitempty"`
	StatementName    string   `json:"StatementName,omitempty"`
	Sqls             []string `json:"Sqls,omitempty"`
	WithEvent        bool     `json:"WithEvent,omitempty"`
}

// SageMakerPipelineParameter is a name/value pair for a SageMaker pipeline.
type SageMakerPipelineParameter struct {
	Name  string `json:"Name,omitempty"`
	Value string `json:"Value,omitempty"`
}

// SageMakerPipelineTargetParameters holds SageMaker pipeline target configuration.
type SageMakerPipelineTargetParameters struct {
	PipelineParameterList []SageMakerPipelineParameter `json:"PipelineParameterList,omitempty"`
}

// BatchArrayProperties holds Batch array job properties.
type BatchArrayProperties struct {
	Size int `json:"Size,omitempty"`
}

// BatchRetryStrategy holds Batch retry configuration.
type BatchRetryStrategy struct {
	Attempts int `json:"Attempts,omitempty"`
}

// BatchJobTargetParameters holds Batch job target configuration.
type BatchJobTargetParameters struct {
	ArrayProperties    *BatchArrayProperties    `json:"ArrayProperties,omitempty"`
	RetryStrategy      *BatchRetryStrategy      `json:"RetryStrategy,omitempty"`
	ContainerOverrides *BatchContainerOverrides `json:"ContainerOverrides,omitempty"`
	Parameters         map[string]string        `json:"Parameters,omitempty"`
	JobDefinition      string                   `json:"JobDefinition,omitempty"`
	JobName            string                   `json:"JobName,omitempty"`
	DependsOn          []BatchJobDependency     `json:"DependsOn,omitempty"`
}

// EcsTag is a key/value tag applied to an ECS RunTask call.
type EcsTag struct {
	Key   string `json:"Key,omitempty"`
	Value string `json:"Value,omitempty"`
}

// ECSTaskTargetParameters holds ECS task target configuration.
type ECSTaskTargetParameters struct {
	NetworkConfiguration     *NetworkConfiguration          `json:"NetworkConfiguration,omitempty"`
	Overrides                *EcsTaskOverride               `json:"Overrides,omitempty"`
	TaskDefinitionArn        string                         `json:"TaskDefinitionArn,omitempty"`
	LaunchType               string                         `json:"LaunchType,omitempty"`
	Group                    string                         `json:"Group,omitempty"`
	PlatformVersion          string                         `json:"PlatformVersion,omitempty"`
	PropagateTags            string                         `json:"PropagateTags,omitempty"`
	ReferenceID              string                         `json:"ReferenceId,omitempty"`
	CapacityProviderStrategy []CapacityProviderStrategyItem `json:"CapacityProviderStrategy,omitempty"`
	PlacementConstraints     []PlacementConstraint          `json:"PlacementConstraints,omitempty"`
	PlacementStrategy        []PlacementStrategy            `json:"PlacementStrategy,omitempty"`
	Tags                     []EcsTag                       `json:"Tags,omitempty"`
	TaskCount                int                            `json:"TaskCount,omitempty"`
	EnableECSManagedTags     bool                           `json:"EnableECSManagedTags,omitempty"`
	EnableExecuteCommand     bool                           `json:"EnableExecuteCommand,omitempty"`
}

// TargetHTTPParameters holds HTTP-specific parameters for API Gateway and API destination targets.
type TargetHTTPParameters struct {
	HeaderParameters      map[string]string `json:"HeaderParameters,omitempty"`
	QueryStringParameters map[string]string `json:"QueryStringParameters,omitempty"`
	PathParameterValues   []string          `json:"PathParameterValues,omitempty"`
}

// TimestreamDimensionMapping maps an event field to a Timestream dimension.
type TimestreamDimensionMapping struct {
	DimensionName      string `json:"DimensionName,omitempty"`
	DimensionValue     string `json:"DimensionValue,omitempty"`
	DimensionValueType string `json:"DimensionValueType,omitempty"`
}

// TimestreamSingleMeasureMapping maps an event field to a single Timestream measure.
type TimestreamSingleMeasureMapping struct {
	MeasureName      string `json:"MeasureName,omitempty"`
	MeasureValue     string `json:"MeasureValue,omitempty"`
	MeasureValueType string `json:"MeasureValueType,omitempty"`
}

// TimestreamMultiMeasureAttributeMapping maps an event field to a multi-measure attribute.
type TimestreamMultiMeasureAttributeMapping struct {
	MeasureValue              string `json:"MeasureValue,omitempty"`
	MeasureValueType          string `json:"MeasureValueType,omitempty"`
	MultiMeasureAttributeName string `json:"MultiMeasureAttributeName,omitempty"`
}

// TimestreamMultiMeasureMapping maps event fields to a Timestream multi-measure record.
type TimestreamMultiMeasureMapping struct {
	MultiMeasureName              string                                   `json:"MultiMeasureName,omitempty"`
	MultiMeasureAttributeMappings []TimestreamMultiMeasureAttributeMapping `json:"MultiMeasureAttributeMappings,omitempty"`
}

// TimestreamParameters holds Timestream target configuration.
type TimestreamParameters struct {
	TimeValue             string                           `json:"TimeValue,omitempty"`
	TimeFieldType         string                           `json:"TimeFieldType,omitempty"`
	TimestampFormat       string                           `json:"TimestampFormat,omitempty"`
	EpochTimeUnit         string                           `json:"EpochTimeUnit,omitempty"`
	VersionValue          string                           `json:"VersionValue,omitempty"`
	DimensionMappings     []TimestreamDimensionMapping     `json:"DimensionMappings,omitempty"`
	SingleMeasureMappings []TimestreamSingleMeasureMapping `json:"SingleMeasureMappings,omitempty"`
	MultiMeasureMappings  []TimestreamMultiMeasureMapping  `json:"MultiMeasureMappings,omitempty"`
}

// TargetParameters holds target-specific configuration.
type TargetParameters struct {
	LambdaFunctionParameters      *LambdaFunctionParameters          `json:"LambdaFunctionParameters,omitempty"`
	SFNStateMachineParameters     *StepFunctionTargetParameters      `json:"StepFunctionStateMachineParameters,omitempty"`
	SqsQueueParameters            *SQSTargetParameters               `json:"SqsQueueParameters,omitempty"`
	KinesisStreamParameters       *KinesisStreamTargetParameters     `json:"KinesisStreamParameters,omitempty"`
	CloudWatchLogsParameters      *CloudWatchLogsTargetParameters    `json:"CloudWatchLogsParameters,omitempty"`
	EventBridgeEventBusParameters *EBEventBusTargetParameters        `json:"EventBridgeEventBusParameters,omitempty"`
	RedshiftDataParameters        *RedshiftDataTargetParameters      `json:"RedshiftDataParameters,omitempty"`
	SageMakerPipelineParameters   *SageMakerPipelineTargetParameters `json:"SageMakerPipelineParameters,omitempty"`
	BatchJobParameters            *BatchJobTargetParameters          `json:"BatchJobParameters,omitempty"`
	EcsTaskParameters             *ECSTaskTargetParameters           `json:"EcsTaskParameters,omitempty"`
	TimestreamParameters          *TimestreamParameters              `json:"TimestreamParameters,omitempty"`
	HTTPParameters                *TargetHTTPParameters              `json:"HttpParameters,omitempty"`
	InputTemplate                 string                             `json:"InputTemplate,omitempty"`
}

func cloneAwsVpcConfiguration(src *AwsVpcConfiguration) *AwsVpcConfiguration {
	if src == nil {
		return nil
	}
	vpc := *src
	vpc.Subnets = append([]string(nil), src.Subnets...)
	vpc.SecurityGroups = append([]string(nil), src.SecurityGroups...)

	return &vpc
}

func cloneBatchJobParameters(src *BatchJobTargetParameters) *BatchJobTargetParameters {
	v := *src
	if v.ArrayProperties != nil {
		ap := *v.ArrayProperties
		v.ArrayProperties = &ap
	}
	if v.RetryStrategy != nil {
		rs := *v.RetryStrategy
		v.RetryStrategy = &rs
	}
	if v.ContainerOverrides != nil {
		co := *v.ContainerOverrides
		co.Command = append([]string(nil), v.ContainerOverrides.Command...)
		co.Environment = append([]BatchEnvironmentVariable(nil), v.ContainerOverrides.Environment...)
		co.ResourceRequirements = append(
			[]BatchResourceRequirement(nil), v.ContainerOverrides.ResourceRequirements...,
		)
		v.ContainerOverrides = &co
	}
	v.DependsOn = append([]BatchJobDependency(nil), src.DependsOn...)
	v.Parameters = maps.Clone(src.Parameters)

	return &v
}

func cloneECSTaskParameters(src *ECSTaskTargetParameters) *ECSTaskTargetParameters {
	v := *src
	if v.NetworkConfiguration != nil {
		nc := *v.NetworkConfiguration
		nc.AwsvpcConfiguration = cloneAwsVpcConfiguration(v.NetworkConfiguration.AwsvpcConfiguration)
		v.NetworkConfiguration = &nc
	}
	if v.Overrides != nil {
		ov := *v.Overrides
		if ov.EphemeralStorage != nil {
			es := *ov.EphemeralStorage
			ov.EphemeralStorage = &es
		}
		ov.ContainerOverrides = append([]EcsContainerOverride(nil), v.Overrides.ContainerOverrides...)
		ov.InferenceAcceleratorOverrides = append(
			[]EcsInferenceAcceleratorOverride(nil), v.Overrides.InferenceAcceleratorOverrides...,
		)
		v.Overrides = &ov
	}
	v.CapacityProviderStrategy = append(
		[]CapacityProviderStrategyItem(nil), src.CapacityProviderStrategy...,
	)
	v.PlacementConstraints = append([]PlacementConstraint(nil), src.PlacementConstraints...)
	v.PlacementStrategy = append([]PlacementStrategy(nil), src.PlacementStrategy...)
	v.Tags = append([]EcsTag(nil), src.Tags...)

	return &v
}

func cloneTargetParameters(src *TargetParameters) *TargetParameters {
	tp := *src
	if src.LambdaFunctionParameters != nil {
		v := *src.LambdaFunctionParameters
		tp.LambdaFunctionParameters = &v
	}
	if src.SFNStateMachineParameters != nil {
		v := *src.SFNStateMachineParameters
		tp.SFNStateMachineParameters = &v
	}
	if src.SqsQueueParameters != nil {
		v := *src.SqsQueueParameters
		tp.SqsQueueParameters = &v
	}
	if src.KinesisStreamParameters != nil {
		v := *src.KinesisStreamParameters
		tp.KinesisStreamParameters = &v
	}
	if src.CloudWatchLogsParameters != nil {
		v := *src.CloudWatchLogsParameters
		tp.CloudWatchLogsParameters = &v
	}
	if src.EventBridgeEventBusParameters != nil {
		v := *src.EventBridgeEventBusParameters
		v.Resources = append([]string(nil), src.EventBridgeEventBusParameters.Resources...)
		tp.EventBridgeEventBusParameters = &v
	}
	if src.RedshiftDataParameters != nil {
		v := *src.RedshiftDataParameters
		v.Sqls = append([]string(nil), src.RedshiftDataParameters.Sqls...)
		tp.RedshiftDataParameters = &v
	}
	if src.SageMakerPipelineParameters != nil {
		v := *src.SageMakerPipelineParameters
		v.PipelineParameterList = append(
			[]SageMakerPipelineParameter(nil),
			src.SageMakerPipelineParameters.PipelineParameterList...,
		)
		tp.SageMakerPipelineParameters = &v
	}
	if src.BatchJobParameters != nil {
		tp.BatchJobParameters = cloneBatchJobParameters(src.BatchJobParameters)
	}
	if src.EcsTaskParameters != nil {
		tp.EcsTaskParameters = cloneECSTaskParameters(src.EcsTaskParameters)
	}
	if src.TimestreamParameters != nil {
		v := *src.TimestreamParameters
		v.DimensionMappings = append([]TimestreamDimensionMapping(nil), src.TimestreamParameters.DimensionMappings...)
		v.SingleMeasureMappings = append(
			[]TimestreamSingleMeasureMapping(nil),
			src.TimestreamParameters.SingleMeasureMappings...,
		)
		v.MultiMeasureMappings = append(
			[]TimestreamMultiMeasureMapping(nil),
			src.TimestreamParameters.MultiMeasureMappings...,
		)
		tp.TimestreamParameters = &v
	}
	if src.HTTPParameters != nil {
		v := *src.HTTPParameters
		v.HeaderParameters = maps.Clone(src.HTTPParameters.HeaderParameters)
		v.QueryStringParameters = maps.Clone(src.HTTPParameters.QueryStringParameters)
		v.PathParameterValues = append([]string(nil), src.HTTPParameters.PathParameterValues...)
		tp.HTTPParameters = &v
	}

	return &tp
}

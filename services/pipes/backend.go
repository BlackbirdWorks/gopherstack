package pipes

import (
<<<<<<< Updated upstream
	"context"
=======
>>>>>>> Stashed changes
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strings"
<<<<<<< Updated upstream
	"sync"
=======
>>>>>>> Stashed changes
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	stateRunning      = "RUNNING"
	stateStopped      = "STOPPED"
<<<<<<< Updated upstream
	stateCreating     = "CREATING"
	stateUpdating     = "UPDATING"
	stateDeleting     = "DELETING"
=======
>>>>>>> Stashed changes
	stateStarting     = "STARTING"
	stateStopping     = "STOPPING"
	stateCreateFailed = "CREATE_FAILED"
	stateUpdateFailed = "UPDATE_FAILED"
	stateDeleteFailed = "DELETE_FAILED"
<<<<<<< Updated upstream
	stateStartFailed  = "START_FAILED"
	stateStopFailed   = "STOP_FAILED"

	// stateTransitionDelay is the simulated delay for async state transitions.
	stateTransitionDelay = 10 * time.Millisecond
=======
>>>>>>> Stashed changes

	maxPipeNameLen  = 64
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
	maxTagsPerPipe  = 50
	maxPipesPerAcct = 1000

<<<<<<< Updated upstream
	// nextTokenSep separates cursor values in pagination tokens.
=======
	// nextTokenSep separates the name from the rest in a NextToken.
>>>>>>> Stashed changes
	nextTokenSep = "\x00"
)

var (
<<<<<<< Updated upstream
	ErrNotFound      = awserr.New("NotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)
=======
	ErrNotFound    = awserr.New("NotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	ErrValidation  = awserr.New("ValidationException", awserr.ErrInvalidParameter)
>>>>>>> Stashed changes

	pipeNameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

<<<<<<< Updated upstream
=======
// --- Sub-parameter types ---

>>>>>>> Stashed changes
// FilterCriteria holds event filter patterns applied before forwarding to the target.
type FilterCriteria struct {
	Filters []Filter `json:"Filters,omitempty"`
}

// Filter is a single JSON-pattern filter.
type Filter struct {
	Pattern string `json:"Pattern,omitempty"`
}

<<<<<<< Updated upstream
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

// EcsTaskOverride holds override values for an ECS task execution.
type EcsTaskOverride struct {
	TaskRoleArn      string `json:"TaskRoleArn,omitempty"`
	ExecutionRoleArn string `json:"ExecutionRoleArn,omitempty"`
	CPU              string `json:"Cpu,omitempty"`
	Memory           string `json:"Memory,omitempty"`
}

// BatchJobDependency represents a dependency between Batch jobs.
type BatchJobDependency struct {
	JobID string `json:"JobId,omitempty"`
	Type  string `json:"Type,omitempty"`
}

// BatchContainerOverrides holds container override values for a Batch job.
type BatchContainerOverrides struct {
	Environment  map[string]string `json:"Environment,omitempty"`
	InstanceType string            `json:"InstanceType,omitempty"`
	Command      []string          `json:"Command,omitempty"`
}

// SelfManagedKafkaAccessCredentials holds authentication credentials for self-managed Kafka.
// Exactly one field is populated (models an AWS union type).
type SelfManagedKafkaAccessCredentials struct {
	BasicAuth                string `json:"BasicAuth,omitempty"`
	ClientCertificateTLSAuth string `json:"ClientCertificateTlsAuth,omitempty"`
	SaslScram256Auth         string `json:"SaslScram256Auth,omitempty"`
	SaslScram512Auth         string `json:"SaslScram512Auth,omitempty"`
}

// SelfManagedKafkaVpc holds VPC configuration for self-managed Kafka connectivity.
type SelfManagedKafkaVpc struct {
	SecurityGroup []string `json:"SecurityGroup,omitempty"`
	Subnets       []string `json:"Subnets,omitempty"`
}

// MSKAccessCredentials holds authentication credentials for MSK sources.
// Exactly one field is populated (models an AWS union type).
type MSKAccessCredentials struct {
	ClientCertificateTLSAuth string `json:"ClientCertificateTlsAuth,omitempty"`
	SaslScram512Auth         string `json:"SaslScram512Auth,omitempty"`
}

// MQBrokerCredentials holds credentials for ActiveMQ or RabbitMQ broker sources.
type MQBrokerCredentials struct {
	BasicAuth string `json:"BasicAuth,omitempty"`
}

// CloudWatchMetricsDestination configures a CloudWatch metrics destination.
type CloudWatchMetricsDestination struct {
	Namespace string `json:"Namespace,omitempty"`
}

// MetricsDestination wraps the destination for pipe runtime metrics.
type MetricsDestination struct {
	CloudwatchMetrics *CloudWatchMetricsDestination `json:"CloudwatchMetrics,omitempty"`
}

// RuntimeMetricsStreaming configures runtime metrics streaming for a pipe.
type RuntimeMetricsStreaming struct {
	MetricsDestination *MetricsDestination `json:"MetricsDestination,omitempty"`
	Level              string              `json:"Level,omitempty"`
}

=======
>>>>>>> Stashed changes
// SQSSourceParameters holds SQS-specific source configuration.
type SQSSourceParameters struct {
	BatchSize                      int `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int `json:"MaximumBatchingWindowInSeconds,omitempty"`
}

<<<<<<< Updated upstream
// KinesisStreamSourceParameters holds Kinesis-specific source configuration.
type KinesisStreamSourceParameters struct {
	StartingPositionTimestamp      *time.Time        `json:"StartingPositionTimestamp,omitempty"`
	DeadLetterConfig               *DeadLetterConfig `json:"DeadLetterConfig,omitempty"`
	StartingPosition               string            `json:"StartingPosition,omitempty"`
	OnPartialBatchItemFailure      string            `json:"OnPartialBatchItemFailure,omitempty"`
	BatchSize                      int               `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int               `json:"MaximumBatchingWindowInSeconds,omitempty"`
	MaximumRecordAgeInSeconds      int               `json:"MaximumRecordAgeInSeconds,omitempty"`
	MaximumRetryAttempts           int               `json:"MaximumRetryAttempts,omitempty"`
	ParallelizationFactor          int               `json:"ParallelizationFactor,omitempty"`
}

// DynamoDBStreamSourceParameters holds DynamoDB stream source configuration.
type DynamoDBStreamSourceParameters struct {
	DeadLetterConfig               *DeadLetterConfig `json:"DeadLetterConfig,omitempty"`
	StartingPosition               string            `json:"StartingPosition,omitempty"`
	OnPartialBatchItemFailure      string            `json:"OnPartialBatchItemFailure,omitempty"`
	BatchSize                      int               `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int               `json:"MaximumBatchingWindowInSeconds,omitempty"`
	MaximumRecordAgeInSeconds      int               `json:"MaximumRecordAgeInSeconds,omitempty"`
	MaximumRetryAttempts           int               `json:"MaximumRetryAttempts,omitempty"`
	ParallelizationFactor          int               `json:"ParallelizationFactor,omitempty"`
}

// MSKSourceParameters holds MSK source configuration.
type MSKSourceParameters struct {
	Credentials                    *MSKAccessCredentials `json:"Credentials,omitempty"`
	TopicName                      string                `json:"TopicName,omitempty"`
	StartingPosition               string                `json:"StartingPosition,omitempty"`
	ConsumerGroupID                string                `json:"ConsumerGroupId,omitempty"`
	BatchSize                      int                   `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int                   `json:"MaximumBatchingWindowInSeconds,omitempty"`
}

// SelfManagedKafkaSourceParameters holds self-managed Kafka source configuration.
type SelfManagedKafkaSourceParameters struct {
	Credentials                    *SelfManagedKafkaAccessCredentials `json:"Credentials,omitempty"`
	Vpc                            *SelfManagedKafkaVpc               `json:"Vpc,omitempty"`
	TopicName                      string                             `json:"TopicName,omitempty"`
	StartingPosition               string                             `json:"StartingPosition,omitempty"`
	ConsumerGroupID                string                             `json:"ConsumerGroupId,omitempty"`
	ServerRootCaCertificate        string                             `json:"ServerRootCaCertificate,omitempty"`
	AdditionalBootstrapServers     []string                           `json:"AdditionalBootstrapServers,omitempty"`
	BatchSize                      int                                `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int                                `json:"MaximumBatchingWindowInSeconds,omitempty"`
}

// RabbitMQBrokerSourceParameters holds RabbitMQ broker source configuration.
type RabbitMQBrokerSourceParameters struct {
	Credentials                    *MQBrokerCredentials `json:"Credentials,omitempty"`
	QueueName                      string               `json:"QueueName,omitempty"`
	VirtualHost                    string               `json:"VirtualHost,omitempty"`
	BatchSize                      int                  `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int                  `json:"MaximumBatchingWindowInSeconds,omitempty"`
}

// ActiveMQBrokerSourceParameters holds ActiveMQ broker source configuration.
type ActiveMQBrokerSourceParameters struct {
	Credentials                    *MQBrokerCredentials `json:"Credentials,omitempty"`
	QueueName                      string               `json:"QueueName,omitempty"`
	BatchSize                      int                  `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int                  `json:"MaximumBatchingWindowInSeconds,omitempty"`
}

// SourceParameters holds source-specific configuration.
type SourceParameters struct {
	FilterCriteria                  *FilterCriteria                   `json:"FilterCriteria,omitempty"`
	SqsQueueParameters              *SQSSourceParameters              `json:"SqsQueueParameters,omitempty"`
	KinesisStreamParameters         *KinesisStreamSourceParameters    `json:"KinesisStreamParameters,omitempty"`
	DynamoDBStreamParameters        *DynamoDBStreamSourceParameters   `json:"DynamoDBStreamParameters,omitempty"`
	ManagedStreamingKafkaParameters *MSKSourceParameters              `json:"ManagedStreamingKafkaParameters,omitempty"`
	SelfManagedKafkaParameters      *SelfManagedKafkaSourceParameters `json:"SelfManagedKafkaParameters,omitempty"`
	RabbitMQBrokerParameters        *RabbitMQBrokerSourceParameters   `json:"RabbitMQBrokerParameters,omitempty"`
	ActiveMQBrokerParameters        *ActiveMQBrokerSourceParameters   `json:"ActiveMQBrokerParameters,omitempty"`
=======
// SourceParameters holds source-specific configuration.
type SourceParameters struct {
	FilterCriteria     *FilterCriteria      `json:"FilterCriteria,omitempty"`
	SqsQueueParameters *SQSSourceParameters `json:"SqsQueueParameters,omitempty"`
>>>>>>> Stashed changes
}

// LambdaFunctionParameters holds Lambda-specific target configuration.
type LambdaFunctionParameters struct {
	InvocationType string `json:"InvocationType,omitempty"`
}

<<<<<<< Updated upstream
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

// ECSTaskTargetParameters holds ECS task target configuration.
type ECSTaskTargetParameters struct {
	NetworkConfiguration     *NetworkConfiguration          `json:"NetworkConfiguration,omitempty"`
	Overrides                *EcsTaskOverride               `json:"Overrides,omitempty"`
	TaskDefinitionArn        string                         `json:"TaskDefinitionArn,omitempty"`
	LaunchType               string                         `json:"LaunchType,omitempty"`
	Group                    string                         `json:"Group,omitempty"`
	PlatformVersion          string                         `json:"PlatformVersion,omitempty"`
	CapacityProviderStrategy []CapacityProviderStrategyItem `json:"CapacityProviderStrategy,omitempty"`
	PlacementConstraints     []PlacementConstraint          `json:"PlacementConstraints,omitempty"`
	PlacementStrategy        []PlacementStrategy            `json:"PlacementStrategy,omitempty"`
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
=======
// TargetParameters holds target-specific configuration.
type TargetParameters struct {
	InputTemplate            string                    `json:"InputTemplate,omitempty"`
	LambdaFunctionParameters *LambdaFunctionParameters `json:"LambdaFunctionParameters,omitempty"`
>>>>>>> Stashed changes
}

// DeadLetterConfig identifies the DLQ for failed pipe events.
type DeadLetterConfig struct {
	Arn string `json:"Arn,omitempty"`
}

<<<<<<< Updated upstream
// EnrichmentHTTPParameters holds HTTP parameters for enrichment calls.
type EnrichmentHTTPParameters struct {
	HeaderParameters      map[string]string `json:"HeaderParameters,omitempty"`
	QueryStringParameters map[string]string `json:"QueryStringParameters,omitempty"`
	PathParameterValues   []string          `json:"PathParameterValues,omitempty"`
}

// EnrichmentParameters holds enrichment-specific configuration.
type EnrichmentParameters struct {
	HTTPParameters *EnrichmentHTTPParameters `json:"HttpParameters,omitempty"`
	InputTemplate  string                    `json:"InputTemplate,omitempty"`
}

// CloudwatchLogsLogDestination is a CloudWatch Logs target.
=======
// CloudwatchLogsLogDestination is a CloudWatch Logs target for pipe execution logs.
>>>>>>> Stashed changes
type CloudwatchLogsLogDestination struct {
	LogGroupArn string `json:"LogGroupArn,omitempty"`
}

<<<<<<< Updated upstream
// FirehoseLogDestination is a Firehose delivery stream log target.
type FirehoseLogDestination struct {
	DeliveryStreamArn string `json:"DeliveryStreamArn,omitempty"`
}

// S3LogDestination is an S3 bucket log target.
type S3LogDestination struct {
	BucketName   string `json:"BucketName,omitempty"`
	BucketOwner  string `json:"BucketOwner,omitempty"`
	Prefix       string `json:"Prefix,omitempty"`
	OutputFormat string `json:"OutputFormat,omitempty"`
}

// LogDestination wraps possible log destination types.
type LogDestination struct {
	CloudwatchLogsLogDestination *CloudwatchLogsLogDestination `json:"CloudwatchLogsLogDestination,omitempty"`
	FirehoseLogDestination       *FirehoseLogDestination       `json:"FirehoseLogDestination,omitempty"`
	S3LogDestination             *S3LogDestination             `json:"S3LogDestination,omitempty"`
=======
// LogDestination wraps possible log destination types.
type LogDestination struct {
	CloudwatchLogsLogDestination *CloudwatchLogsLogDestination `json:"CloudwatchLogsLogDestination,omitempty"`
>>>>>>> Stashed changes
}

// LogConfiguration controls pipe execution logging.
type LogConfiguration struct {
<<<<<<< Updated upstream
	Level                string           `json:"Level,omitempty"`
	Destinations         []LogDestination `json:"Destinations,omitempty"`
	IncludeExecutionData []string         `json:"IncludeExecutionData,omitempty"`
}

// Pipe represents an EventBridge Pipe.
type Pipe struct {
	SourceParameters        *SourceParameters        `json:"sourceParameters,omitempty"`
	TargetParameters        *TargetParameters        `json:"targetParameters,omitempty"`
	DeadLetterConfig        *DeadLetterConfig        `json:"deadLetterConfig,omitempty"`
	LogConfiguration        *LogConfiguration        `json:"logConfiguration,omitempty"`
	EnrichmentParameters    *EnrichmentParameters    `json:"enrichmentParameters,omitempty"`
	RuntimeMetricsStreaming *RuntimeMetricsStreaming `json:"runtimeMetricsStreaming,omitempty"`
	LastModifiedTime        time.Time                `json:"lastModifiedTime"`
	CreationTime            time.Time                `json:"creationTime"`
	Tags                    map[string]string        `json:"tags,omitempty"`
	Description             string                   `json:"description,omitempty"`
	Enrichment              string                   `json:"enrichment,omitempty"`
	KmsKeyIdentifier        string                   `json:"kmsKeyIdentifier,omitempty"`
	Source                  string                   `json:"source"`
	Target                  string                   `json:"target"`
	RoleARN                 string                   `json:"roleArn"`
	StateReason             string                   `json:"stateReason,omitempty"`
	DesiredState            string                   `json:"desiredState"`
	CurrentState            string                   `json:"currentState"`
	AccountID               string                   `json:"accountID"`
	Region                  string                   `json:"region"`
	ARN                     string                   `json:"arn"`
	Name                    string                   `json:"name"`
}

func sourceBatchSize(sp *SourceParameters) int {
	switch {
	case sp.SqsQueueParameters != nil && sp.SqsQueueParameters.BatchSize > 0:
		return sp.SqsQueueParameters.BatchSize
	case sp.KinesisStreamParameters != nil && sp.KinesisStreamParameters.BatchSize > 0:
		return sp.KinesisStreamParameters.BatchSize
	case sp.DynamoDBStreamParameters != nil && sp.DynamoDBStreamParameters.BatchSize > 0:
		return sp.DynamoDBStreamParameters.BatchSize
	case sp.ManagedStreamingKafkaParameters != nil && sp.ManagedStreamingKafkaParameters.BatchSize > 0:
		return sp.ManagedStreamingKafkaParameters.BatchSize
	case sp.SelfManagedKafkaParameters != nil && sp.SelfManagedKafkaParameters.BatchSize > 0:
		return sp.SelfManagedKafkaParameters.BatchSize
	case sp.RabbitMQBrokerParameters != nil && sp.RabbitMQBrokerParameters.BatchSize > 0:
		return sp.RabbitMQBrokerParameters.BatchSize
	case sp.ActiveMQBrokerParameters != nil && sp.ActiveMQBrokerParameters.BatchSize > 0:
		return sp.ActiveMQBrokerParameters.BatchSize
	}

	return 0
}

const maxBatchSize = 10000

// validateSourceBatchSize checks that all BatchSize fields in SourceParameters
// are within the valid range [0, 10000]. Negative values and values above
// 10000 are rejected with a ValidationException, matching AWS behaviour.
// batchSizeEntry pairs a BatchSize value with its source type label.
type batchSizeEntry struct {
	Name string
	Size int
}

// sourceBatchSizes collects all configured BatchSize values from SourceParameters
// along with their source type labels.
func sourceBatchSizes(sp *SourceParameters) []batchSizeEntry {
	var out []batchSizeEntry

	if p := sp.SqsQueueParameters; p != nil {
		out = append(out, batchSizeEntry{"SQS", p.BatchSize})
	}
	if p := sp.KinesisStreamParameters; p != nil {
		out = append(out, batchSizeEntry{"Kinesis", p.BatchSize})
	}
	if p := sp.DynamoDBStreamParameters; p != nil {
		out = append(out, batchSizeEntry{"DynamoDB", p.BatchSize})
	}
	if p := sp.ManagedStreamingKafkaParameters; p != nil {
		out = append(out, batchSizeEntry{"MSK", p.BatchSize})
	}
	if p := sp.SelfManagedKafkaParameters; p != nil {
		out = append(out, batchSizeEntry{"SelfManagedKafka", p.BatchSize})
	}
	if p := sp.RabbitMQBrokerParameters; p != nil {
		out = append(out, batchSizeEntry{"RabbitMQ", p.BatchSize})
	}
	if p := sp.ActiveMQBrokerParameters; p != nil {
		out = append(out, batchSizeEntry{"ActiveMQ", p.BatchSize})
	}

	return out
}

func validateSourceBatchSize(sp *SourceParameters) error {
	if sp == nil {
		return nil
	}

	for _, bs := range sourceBatchSizes(sp) {
		if bs.Size < 0 || bs.Size > maxBatchSize {
			return fmt.Errorf(
				"%w: %s BatchSize must be between 0 and %d, got %d",
				ErrValidation, bs.Name, maxBatchSize, bs.Size,
			)
		}
	}

	return nil
}

func (p *Pipe) effectiveBatchSize() int {
	if p.SourceParameters != nil {
		if bs := sourceBatchSize(p.SourceParameters); bs > 0 {
			return bs
		}
	}

	return pipeDefaultBatchSize
}

func cloneDeadLetterConfig(src *DeadLetterConfig) *DeadLetterConfig {
	if src == nil {
		return nil
	}
	v := *src

	return &v
}

func cloneSelfManagedKafkaVpc(src *SelfManagedKafkaVpc) *SelfManagedKafkaVpc {
	if src == nil {
		return nil
	}
	vpc := *src
	vpc.SecurityGroup = append([]string(nil), src.SecurityGroup...)
	vpc.Subnets = append([]string(nil), src.Subnets...)

	return &vpc
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
		co.Environment = maps.Clone(v.ContainerOverrides.Environment)
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
		v.Overrides = &ov
	}
	v.CapacityProviderStrategy = append(
		[]CapacityProviderStrategyItem(nil), src.CapacityProviderStrategy...,
	)
	v.PlacementConstraints = append([]PlacementConstraint(nil), src.PlacementConstraints...)
	v.PlacementStrategy = append([]PlacementStrategy(nil), src.PlacementStrategy...)

	return &v
}

func cloneSourceParameters(src *SourceParameters) *SourceParameters {
	sp := *src
	if src.FilterCriteria != nil {
		fc := *src.FilterCriteria
		fc.Filters = append([]Filter(nil), src.FilterCriteria.Filters...)
		sp.FilterCriteria = &fc
	}
	if src.SqsQueueParameters != nil {
		v := *src.SqsQueueParameters
		sp.SqsQueueParameters = &v
	}
	if src.KinesisStreamParameters != nil {
		v := *src.KinesisStreamParameters
		v.DeadLetterConfig = cloneDeadLetterConfig(v.DeadLetterConfig)
		sp.KinesisStreamParameters = &v
	}
	if src.DynamoDBStreamParameters != nil {
		v := *src.DynamoDBStreamParameters
		v.DeadLetterConfig = cloneDeadLetterConfig(v.DeadLetterConfig)
		sp.DynamoDBStreamParameters = &v
	}
	if src.ManagedStreamingKafkaParameters != nil {
		v := *src.ManagedStreamingKafkaParameters
		if v.Credentials != nil {
			c := *v.Credentials
			v.Credentials = &c
		}
		sp.ManagedStreamingKafkaParameters = &v
	}
	if src.SelfManagedKafkaParameters != nil {
		v := *src.SelfManagedKafkaParameters
		v.AdditionalBootstrapServers = append(
			[]string(nil), src.SelfManagedKafkaParameters.AdditionalBootstrapServers...,
		)
		if v.Credentials != nil {
			c := *v.Credentials
			v.Credentials = &c
		}
		v.Vpc = cloneSelfManagedKafkaVpc(v.Vpc)
		sp.SelfManagedKafkaParameters = &v
	}
	if src.RabbitMQBrokerParameters != nil {
		v := *src.RabbitMQBrokerParameters
		if v.Credentials != nil {
			c := *v.Credentials
			v.Credentials = &c
		}
		sp.RabbitMQBrokerParameters = &v
	}
	if src.ActiveMQBrokerParameters != nil {
		v := *src.ActiveMQBrokerParameters
		if v.Credentials != nil {
			c := *v.Credentials
			v.Credentials = &c
		}
		sp.ActiveMQBrokerParameters = &v
	}

	return &sp
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

func cloneEnrichmentParameters(src *EnrichmentParameters) *EnrichmentParameters {
	ep := *src
	if src.HTTPParameters != nil {
		hp := *src.HTTPParameters
		hp.HeaderParameters = maps.Clone(src.HTTPParameters.HeaderParameters)
		hp.PathParameterValues = append([]string(nil), src.HTTPParameters.PathParameterValues...)
		hp.QueryStringParameters = maps.Clone(src.HTTPParameters.QueryStringParameters)
		ep.HTTPParameters = &hp
	}

	return &ep
}

=======
	Destinations         []LogDestination `json:"Destinations,omitempty"`
	IncludeExecutionData []string         `json:"IncludeExecutionData,omitempty"`
	Level                string           `json:"Level,omitempty"`
}

// --- Pipe ---

// Pipe represents an EventBridge Pipe.
type Pipe struct {
	SourceParameters *SourceParameters `json:"sourceParameters,omitempty"`
	TargetParameters *TargetParameters `json:"targetParameters,omitempty"`
	DeadLetterConfig *DeadLetterConfig `json:"deadLetterConfig,omitempty"`
	LogConfiguration *LogConfiguration `json:"logConfiguration,omitempty"`
	LastModifiedTime time.Time         `json:"lastModifiedTime"`
	CreationTime     time.Time         `json:"creationTime"`
	Tags             map[string]string `json:"tags,omitempty"`
	Description      string            `json:"description,omitempty"`
	Enrichment       string            `json:"enrichment,omitempty"`
	Source           string            `json:"source"`
	Target           string            `json:"target"`
	RoleARN          string            `json:"roleArn"`
	StateReason      string            `json:"stateReason,omitempty"`
	DesiredState     string            `json:"desiredState"`
	CurrentState     string            `json:"currentState"`
	AccountID        string            `json:"accountID"`
	Region           string            `json:"region"`
	ARN              string            `json:"arn"`
	Name             string            `json:"name"`
}

// effectiveBatchSize returns the configured SQS batch size, or the default.
func (p *Pipe) effectiveBatchSize() int {
	if p.SourceParameters != nil &&
		p.SourceParameters.SqsQueueParameters != nil &&
		p.SourceParameters.SqsQueueParameters.BatchSize > 0 {
		return p.SourceParameters.SqsQueueParameters.BatchSize
	}

	return pipeDefaultBatchSize
}

// clonePipe returns a deep copy of p.
>>>>>>> Stashed changes
func clonePipe(p *Pipe) *Pipe {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)
	if p.SourceParameters != nil {
		cp.SourceParameters = cloneSourceParameters(p.SourceParameters)
	}
	if p.TargetParameters != nil {
		cp.TargetParameters = cloneTargetParameters(p.TargetParameters)
	}
	if p.DeadLetterConfig != nil {
		dlc := *p.DeadLetterConfig
		cp.DeadLetterConfig = &dlc
	}
	if p.EnrichmentParameters != nil {
		cp.EnrichmentParameters = cloneEnrichmentParameters(p.EnrichmentParameters)
	}
	if p.LogConfiguration != nil {
		lc := *p.LogConfiguration
		lc.Destinations = append([]LogDestination(nil), p.LogConfiguration.Destinations...)
		lc.IncludeExecutionData = append([]string(nil), p.LogConfiguration.IncludeExecutionData...)
		cp.LogConfiguration = &lc
	}
	if p.RuntimeMetricsStreaming != nil {
		rms := *p.RuntimeMetricsStreaming
		if rms.MetricsDestination != nil {
			md := *rms.MetricsDestination
			if md.CloudwatchMetrics != nil {
				cw := *md.CloudwatchMetrics
				md.CloudwatchMetrics = &cw
			}
			rms.MetricsDestination = &md
		}
		cp.RuntimeMetricsStreaming = &rms
	}

	if p.SourceParameters != nil {
		sp := *p.SourceParameters
		if p.SourceParameters.FilterCriteria != nil {
			fc := *p.SourceParameters.FilterCriteria
			fc.Filters = append([]Filter(nil), p.SourceParameters.FilterCriteria.Filters...)
			sp.FilterCriteria = &fc
		}

		if p.SourceParameters.SqsQueueParameters != nil {
			sqsp := *p.SourceParameters.SqsQueueParameters
			sp.SqsQueueParameters = &sqsp
		}

		cp.SourceParameters = &sp
	}

	if p.TargetParameters != nil {
		tp := *p.TargetParameters
		if p.TargetParameters.LambdaFunctionParameters != nil {
			lfp := *p.TargetParameters.LambdaFunctionParameters
			tp.LambdaFunctionParameters = &lfp
		}

		cp.TargetParameters = &tp
	}

	if p.DeadLetterConfig != nil {
		dlc := *p.DeadLetterConfig
		cp.DeadLetterConfig = &dlc
	}

	if p.LogConfiguration != nil {
		lc := *p.LogConfiguration
		lc.Destinations = append([]LogDestination(nil), p.LogConfiguration.Destinations...)
		lc.IncludeExecutionData = append([]string(nil), p.LogConfiguration.IncludeExecutionData...)
		cp.LogConfiguration = &lc
	}

	return &cp
}

<<<<<<< Updated upstream
// InMemoryBackend is the in-memory store for pipes.
=======
// --- Backend ---

>>>>>>> Stashed changes
type InMemoryBackend struct {
	svcCtx              context.Context
	pipes               map[string]*Pipe
	pipeARNIndex        map[string]string
	enrichmentCallCount map[string]int64
	mu                  *lockmetrics.RWMutex
	cancel              context.CancelFunc
	accountID           string
	region              string
	wg                  sync.WaitGroup
}

// NewInMemoryBackend creates a new InMemoryBackend with a background lifecycle
// context. Prefer [NewInMemoryBackendWithContext] when a service context is
// available so delayed state transitions are cancelled on shutdown.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new InMemoryBackend whose delayed
// state-transition goroutines are tied to svcCtx, so they are cancelled when the
// service shuts down. If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	ctx, cancel := context.WithCancel(svcCtx)

	return &InMemoryBackend{
		pipes:               make(map[string]*Pipe),
		pipeARNIndex:        make(map[string]string),
		enrichmentCallCount: make(map[string]int64),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("pipes"),
		svcCtx:              ctx,
		cancel:              cancel,
	}
}

// runDelayed runs fn after delay, unless the backend's lifecycle context is
// cancelled first. The goroutine is tracked by b.wg so [InMemoryBackend.Shutdown]
// can wait for it.
func (b *InMemoryBackend) runDelayed(fn func()) {
	b.wg.Go(func() {
		select {
		case <-b.svcCtx.Done():
			return
		case <-time.After(stateTransitionDelay):
		}

		fn()
	})
}

// Shutdown cancels in-flight delayed state transitions and waits for their
// goroutines to exit, bounded by ctx. It implements the service shutdown
// contract used by the handler.
func (b *InMemoryBackend) Shutdown(ctx context.Context) {
	if b.cancel != nil {
		b.cancel()
	}

	done := make(chan struct{})

	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// RecordEnrichmentCall increments the enrichment invocation counter for a pipe.
func (b *InMemoryBackend) RecordEnrichmentCall(pipeName string) {
	b.mu.Lock("RecordEnrichmentCall")
	defer b.mu.Unlock()
	b.enrichmentCallCount[pipeName]++
}

// GetEnrichmentCallCount returns the number of enrichment calls for a pipe.
func (b *InMemoryBackend) GetEnrichmentCallCount(pipeName string) int64 {
	b.mu.RLock("GetEnrichmentCallCount")
	defer b.mu.RUnlock()

	return b.enrichmentCallCount[pipeName]
}

func (b *InMemoryBackend) Region() string { return b.region }

// CreatePipeInput holds the full set of fields for pipe creation.
type CreatePipeInput struct {
<<<<<<< Updated upstream
	Tags                    map[string]string
	SourceParameters        *SourceParameters
	TargetParameters        *TargetParameters
	DeadLetterConfig        *DeadLetterConfig
	LogConfiguration        *LogConfiguration
	EnrichmentParameters    *EnrichmentParameters
	RuntimeMetricsStreaming *RuntimeMetricsStreaming
	Name                    string
	RoleARN                 string
	Source                  string
	Target                  string
	Description             string
	Enrichment              string
	KmsKeyIdentifier        string
	DesiredState            string
}

=======
	Tags             map[string]string
	SourceParameters *SourceParameters
	TargetParameters *TargetParameters
	DeadLetterConfig *DeadLetterConfig
	LogConfiguration *LogConfiguration
	Name             string
	RoleARN          string
	Source           string
	Target           string
	Description      string
	Enrichment       string
	DesiredState     string
}

// CreatePipe creates a new pipe.
>>>>>>> Stashed changes
func (b *InMemoryBackend) CreatePipe(in CreatePipeInput) (*Pipe, error) {
	if err := validatePipeName(in.Name); err != nil {
		return nil, err
	}
<<<<<<< Updated upstream
	if err := validateDesiredState(in.DesiredState); err != nil {
		return nil, err
	}
	if in.Source == "" {
		return nil, fmt.Errorf("%w: Source is required", ErrValidation)
	}
	if in.Target == "" {
		return nil, fmt.Errorf("%w: Target is required", ErrValidation)
	}
	if err := validateTags(in.Tags); err != nil {
		return nil, err
	}
	if err := validateSourceBatchSize(in.SourceParameters); err != nil {
		return nil, err
	}
=======

	if err := validateDesiredState(in.DesiredState); err != nil {
		return nil, err
	}

	if in.Source == "" {
		return nil, fmt.Errorf("%w: Source is required", ErrValidation)
	}

	if in.Target == "" {
		return nil, fmt.Errorf("%w: Target is required", ErrValidation)
	}

	if err := validateTags(in.Tags); err != nil {
		return nil, err
	}
>>>>>>> Stashed changes

	b.mu.Lock("CreatePipe")
	defer b.mu.Unlock()

	if len(b.pipes) >= maxPipesPerAcct {
<<<<<<< Updated upstream
		return nil, fmt.Errorf(
			"%w: account has reached the maximum number of pipes (%d)",
			ErrValidation,
			maxPipesPerAcct,
		)
	}
	if _, ok := b.pipes[in.Name]; ok {
		return nil, fmt.Errorf("%w: pipe %s already exists", ErrAlreadyExists, in.Name)
	}
=======
		return nil, fmt.Errorf("%w: account has reached the maximum number of pipes (%d)",
			ErrValidation, maxPipesPerAcct)
	}

	if _, ok := b.pipes[in.Name]; ok {
		return nil, fmt.Errorf("%w: pipe %s already exists", ErrAlreadyExists, in.Name)
	}

>>>>>>> Stashed changes
	if in.DesiredState == "" {
		in.DesiredState = stateRunning
	}

	now := time.Now()
	pipeARN := arn.Build("pipes", b.region, b.accountID, "pipe/"+in.Name)
	p := &Pipe{
<<<<<<< Updated upstream
		Name: in.Name, ARN: pipeARN, RoleARN: in.RoleARN,
		Source: in.Source, Target: in.Target, Description: in.Description,
		Enrichment: in.Enrichment, KmsKeyIdentifier: in.KmsKeyIdentifier,
		DesiredState: in.DesiredState, CurrentState: stateCreating,
		AccountID: b.accountID, Region: b.region,
		CreationTime: now, LastModifiedTime: now,
		Tags:                    mergeTags(nil, in.Tags),
		SourceParameters:        in.SourceParameters,
		TargetParameters:        in.TargetParameters,
		DeadLetterConfig:        in.DeadLetterConfig,
		LogConfiguration:        in.LogConfiguration,
		EnrichmentParameters:    in.EnrichmentParameters,
		RuntimeMetricsStreaming: in.RuntimeMetricsStreaming,
=======
		Name:             in.Name,
		ARN:              pipeARN,
		RoleARN:          in.RoleARN,
		Source:           in.Source,
		Target:           in.Target,
		Description:      in.Description,
		Enrichment:       in.Enrichment,
		DesiredState:     in.DesiredState,
		CurrentState:     in.DesiredState,
		AccountID:        b.accountID,
		Region:           b.region,
		CreationTime:     now,
		LastModifiedTime: now,
		Tags:             mergeTags(nil, in.Tags),
		SourceParameters: in.SourceParameters,
		TargetParameters: in.TargetParameters,
		DeadLetterConfig: in.DeadLetterConfig,
		LogConfiguration: in.LogConfiguration,
>>>>>>> Stashed changes
	}
	b.pipes[in.Name] = p
	b.pipeARNIndex[pipeARN] = in.Name

	cp := clonePipe(p)
	b.runDelayed(func() {
		b.completeCreateTransition(in.Name, in.DesiredState)
	})

	return cp, nil
}

// completeCreateTransition moves a pipe from CREATING to its desired state.
func (b *InMemoryBackend) completeCreateTransition(name, desiredState string) {
	b.mu.Lock("completeCreateTransition")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return
	}
	if p.CurrentState == stateCreating {
		p.CurrentState = desiredState
		p.LastModifiedTime = time.Now()
	}
}

func (b *InMemoryBackend) GetPipe(name string) (*Pipe, error) {
	b.mu.RLock("GetPipe")
	defer b.mu.RUnlock()
	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}

	return clonePipe(p), nil
}

// ListPipesFilter holds optional query parameters for ListPipes.
type ListPipesFilter struct {
	NamePrefix   string
	DesiredState string
	CurrentState string
	SourcePrefix string
	TargetPrefix string
	NextToken    string
	Limit        int
}

// ListPipesResult holds the paginated result of a ListPipes call.
type ListPipesResult struct {
<<<<<<< Updated upstream
	NextToken string
	Pipes     []*Pipe
}

=======
	Pipes     []*Pipe
	NextToken string
}

// ListPipes returns pipes matching the filter with optional pagination.
>>>>>>> Stashed changes
func (b *InMemoryBackend) ListPipes(f ListPipesFilter) ListPipesResult {
	b.mu.RLock("ListPipes")
	defer b.mu.RUnlock()

	limit := f.Limit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

<<<<<<< Updated upstream
	names := b.sortedPipeNames()
	startIdx := b.resolveStartIndex(names, f.NextToken)
	result, lastIncluded := b.collectMatchingPipes(names, startIdx, limit, f)
	nextToken := b.buildNextToken(names, startIdx, len(result), limit, lastIncluded, f)

	return ListPipesResult{Pipes: result, NextToken: nextToken}
}

func (b *InMemoryBackend) sortedPipeNames() []string {
	names := make([]string, 0, len(b.pipes))
	for name := range b.pipes {
		names = append(names, name)
	}
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}

	return names
}

func (b *InMemoryBackend) resolveStartIndex(names []string, nextToken string) int {
	if nextToken == "" {
		return 0
	}
	decoded, err := base64.StdEncoding.DecodeString(nextToken)
	if err != nil {
		return 0
	}
	cursor := strings.TrimSuffix(string(decoded), nextTokenSep)
	startIdx := len(names)
	for i, n := range names {
		if n > cursor {
			startIdx = i

			break
		}
	}

	return startIdx
}

func (b *InMemoryBackend) collectMatchingPipes(
	names []string, startIdx, limit int, f ListPipesFilter,
) ([]*Pipe, string) {
	var result []*Pipe
	var lastIncluded string
	for i := startIdx; i < len(names); i++ {
		if len(result) >= limit {
			break
		}
		p := b.pipes[names[i]]
		if !matchesFilter(p, f) {
			continue
		}
		result = append(result, clonePipe(p))
		lastIncluded = p.Name
	}

	return result, lastIncluded
}

func (b *InMemoryBackend) buildNextToken(
	names []string, startIdx, resultLen, limit int, lastIncluded string, f ListPipesFilter,
) string {
	if resultLen < limit || lastIncluded == "" {
		return ""
	}
	for i := startIdx + resultLen; i < len(names); i++ {
		if matchesFilter(b.pipes[names[i]], f) {
			return base64.StdEncoding.EncodeToString([]byte(lastIncluded + nextTokenSep))
		}
	}

	return ""
}

func matchesFilter(p *Pipe, f ListPipesFilter) bool {
	if f.NamePrefix != "" && !strings.HasPrefix(p.Name, f.NamePrefix) {
		return false
	}
	if f.DesiredState != "" && p.DesiredState != f.DesiredState {
		return false
	}
	if f.CurrentState != "" && p.CurrentState != f.CurrentState {
		return false
	}
	if f.SourcePrefix != "" && !strings.HasPrefix(p.Source, f.SourcePrefix) {
		return false
	}
	if f.TargetPrefix != "" && !strings.HasPrefix(p.Target, f.TargetPrefix) {
		return false
	}

	return true
}

// UpdatePipeInput holds the fields that can be updated on an existing pipe.
type UpdatePipeInput struct {
	SourceParameters        *SourceParameters
	TargetParameters        *TargetParameters
	DeadLetterConfig        *DeadLetterConfig
	LogConfiguration        *LogConfiguration
	EnrichmentParameters    *EnrichmentParameters
	RuntimeMetricsStreaming *RuntimeMetricsStreaming
	Description             *string
	RoleARN                 string
	Target                  string
	Enrichment              string
	KmsKeyIdentifier        string
	DesiredState            string
}

// applyUpdateFields patches the pipe with non-zero values from the update input.
func applyUpdateFields(p *Pipe, in UpdatePipeInput) {
	if in.RoleARN != "" {
		p.RoleARN = in.RoleARN
	}
	if in.Target != "" {
		p.Target = in.Target
	}
	if in.DesiredState != "" {
		p.DesiredState = in.DesiredState
	}
	if in.Enrichment != "" {
		p.Enrichment = in.Enrichment
	}
	if in.KmsKeyIdentifier != "" {
		p.KmsKeyIdentifier = in.KmsKeyIdentifier
	}
	if in.Description != nil {
		p.Description = *in.Description
	}
	if in.SourceParameters != nil {
		p.SourceParameters = in.SourceParameters
	}
	if in.TargetParameters != nil {
		p.TargetParameters = in.TargetParameters
	}
	if in.DeadLetterConfig != nil {
		p.DeadLetterConfig = in.DeadLetterConfig
	}
	if in.LogConfiguration != nil {
		p.LogConfiguration = in.LogConfiguration
	}
	if in.EnrichmentParameters != nil {
		p.EnrichmentParameters = in.EnrichmentParameters
	}
	if in.RuntimeMetricsStreaming != nil {
		p.RuntimeMetricsStreaming = in.RuntimeMetricsStreaming
	}
}

=======
	// Deterministic iteration order: sort pipe names so NextToken is stable.
	names := make([]string, 0, len(b.pipes))
	for name := range b.pipes {
		names = append(names, name)
	}

	// Simple lexicographic sort without importing sort package; use a manual bubble for small N.
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}

	// Apply NextToken offset.
	startIdx := 0
	if f.NextToken != "" {
		decoded, err := base64.StdEncoding.DecodeString(f.NextToken)
		if err == nil {
			cursor := strings.TrimSuffix(string(decoded), nextTokenSep)
			for i, n := range names {
				if n > cursor {
					startIdx = i
					break
				}
				startIdx = len(names)
			}
		}
	}

	var result []*Pipe
	var lastIncluded string

	for i := startIdx; i < len(names); i++ {
		if len(result) >= limit {
			break
		}

		p := b.pipes[names[i]]

		if f.NamePrefix != "" && !strings.HasPrefix(p.Name, f.NamePrefix) {
			continue
		}

		if f.DesiredState != "" && p.DesiredState != f.DesiredState {
			continue
		}

		if f.CurrentState != "" && p.CurrentState != f.CurrentState {
			continue
		}

		if f.SourcePrefix != "" && !strings.HasPrefix(p.Source, f.SourcePrefix) {
			continue
		}

		if f.TargetPrefix != "" && !strings.HasPrefix(p.Target, f.TargetPrefix) {
			continue
		}

		result = append(result, clonePipe(p))
		lastIncluded = p.Name
	}

	var nextToken string

	if len(result) == limit && lastIncluded != "" {
		// Check if there are more matching pipes beyond the limit.
		for i := startIdx + len(result); i < len(names); i++ {
			p := b.pipes[names[i]]
			if matchesFilter(p, f) {
				nextToken = base64.StdEncoding.EncodeToString([]byte(lastIncluded + nextTokenSep))
				break
			}
		}
	}

	return ListPipesResult{Pipes: result, NextToken: nextToken}
}

// matchesFilter reports whether a pipe matches the filter criteria (excluding pagination).
func matchesFilter(p *Pipe, f ListPipesFilter) bool {
	if f.NamePrefix != "" && !strings.HasPrefix(p.Name, f.NamePrefix) {
		return false
	}

	if f.DesiredState != "" && p.DesiredState != f.DesiredState {
		return false
	}

	if f.CurrentState != "" && p.CurrentState != f.CurrentState {
		return false
	}

	if f.SourcePrefix != "" && !strings.HasPrefix(p.Source, f.SourcePrefix) {
		return false
	}

	if f.TargetPrefix != "" && !strings.HasPrefix(p.Target, f.TargetPrefix) {
		return false
	}

	return true
}

// UpdatePipeInput holds the fields that can be updated on an existing pipe.
type UpdatePipeInput struct {
	SourceParameters *SourceParameters
	TargetParameters *TargetParameters
	DeadLetterConfig *DeadLetterConfig
	LogConfiguration *LogConfiguration
	RoleARN          string
	Target           string
	Description      string
	Enrichment       string
	DesiredState     string
}

// UpdatePipe updates an existing pipe.
>>>>>>> Stashed changes
func (b *InMemoryBackend) UpdatePipe(name string, in UpdatePipeInput) (*Pipe, error) {
	if err := validateDesiredState(in.DesiredState); err != nil {
		return nil, err
	}
<<<<<<< Updated upstream
	if err := validateSourceBatchSize(in.SourceParameters); err != nil {
		return nil, err
	}
=======
>>>>>>> Stashed changes

	b.mu.Lock("UpdatePipe")
	defer b.mu.Unlock()

	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}

<<<<<<< Updated upstream
	applyUpdateFields(p, in)

	prevDesiredState := p.DesiredState
	if in.DesiredState != "" {
		prevDesiredState = in.DesiredState
	}
	p.CurrentState = stateUpdating
=======
	if in.RoleARN != "" {
		p.RoleARN = in.RoleARN
	}

	if in.Target != "" {
		p.Target = in.Target
	}

	if in.DesiredState != "" {
		p.DesiredState = in.DesiredState
	}

	if in.Enrichment != "" {
		p.Enrichment = in.Enrichment
	}

	p.Description = in.Description

	if in.SourceParameters != nil {
		p.SourceParameters = in.SourceParameters
	}

	if in.TargetParameters != nil {
		p.TargetParameters = in.TargetParameters
	}

	if in.DeadLetterConfig != nil {
		p.DeadLetterConfig = in.DeadLetterConfig
	}

	if in.LogConfiguration != nil {
		p.LogConfiguration = in.LogConfiguration
	}

>>>>>>> Stashed changes
	p.LastModifiedTime = time.Now()
	cp := clonePipe(p)

	b.runDelayed(func() {
		b.completeUpdateTransition(name, prevDesiredState)
	})

	return cp, nil
}

// completeUpdateTransition moves a pipe from UPDATING to its desired state.
func (b *InMemoryBackend) completeUpdateTransition(name, desiredState string) {
	b.mu.Lock("completeUpdateTransition")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return
	}
	if p.CurrentState == stateUpdating {
		p.CurrentState = desiredState
		p.LastModifiedTime = time.Now()
	}
}

func (b *InMemoryBackend) DeletePipe(name string) (*Pipe, error) {
	b.mu.Lock("DeletePipe")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
	p.CurrentState = stateDeleting
	p.LastModifiedTime = time.Now()
	cp := clonePipe(p)

	b.runDelayed(func() {
		b.completeDeleteTransition(name)
	})

	return cp, nil
}

// completeDeleteTransition removes the pipe after it has been marked DELETING.
func (b *InMemoryBackend) completeDeleteTransition(name string) {
	b.mu.Lock("completeDeleteTransition")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return
	}
	if p.CurrentState == stateDeleting {
		delete(b.pipeARNIndex, p.ARN)
		delete(b.pipes, name)
	}
}

<<<<<<< Updated upstream
=======
// StartPipe transitions a pipe to the RUNNING desired/current state.
>>>>>>> Stashed changes
func (b *InMemoryBackend) StartPipe(name string) (*Pipe, error) {
	b.mu.Lock("StartPipe")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
<<<<<<< Updated upstream
	if p.DesiredState == stateRunning {
		return nil, fmt.Errorf("%w: pipe %s already has desired state RUNNING", ErrValidation, name)
	}
	p.DesiredState = stateRunning
	// Transition through STARTING → RUNNING to simulate AWS behavior.
	p.CurrentState = stateStarting
=======

	if p.DesiredState == stateRunning && p.CurrentState == stateRunning {
		return nil, fmt.Errorf("%w: pipe %s is already in RUNNING state", ErrValidation, name)
	}

	p.DesiredState = stateRunning
	p.CurrentState = stateRunning
>>>>>>> Stashed changes
	p.StateReason = ""
	p.LastModifiedTime = time.Now()
	cp := clonePipe(p)

	// Complete the transition to RUNNING asynchronously.
	b.runDelayed(func() {
		b.completeStartTransition(name)
	})

	return cp, nil
}

// completeStartTransition moves a pipe from STARTING to RUNNING.
func (b *InMemoryBackend) completeStartTransition(name string) {
	b.mu.Lock("completeStartTransition")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return
	}
	if p.CurrentState == stateStarting {
		p.CurrentState = stateRunning
		p.LastModifiedTime = time.Now()
	}
}

<<<<<<< Updated upstream
=======
// StopPipe transitions a pipe to the STOPPED desired/current state.
>>>>>>> Stashed changes
func (b *InMemoryBackend) StopPipe(name string) (*Pipe, error) {
	b.mu.Lock("StopPipe")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
<<<<<<< Updated upstream
	if p.DesiredState == stateStopped {
		return nil, fmt.Errorf("%w: pipe %s already has desired state STOPPED", ErrValidation, name)
	}
	p.DesiredState = stateStopped
	// Transition through STOPPING → STOPPED to simulate AWS behavior.
	p.CurrentState = stateStopping
=======

	if p.DesiredState == stateStopped && p.CurrentState == stateStopped {
		return nil, fmt.Errorf("%w: pipe %s is already in STOPPED state", ErrValidation, name)
	}

	p.DesiredState = stateStopped
	p.CurrentState = stateStopped
>>>>>>> Stashed changes
	p.StateReason = ""
	p.LastModifiedTime = time.Now()
	cp := clonePipe(p)

	// Complete the transition to STOPPED asynchronously.
	b.runDelayed(func() {
		b.completeStopTransition(name)
	})

	return cp, nil
}

// completeStopTransition moves a pipe from STOPPING to STOPPED.
func (b *InMemoryBackend) completeStopTransition(name string) {
	b.mu.Lock("completeStopTransition")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return
	}
	if p.CurrentState == stateStopping {
		p.CurrentState = stateStopped
		p.LastModifiedTime = time.Now()
	}
}

// MarkPipeFailed updates a pipe to a failed state with a reason message.
func (b *InMemoryBackend) MarkPipeFailed(name, state, reason string) {
	b.mu.Lock("MarkPipeFailed")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return
	}
	p.CurrentState = state
	p.StateReason = reason
	p.LastModifiedTime = time.Now()
}

<<<<<<< Updated upstream
=======
// MarkPipeFailed updates a pipe to a failed state with a reason message.
// This is called by the runner when a pipe encounters a persistent error.
func (b *InMemoryBackend) MarkPipeFailed(name, state, reason string) {
	b.mu.Lock("MarkPipeFailed")
	defer b.mu.Unlock()

	p, ok := b.pipes[name]
	if !ok {
		return
	}

	p.CurrentState = state
	p.StateReason = reason
	p.LastModifiedTime = time.Now()
}

// TagResource adds or updates tags on a pipe identified by ARN.
>>>>>>> Stashed changes
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	if err := validateTags(kv); err != nil {
		return err
	}
<<<<<<< Updated upstream
=======

>>>>>>> Stashed changes
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()
	name, ok := b.pipeARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	p := b.pipes[name]
<<<<<<< Updated upstream
=======

>>>>>>> Stashed changes
	merged := mergeTags(p.Tags, kv)
	if len(merged) > maxTagsPerPipe {
		return fmt.Errorf("%w: pipe would exceed %d tags limit", ErrValidation, maxTagsPerPipe)
	}
<<<<<<< Updated upstream
=======

>>>>>>> Stashed changes
	p.Tags = merged

	return nil
}

func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()
	name, ok := b.pipeARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	p := b.pipes[name]
	for _, k := range keys {
		delete(p.Tags, k)
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	name, ok := b.pipeARNIndex[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	p := b.pipes[name]
	result := make(map[string]string, len(p.Tags))
	maps.Copy(result, p.Tags)

	return result, nil
}

func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}

func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.pipes = make(map[string]*Pipe)
	b.pipeARNIndex = make(map[string]string)
	b.enrichmentCallCount = make(map[string]int64)
}

func validatePipeName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: pipe name must not be empty", ErrValidation)
	}
	if len(name) > maxPipeNameLen {
		return fmt.Errorf(
			"%w: pipe name exceeds maximum length of %d characters",
			ErrValidation,
			maxPipeNameLen,
		)
	}
	if !pipeNameRE.MatchString(name) {
		return fmt.Errorf(
			"%w: pipe name %q contains invalid characters (allowed: a-z, A-Z, 0-9, -, _)",
			ErrValidation,
			name,
		)
	}

	return nil
}

func validateDesiredState(state string) error {
	if state == "" || state == stateRunning || state == stateStopped {
		return nil
	}

	return fmt.Errorf("%w: DesiredState must be RUNNING or STOPPED, got %q", ErrValidation, state)
}

func validateTags(tags map[string]string) error {
	for k, v := range tags {
		if len(k) == 0 {
			return fmt.Errorf("%w: tag key must not be empty", ErrValidation)
		}
		if len(k) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key %q exceeds maximum length of %d",
				ErrValidation,
				k,
				maxTagKeyLen,
			)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value for key %q exceeds maximum length of %d",
				ErrValidation,
				k,
				maxTagValueLen,
			)
		}
	}

	return nil
}

func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()
	type snap struct {
		Pipes               map[string]*Pipe `json:"pipes"`
		EnrichmentCallCount map[string]int64 `json:"enrichmentCallCount,omitempty"`
		AccountID           string           `json:"accountID"`
		Region              string           `json:"region"`
	}
	s := snap{
		Pipes:               b.pipes,
		AccountID:           b.accountID,
		Region:              b.region,
		EnrichmentCallCount: b.enrichmentCallCount,
	}
	data, err := json.Marshal(s)
	if err != nil {
		return nil
	}

	return data
}

func (b *InMemoryBackend) Restore(data []byte) error {
	type snap struct {
		Pipes               map[string]*Pipe `json:"pipes"`
		EnrichmentCallCount map[string]int64 `json:"enrichmentCallCount,omitempty"`
		AccountID           string           `json:"accountID"`
		Region              string           `json:"region"`
	}
	var s snap
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	b.mu.Lock("Restore")
	defer b.mu.Unlock()
	if s.Pipes == nil {
		s.Pipes = make(map[string]*Pipe)
	}
	b.pipes = s.Pipes
	b.accountID = s.AccountID
	b.region = s.Region
	b.pipeARNIndex = make(map[string]string, len(b.pipes))
	for name, p := range b.pipes {
		b.pipeARNIndex[p.ARN] = name
	}
	if s.EnrichmentCallCount != nil {
		b.enrichmentCallCount = s.EnrichmentCallCount
	} else {
		b.enrichmentCallCount = make(map[string]int64)
	}

	return nil
}

// --- Validation ---

func validatePipeName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: pipe name must not be empty", ErrValidation)
	}

	if len(name) > maxPipeNameLen {
		return fmt.Errorf("%w: pipe name exceeds maximum length of %d characters", ErrValidation, maxPipeNameLen)
	}

	if !pipeNameRE.MatchString(name) {
		return fmt.Errorf("%w: pipe name %q contains invalid characters (allowed: a-z, A-Z, 0-9, -, _)", ErrValidation, name)
	}

	return nil
}

func validateDesiredState(state string) error {
	if state == "" || state == stateRunning || state == stateStopped {
		return nil
	}

	return fmt.Errorf("%w: DesiredState must be RUNNING or STOPPED, got %q", ErrValidation, state)
}

func validateTags(tags map[string]string) error {
	for k, v := range tags {
		if len(k) == 0 {
			return fmt.Errorf("%w: tag key must not be empty", ErrValidation)
		}

		if len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key %q exceeds maximum length of %d", ErrValidation, k, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value for key %q exceeds maximum length of %d", ErrValidation, k, maxTagValueLen)
		}
	}

	return nil
}

// cloneSourceParameters returns a deep copy of SourceParameters for snapshot serialisation.
func cloneSourceParameters(sp *SourceParameters) *SourceParameters {
	if sp == nil {
		return nil
	}

	out := &SourceParameters{}

	if sp.FilterCriteria != nil {
		fc := &FilterCriteria{
			Filters: append([]Filter(nil), sp.FilterCriteria.Filters...),
		}
		out.FilterCriteria = fc
	}

	if sp.SqsQueueParameters != nil {
		sqsp := *sp.SqsQueueParameters
		out.SqsQueueParameters = &sqsp
	}

	return out
}

// pipeResponseJSON encodes a pipe snapshot for the Snapshot/Restore persistence path.
// This mirrors the Pipe struct but uses exported names for clarity.
type pipeSnapshot struct {
	SourceParameters *SourceParameters `json:"sourceParameters,omitempty"`
	TargetParameters *TargetParameters `json:"targetParameters,omitempty"`
	DeadLetterConfig *DeadLetterConfig `json:"deadLetterConfig,omitempty"`
	LogConfiguration *LogConfiguration `json:"logConfiguration,omitempty"`
	LastModifiedTime time.Time         `json:"lastModifiedTime"`
	CreationTime     time.Time         `json:"creationTime"`
	Tags             map[string]string `json:"tags,omitempty"`
	Description      string            `json:"description,omitempty"`
	Enrichment       string            `json:"enrichment,omitempty"`
	Source           string            `json:"source"`
	Target           string            `json:"target"`
	RoleARN          string            `json:"roleArn"`
	StateReason      string            `json:"stateReason,omitempty"`
	DesiredState     string            `json:"desiredState"`
	CurrentState     string            `json:"currentState"`
	AccountID        string            `json:"accountID"`
	Region           string            `json:"region"`
	ARN              string            `json:"arn"`
	Name             string            `json:"name"`
}

// marshalPipeForSnapshot converts a Pipe to JSON-serialisable form.
func marshalPipeForSnapshot(p *Pipe) pipeSnapshot {
	return pipeSnapshot{
		Name:             p.Name,
		ARN:              p.ARN,
		RoleARN:          p.RoleARN,
		Source:           p.Source,
		Target:           p.Target,
		Description:      p.Description,
		Enrichment:       p.Enrichment,
		DesiredState:     p.DesiredState,
		CurrentState:     p.CurrentState,
		StateReason:      p.StateReason,
		AccountID:        p.AccountID,
		Region:           p.Region,
		CreationTime:     p.CreationTime,
		LastModifiedTime: p.LastModifiedTime,
		Tags:             p.Tags,
		SourceParameters: cloneSourceParameters(p.SourceParameters),
		TargetParameters: p.TargetParameters,
		DeadLetterConfig: p.DeadLetterConfig,
		LogConfiguration: p.LogConfiguration,
	}
}

// restorePipeFromSnapshot converts a pipeSnapshot back to a Pipe.
func restorePipeFromSnapshot(s pipeSnapshot) *Pipe {
	return &Pipe{
		Name:             s.Name,
		ARN:              s.ARN,
		RoleARN:          s.RoleARN,
		Source:           s.Source,
		Target:           s.Target,
		Description:      s.Description,
		Enrichment:       s.Enrichment,
		DesiredState:     s.DesiredState,
		CurrentState:     s.CurrentState,
		StateReason:      s.StateReason,
		AccountID:        s.AccountID,
		Region:           s.Region,
		CreationTime:     s.CreationTime,
		LastModifiedTime: s.LastModifiedTime,
		Tags:             s.Tags,
		SourceParameters: s.SourceParameters,
		TargetParameters: s.TargetParameters,
		DeadLetterConfig: s.DeadLetterConfig,
		LogConfiguration: s.LogConfiguration,
	}
}

// snapshotV2 is the versioned snapshot format used since the addition of extended fields.
type snapshotV2 struct {
	Pipes     map[string]pipeSnapshot `json:"pipes"`
	AccountID string                  `json:"accountID"`
	Region    string                  `json:"region"`
	Version   int                     `json:"version"`
}

// Snapshot serialises the backend state to JSON (used by persistence layer).
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := snapshotV2{
		Version:   2,
		AccountID: b.accountID,
		Region:    b.region,
		Pipes:     make(map[string]pipeSnapshot, len(b.pipes)),
	}

	for name, p := range b.pipes {
		snap.Pipes[name] = marshalPipeForSnapshot(p)
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap snapshotV2

	if err := json.Unmarshal(data, &snap); err != nil {
		// Attempt legacy v1 restore.
		return b.restoreLegacy(data)
	}

	if snap.Version == 0 {
		return b.restoreLegacy(data)
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.pipes = make(map[string]*Pipe, len(snap.Pipes))
	b.pipeARNIndex = make(map[string]string, len(snap.Pipes))
	b.accountID = snap.AccountID
	b.region = snap.Region

	for name, ps := range snap.Pipes {
		p := restorePipeFromSnapshot(ps)
		b.pipes[name] = p
		b.pipeARNIndex[p.ARN] = name
	}

	return nil
}

// restoreLegacy handles v1 snapshots that used the old Pipe struct directly.
func (b *InMemoryBackend) restoreLegacy(data []byte) error {
	type legacySnap struct {
		Pipes     map[string]*Pipe `json:"pipes"`
		AccountID string           `json:"accountID"`
		Region    string           `json:"region"`
	}

	var snap legacySnap
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("RestoreLegacy")
	defer b.mu.Unlock()

	if snap.Pipes == nil {
		snap.Pipes = make(map[string]*Pipe)
	}

	b.pipes = snap.Pipes
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.pipeARNIndex = make(map[string]string, len(b.pipes))

	for name, p := range b.pipes {
		b.pipeARNIndex[p.ARN] = name
	}

	return nil
}

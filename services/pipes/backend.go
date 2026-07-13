package pipes

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	stateRunning      = "RUNNING"
	stateStopped      = "STOPPED"
	stateCreating     = "CREATING"
	stateUpdating     = "UPDATING"
	stateDeleting     = "DELETING"
	stateStarting     = "STARTING"
	stateStopping     = "STOPPING"
	stateCreateFailed = "CREATE_FAILED"
	stateUpdateFailed = "UPDATE_FAILED"
	stateDeleteFailed = "DELETE_FAILED"
	stateStartFailed  = "START_FAILED"
	stateStopFailed   = "STOP_FAILED"

	// stateTransitionDelay is the simulated delay for async state transitions.
	stateTransitionDelay = 10 * time.Millisecond

	maxPipeNameLen  = 64
	maxTagKeyLen    = 128
	maxTagValueLen  = 256
	maxTagsPerPipe  = 50
	maxPipesPerAcct = 1000

	// nextTokenSep separates cursor values in pagination tokens.
	nextTokenSep = "\x00"
)

var (
	ErrNotFound      = awserr.New("NotFoundException", awserr.ErrNotFound)
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	ErrValidation    = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	ErrConflict      = awserr.New("ConflictException", awserr.ErrConflict)
	ErrQuota         = awserr.New("ServiceQuotaExceededException", awserr.ErrConflict)

	pipeNameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// Pipes resources are isolated per region: every backend operation resolves the
// caller's region from the request context and operates only on that region's
// nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// FilterCriteria holds event filter patterns applied before forwarding to the target.
type FilterCriteria struct {
	Filters []Filter `json:"Filters,omitempty"`
}

// Filter is a single JSON-pattern filter.
type Filter struct {
	Pattern string `json:"Pattern,omitempty"`
}

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

// SQSSourceParameters holds SQS-specific source configuration.
type SQSSourceParameters struct {
	BatchSize                      int `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int `json:"MaximumBatchingWindowInSeconds,omitempty"`
}

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
}

// DeadLetterConfig identifies the DLQ for failed pipe events.
type DeadLetterConfig struct {
	Arn string `json:"Arn,omitempty"`
}

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
type CloudwatchLogsLogDestination struct {
	LogGroupArn string `json:"LogGroupArn,omitempty"`
}

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
}

// LogConfiguration controls pipe execution logging.
type LogConfiguration struct {
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

	return &cp
}

// InMemoryBackend is the in-memory store for pipes.
//
// All resource collections are nested by region (outer key = region) so that
// same-named pipes are isolated across regions. The per-region *store.Table /
// *store.Index values are created lazily via the pipesTable /
// pipesByARNIndex / enrichmentCountsTable accessors in store_setup.go —
// mirroring the lazy map creation the hand-rolled code did before Phase 3.3's
// pkgs/store conversion. Callers must hold b.mu while accessing them.
type InMemoryBackend struct {
	svcCtx context.Context
	// registry is the lifecycle registry for every *store.Table this backend
	// owns; Reset/Snapshot/Restore drive it via ResetAll/SnapshotAll/RestoreAll
	// instead of hand-written per-map boilerplate. See store_setup.go.
	registry *store.Registry
	// pipes holds one *store.Table[Pipe] per region, keyed by pipe name.
	pipes map[string]*store.Table[Pipe]
	// pipesByARN is a secondary index on pipes, keyed by ARN, one per region.
	// It replaces the hand-rolled pipeARNIndex map and is kept consistent
	// automatically by store.Table.Put/Delete/Restore.
	pipesByARN map[string]*store.Index[Pipe]
	// enrichmentCounts holds one *store.Table[enrichmentCounter] per region,
	// keyed by pipe name. enrichmentCounter is an identity-less DTO (a plain
	// int64 counter) wrapped so it can be keyed by store.Table.
	enrichmentCounts map[string]*store.Table[enrichmentCounter]
	mu               *lockmetrics.RWMutex
	cancel           context.CancelFunc
	accountID        string
	region           string
	wg               sync.WaitGroup
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
		registry:         store.NewRegistry(),
		pipes:            make(map[string]*store.Table[Pipe]),
		pipesByARN:       make(map[string]*store.Index[Pipe]),
		enrichmentCounts: make(map[string]*store.Table[enrichmentCounter]),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("pipes"),
		svcCtx:           ctx,
		cancel:           cancel,
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
func (b *InMemoryBackend) RecordEnrichmentCall(ctx context.Context, pipeName string) {
	b.mu.Lock("RecordEnrichmentCall")
	defer b.mu.Unlock()

	t := b.enrichmentCountsTable(getRegion(ctx, b.region))

	var count int64
	if c, ok := t.Get(pipeName); ok {
		count = c.Count
	}

	t.Put(&enrichmentCounter{Name: pipeName, Count: count + 1})
}

// GetEnrichmentCallCount returns the number of enrichment calls for a pipe.
func (b *InMemoryBackend) GetEnrichmentCallCount(ctx context.Context, pipeName string) int64 {
	b.mu.RLock("GetEnrichmentCallCount")
	defer b.mu.RUnlock()

	c, ok := b.enrichmentCountsTable(getRegion(ctx, b.region)).Get(pipeName)
	if !ok {
		return 0
	}

	return c.Count
}

func (b *InMemoryBackend) Region() string { return b.region }

// CreatePipeInput holds the full set of fields for pipe creation.
type CreatePipeInput struct {
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

func (b *InMemoryBackend) CreatePipe(ctx context.Context, in CreatePipeInput) (*Pipe, error) {
	if err := validatePipeName(in.Name); err != nil {
		return nil, err
	}
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
	if len(in.Tags) > maxTagsPerPipe {
		return nil, fmt.Errorf("%w: pipe would exceed %d tags limit", ErrValidation, maxTagsPerPipe)
	}
	if err := validateSourceBatchSize(in.SourceParameters); err != nil {
		return nil, err
	}

	b.mu.Lock("CreatePipe")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	pipesTable := b.pipesTable(region)

	if pipesTable.Len() >= maxPipesPerAcct {
		return nil, fmt.Errorf(
			"%w: account has reached the maximum number of pipes (%d)",
			ErrQuota,
			maxPipesPerAcct,
		)
	}
	if pipesTable.Has(in.Name) {
		return nil, fmt.Errorf("%w: pipe %s already exists", ErrAlreadyExists, in.Name)
	}
	if in.DesiredState == "" {
		in.DesiredState = stateRunning
	}

	now := time.Now()
	pipeARN := arn.Build("pipes", region, b.accountID, "pipe/"+in.Name)
	p := &Pipe{
		Name: in.Name, ARN: pipeARN, RoleARN: in.RoleARN,
		Source: in.Source, Target: in.Target, Description: in.Description,
		Enrichment: in.Enrichment, KmsKeyIdentifier: in.KmsKeyIdentifier,
		DesiredState: in.DesiredState, CurrentState: stateCreating,
		AccountID: b.accountID, Region: region,
		CreationTime: now, LastModifiedTime: now,
		Tags:                    mergeTags(nil, in.Tags),
		SourceParameters:        in.SourceParameters,
		TargetParameters:        in.TargetParameters,
		DeadLetterConfig:        in.DeadLetterConfig,
		LogConfiguration:        in.LogConfiguration,
		EnrichmentParameters:    in.EnrichmentParameters,
		RuntimeMetricsStreaming: in.RuntimeMetricsStreaming,
	}
	pipesTable.Put(p)

	cp := clonePipe(p)
	b.runDelayed(func() {
		b.completeCreateTransition(region, in.Name, in.DesiredState)
	})

	return cp, nil
}

// completeCreateTransition moves a pipe from CREATING to its desired state.
func (b *InMemoryBackend) completeCreateTransition(region, name, desiredState string) {
	b.mu.Lock("completeCreateTransition")
	defer b.mu.Unlock()
	p, ok := b.pipesTable(region).Get(name)
	if !ok {
		return
	}
	if p.CurrentState == stateCreating {
		p.CurrentState = desiredState
		p.LastModifiedTime = time.Now()
	}
}

func (b *InMemoryBackend) GetPipe(ctx context.Context, name string) (*Pipe, error) {
	b.mu.RLock("GetPipe")
	defer b.mu.RUnlock()
	p, ok := b.pipesTable(getRegion(ctx, b.region)).Get(name)
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
	NextToken string
	Pipes     []*Pipe
}

func (b *InMemoryBackend) ListPipes(ctx context.Context, f ListPipesFilter) (ListPipesResult, error) {
	b.mu.RLock("ListPipes")
	defer b.mu.RUnlock()

	pipesTable := b.pipesTable(getRegion(ctx, b.region))

	limit := f.Limit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

	names := sortedPipeNames(pipesTable)

	startIdx, err := resolveStartIndex(names, f.NextToken)
	if err != nil {
		return ListPipesResult{}, err
	}

	result, lastIncluded := collectMatchingPipes(pipesTable, names, startIdx, limit, f)
	nextToken := buildNextToken(pipesTable, names, startIdx, len(result), limit, lastIncluded, f)

	return ListPipesResult{Pipes: result, NextToken: nextToken}, nil
}

// allRunningPipes returns all RUNNING pipes across every region. Used by the
// background runner which must poll all regions without a request context.
func (b *InMemoryBackend) allRunningPipes() []*Pipe {
	b.mu.RLock("allRunningPipes")
	defer b.mu.RUnlock()

	var result []*Pipe

	for _, regionTable := range b.pipes {
		for _, p := range regionTable.All() {
			if p.CurrentState == stateRunning {
				result = append(result, clonePipe(p))
			}
		}
	}

	return result
}

func sortedPipeNames(pipesTable *store.Table[Pipe]) []string {
	names := make([]string, 0, pipesTable.Len())
	for _, p := range pipesTable.All() {
		names = append(names, p.Name)
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

func resolveStartIndex(names []string, nextToken string) (int, error) {
	if nextToken == "" {
		return 0, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(nextToken)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid NextToken", ErrValidation)
	}
	cursor := strings.TrimSuffix(string(decoded), nextTokenSep)
	startIdx := len(names)
	for i, n := range names {
		if n > cursor {
			startIdx = i

			break
		}
	}

	return startIdx, nil
}

func collectMatchingPipes(
	pipesTable *store.Table[Pipe],
	names []string, startIdx, limit int, f ListPipesFilter,
) ([]*Pipe, string) {
	var result []*Pipe
	var lastIncluded string
	for i := startIdx; i < len(names); i++ {
		if len(result) >= limit {
			break
		}
		p, _ := pipesTable.Get(names[i])
		if !matchesFilter(p, f) {
			continue
		}
		result = append(result, clonePipe(p))
		lastIncluded = p.Name
	}

	return result, lastIncluded
}

func buildNextToken(
	pipesTable *store.Table[Pipe],
	names []string, startIdx, resultLen, limit int, lastIncluded string, f ListPipesFilter,
) string {
	if resultLen < limit || lastIncluded == "" {
		return ""
	}
	for i := startIdx + resultLen; i < len(names); i++ {
		p, _ := pipesTable.Get(names[i])
		if matchesFilter(p, f) {
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

func (b *InMemoryBackend) UpdatePipe(ctx context.Context, name string, in UpdatePipeInput) (*Pipe, error) {
	if err := validateDesiredState(in.DesiredState); err != nil {
		return nil, err
	}
	if err := validateSourceBatchSize(in.SourceParameters); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdatePipe")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	p, ok := b.pipesTable(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
	if p.CurrentState == stateDeleting {
		return nil, fmt.Errorf(
			"%w: pipe %s is being deleted",
			ErrConflict, name,
		)
	}
	applyUpdateFields(p, in)

	prevDesiredState := p.DesiredState
	if in.DesiredState != "" {
		prevDesiredState = in.DesiredState
	}
	p.CurrentState = stateUpdating
	p.LastModifiedTime = time.Now()
	cp := clonePipe(p)

	b.runDelayed(func() {
		b.completeUpdateTransition(region, name, prevDesiredState)
	})

	return cp, nil
}

// completeUpdateTransition moves a pipe from UPDATING to its desired state.
func (b *InMemoryBackend) completeUpdateTransition(region, name, desiredState string) {
	b.mu.Lock("completeUpdateTransition")
	defer b.mu.Unlock()
	p, ok := b.pipesTable(region).Get(name)
	if !ok {
		return
	}
	if p.CurrentState == stateUpdating {
		p.CurrentState = desiredState
		p.LastModifiedTime = time.Now()
	}
}

func (b *InMemoryBackend) DeletePipe(ctx context.Context, name string) (*Pipe, error) {
	b.mu.Lock("DeletePipe")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	p, ok := b.pipesTable(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
	if p.CurrentState == stateDeleting {
		return nil, fmt.Errorf(
			"%w: pipe %s is already being deleted",
			ErrConflict, name,
		)
	}
	p.CurrentState = stateDeleting
	p.LastModifiedTime = time.Now()
	cp := clonePipe(p)

	b.runDelayed(func() {
		b.completeDeleteTransition(region, name)
	})

	return cp, nil
}

// completeDeleteTransition removes the pipe after it has been marked DELETING.
func (b *InMemoryBackend) completeDeleteTransition(region, name string) {
	b.mu.Lock("completeDeleteTransition")
	defer b.mu.Unlock()
	pipesTable := b.pipesTable(region)
	p, ok := pipesTable.Get(name)
	if !ok {
		return
	}
	if p.CurrentState == stateDeleting {
		// Delete also removes p from the ARN secondary index automatically.
		pipesTable.Delete(name)
		// Prune the per-pipe enrichment counter to prevent unbounded growth.
		if ct, exists := b.enrichmentCounts[region]; exists {
			ct.Delete(name)
		}
	}
}

// pipeDesiredStateOpts parameterises the common logic of StartPipe and StopPipe.
type pipeDesiredStateOpts struct {
	completeAfter func(region, name string)
	lockName      string
	blockedState  string
	desiredState  string
	transitState  string
}

// changePipeDesiredState is the shared body of StartPipe and StopPipe.
func (b *InMemoryBackend) changePipeDesiredState(
	ctx context.Context,
	name string,
	opts pipeDesiredStateOpts,
) (*Pipe, error) {
	b.mu.Lock(opts.lockName)
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	p, ok := b.pipesTable(region).Get(name)

	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}

	if p.CurrentState == opts.blockedState || p.CurrentState == stateDeleting {
		return nil, fmt.Errorf(
			"%w: pipe %s is in a transitional state %s",
			ErrConflict, name, p.CurrentState,
		)
	}

	if p.DesiredState == opts.desiredState {
		return nil, fmt.Errorf(
			"%w: pipe %s already has desired state %s",
			ErrConflict, name, opts.desiredState,
		)
	}

	p.DesiredState = opts.desiredState
	p.CurrentState = opts.transitState
	p.StateReason = ""
	p.LastModifiedTime = time.Now()
	cp := clonePipe(p)

	b.runDelayed(func() {
		opts.completeAfter(region, name)
	})

	return cp, nil
}

func (b *InMemoryBackend) StartPipe(ctx context.Context, name string) (*Pipe, error) {
	return b.changePipeDesiredState(ctx, name, pipeDesiredStateOpts{
		lockName:      "StartPipe",
		blockedState:  stateStarting,
		desiredState:  stateRunning,
		transitState:  stateStarting,
		completeAfter: b.completeStartTransition,
	})
}

// completeStartTransition moves a pipe from STARTING to RUNNING.
func (b *InMemoryBackend) completeStartTransition(region, name string) {
	b.mu.Lock("completeStartTransition")
	defer b.mu.Unlock()
	p, ok := b.pipesTable(region).Get(name)
	if !ok {
		return
	}
	if p.CurrentState == stateStarting {
		p.CurrentState = stateRunning
		p.LastModifiedTime = time.Now()
	}
}

func (b *InMemoryBackend) StopPipe(ctx context.Context, name string) (*Pipe, error) {
	return b.changePipeDesiredState(ctx, name, pipeDesiredStateOpts{
		lockName:      "StopPipe",
		blockedState:  stateStopping,
		desiredState:  stateStopped,
		transitState:  stateStopping,
		completeAfter: b.completeStopTransition,
	})
}

// completeStopTransition moves a pipe from STOPPING to STOPPED.
func (b *InMemoryBackend) completeStopTransition(region, name string) {
	b.mu.Lock("completeStopTransition")
	defer b.mu.Unlock()
	p, ok := b.pipesTable(region).Get(name)
	if !ok {
		return
	}
	if p.CurrentState == stateStopping {
		p.CurrentState = stateStopped
		p.LastModifiedTime = time.Now()
	}
}

// MarkPipeFailed updates a pipe to a failed state with a reason message.
// It searches all regions for the named pipe.
func (b *InMemoryBackend) MarkPipeFailed(name, state, reason string) {
	b.mu.Lock("MarkPipeFailed")
	defer b.mu.Unlock()

	for _, regionTable := range b.pipes {
		if p, ok := regionTable.Get(name); ok {
			p.CurrentState = state
			p.StateReason = reason
			p.LastModifiedTime = time.Now()

			return
		}
	}
}

func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, kv map[string]string) error {
	if err := validateTags(kv); err != nil {
		return err
	}
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	p, ok := b.pipeByARN(region, resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	merged := mergeTags(p.Tags, kv)
	if len(merged) > maxTagsPerPipe {
		return fmt.Errorf("%w: pipe would exceed %d tags limit", ErrValidation, maxTagsPerPipe)
	}
	p.Tags = merged

	return nil
}

func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	p, ok := b.pipeByARN(region, resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	for _, k := range keys {
		delete(p.Tags, k)
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	p, ok := b.pipeByARN(region, resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	result := make(map[string]string, len(p.Tags))
	maps.Copy(result, p.Tags)

	return result, nil
}

// pipeByARN looks up a pipe by ARN within region via the pipesByARN secondary
// index, returning (nil, false) if no pipe in that region has that ARN. ARNs
// are unique per pipe, so the index group has at most one entry.
func (b *InMemoryBackend) pipeByARN(region, resourceARN string) (*Pipe, bool) {
	list := b.pipesByARNIndex(region).Get(resourceARN)
	if len(list) == 0 {
		return nil, false
	}

	return list[0], true
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
	b.registry.ResetAll()
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

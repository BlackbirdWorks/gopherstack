package pipes

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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

	pipeNameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

// FilterCriteria holds event filter patterns applied before forwarding to the target.
type FilterCriteria struct {
	Filters []Filter `json:"Filters,omitempty"`
}

// Filter is a single JSON-pattern filter.
type Filter struct {
	Pattern string `json:"Pattern,omitempty"`
}

// SQSSourceParameters holds SQS-specific source configuration.
type SQSSourceParameters struct {
	BatchSize                      int `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int `json:"MaximumBatchingWindowInSeconds,omitempty"`
}

// KinesisStreamSourceParameters holds Kinesis-specific source configuration.
type KinesisStreamSourceParameters struct {
	StartingPosition               string            `json:"StartingPosition,omitempty"`
	StartingPositionTimestamp      *time.Time        `json:"StartingPositionTimestamp,omitempty"`
	BatchSize                      int               `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int               `json:"MaximumBatchingWindowInSeconds,omitempty"`
	MaximumRecordAgeInSeconds      int               `json:"MaximumRecordAgeInSeconds,omitempty"`
	MaximumRetryAttempts           int               `json:"MaximumRetryAttempts,omitempty"`
	OnPartialBatchItemFailure      string            `json:"OnPartialBatchItemFailure,omitempty"`
	ParallelizationFactor          int               `json:"ParallelizationFactor,omitempty"`
	DeadLetterConfig               *DeadLetterConfig `json:"DeadLetterConfig,omitempty"`
}

// DynamoDBStreamSourceParameters holds DynamoDB stream source configuration.
type DynamoDBStreamSourceParameters struct {
	StartingPosition               string            `json:"StartingPosition,omitempty"`
	BatchSize                      int               `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int               `json:"MaximumBatchingWindowInSeconds,omitempty"`
	MaximumRecordAgeInSeconds      int               `json:"MaximumRecordAgeInSeconds,omitempty"`
	MaximumRetryAttempts           int               `json:"MaximumRetryAttempts,omitempty"`
	OnPartialBatchItemFailure      string            `json:"OnPartialBatchItemFailure,omitempty"`
	ParallelizationFactor          int               `json:"ParallelizationFactor,omitempty"`
	DeadLetterConfig               *DeadLetterConfig `json:"DeadLetterConfig,omitempty"`
}

// ManagedStreamingKafkaSourceParameters holds MSK source configuration.
type ManagedStreamingKafkaSourceParameters struct {
	TopicName                      string `json:"TopicName,omitempty"`
	StartingPosition               string `json:"StartingPosition,omitempty"`
	BatchSize                      int    `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int    `json:"MaximumBatchingWindowInSeconds,omitempty"`
	ConsumerGroupID                string `json:"ConsumerGroupId,omitempty"`
}

// SelfManagedKafkaSourceParameters holds self-managed Kafka source configuration.
type SelfManagedKafkaSourceParameters struct {
	TopicName                      string   `json:"TopicName,omitempty"`
	StartingPosition               string   `json:"StartingPosition,omitempty"`
	BatchSize                      int      `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int      `json:"MaximumBatchingWindowInSeconds,omitempty"`
	ConsumerGroupID                string   `json:"ConsumerGroupId,omitempty"`
	AdditionalBootstrapServers     []string `json:"AdditionalBootstrapServers,omitempty"`
	ServerRootCaCertificate        string   `json:"ServerRootCaCertificate,omitempty"`
}

// RabbitMQBrokerSourceParameters holds RabbitMQ broker source configuration.
type RabbitMQBrokerSourceParameters struct {
	QueueName                      string `json:"QueueName,omitempty"`
	VirtualHost                    string `json:"VirtualHost,omitempty"`
	BatchSize                      int    `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int    `json:"MaximumBatchingWindowInSeconds,omitempty"`
}

// ActiveMQBrokerSourceParameters holds ActiveMQ broker source configuration.
type ActiveMQBrokerSourceParameters struct {
	QueueName                      string `json:"QueueName,omitempty"`
	BatchSize                      int    `json:"BatchSize,omitempty"`
	MaximumBatchingWindowInSeconds int    `json:"MaximumBatchingWindowInSeconds,omitempty"`
}

// SourceParameters holds source-specific configuration.
type SourceParameters struct {
	FilterCriteria                  *FilterCriteria                        `json:"FilterCriteria,omitempty"`
	SqsQueueParameters              *SQSSourceParameters                   `json:"SqsQueueParameters,omitempty"`
	KinesisStreamParameters         *KinesisStreamSourceParameters         `json:"KinesisStreamParameters,omitempty"`
	DynamoDBStreamParameters        *DynamoDBStreamSourceParameters        `json:"DynamoDBStreamParameters,omitempty"`
	ManagedStreamingKafkaParameters *ManagedStreamingKafkaSourceParameters `json:"ManagedStreamingKafkaParameters,omitempty"`
	SelfManagedKafkaParameters      *SelfManagedKafkaSourceParameters      `json:"SelfManagedKafkaParameters,omitempty"`
	RabbitMQBrokerParameters        *RabbitMQBrokerSourceParameters        `json:"RabbitMQBrokerParameters,omitempty"`
	ActiveMQBrokerParameters        *ActiveMQBrokerSourceParameters        `json:"ActiveMQBrokerParameters,omitempty"`
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
	MessageGroupId         string `json:"MessageGroupId,omitempty"`
	MessageDeduplicationId string `json:"MessageDeduplicationId,omitempty"`
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

// EventBridgeEventBusTargetParameters holds EventBridge event bus target configuration.
type EventBridgeEventBusTargetParameters struct {
	DetailType string   `json:"DetailType,omitempty"`
	EndpointId string   `json:"EndpointId,omitempty"`
	Resources  []string `json:"Resources,omitempty"`
	Source     string   `json:"Source,omitempty"`
	Time       string   `json:"Time,omitempty"`
}

// RedshiftDataTargetParameters holds Redshift Data API target configuration.
type RedshiftDataTargetParameters struct {
	Database         string   `json:"Database,omitempty"`
	DbUser           string   `json:"DbUser,omitempty"`
	SecretManagerArn string   `json:"SecretManagerArn,omitempty"`
	Sqls             []string `json:"Sqls,omitempty"`
	StatementName    string   `json:"StatementName,omitempty"`
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
	JobDefinition   string               `json:"JobDefinition,omitempty"`
	JobName         string               `json:"JobName,omitempty"`
	ArrayProperties *BatchArrayProperties `json:"ArrayProperties,omitempty"`
	RetryStrategy   *BatchRetryStrategy  `json:"RetryStrategy,omitempty"`
	Parameters      map[string]string    `json:"Parameters,omitempty"`
}

// ECSTaskTargetParameters holds ECS task target configuration.
type ECSTaskTargetParameters struct {
	TaskDefinitionArn string `json:"TaskDefinitionArn,omitempty"`
	TaskCount         int    `json:"TaskCount,omitempty"`
	LaunchType        string `json:"LaunchType,omitempty"`
}

// TargetParameters holds target-specific configuration.
type TargetParameters struct {
	LambdaFunctionParameters           *LambdaFunctionParameters            `json:"LambdaFunctionParameters,omitempty"`
	StepFunctionStateMachineParameters *StepFunctionTargetParameters        `json:"StepFunctionStateMachineParameters,omitempty"`
	SqsQueueParameters                 *SQSTargetParameters                 `json:"SqsQueueParameters,omitempty"`
	KinesisStreamParameters            *KinesisStreamTargetParameters       `json:"KinesisStreamParameters,omitempty"`
	CloudWatchLogsParameters           *CloudWatchLogsTargetParameters      `json:"CloudWatchLogsParameters,omitempty"`
	EventBridgeEventBusParameters      *EventBridgeEventBusTargetParameters `json:"EventBridgeEventBusParameters,omitempty"`
	RedshiftDataParameters             *RedshiftDataTargetParameters        `json:"RedshiftDataParameters,omitempty"`
	SageMakerPipelineParameters        *SageMakerPipelineTargetParameters   `json:"SageMakerPipelineParameters,omitempty"`
	BatchJobParameters                 *BatchJobTargetParameters            `json:"BatchJobParameters,omitempty"`
	EcsTaskParameters                  *ECSTaskTargetParameters             `json:"EcsTaskParameters,omitempty"`
	InputTemplate                      string                               `json:"InputTemplate,omitempty"`
}

// DeadLetterConfig identifies the DLQ for failed pipe events.
type DeadLetterConfig struct {
	Arn string `json:"Arn,omitempty"`
}

// EnrichmentHTTPParameters holds HTTP parameters for enrichment calls.
type EnrichmentHTTPParameters struct {
	HeaderParameters      map[string]string `json:"HeaderParameters,omitempty"`
	PathParameterValues   []string          `json:"PathParameterValues,omitempty"`
	QueryStringParameters map[string]string `json:"QueryStringParameters,omitempty"`
}

// EnrichmentParameters holds enrichment-specific configuration.
type EnrichmentParameters struct {
	InputTemplate  string                    `json:"InputTemplate,omitempty"`
	HttpParameters *EnrichmentHTTPParameters `json:"HttpParameters,omitempty"`
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
	SourceParameters     *SourceParameters     `json:"sourceParameters,omitempty"`
	TargetParameters     *TargetParameters     `json:"targetParameters,omitempty"`
	DeadLetterConfig     *DeadLetterConfig     `json:"deadLetterConfig,omitempty"`
	LogConfiguration     *LogConfiguration     `json:"logConfiguration,omitempty"`
	EnrichmentParameters *EnrichmentParameters `json:"enrichmentParameters,omitempty"`
	LastModifiedTime     time.Time             `json:"lastModifiedTime"`
	CreationTime         time.Time             `json:"creationTime"`
	Tags                 map[string]string     `json:"tags,omitempty"`
	Description          string                `json:"description,omitempty"`
	Enrichment           string                `json:"enrichment,omitempty"`
	KmsKeyIdentifier     string                `json:"kmsKeyIdentifier,omitempty"`
	Source               string                `json:"source"`
	Target               string                `json:"target"`
	RoleARN              string                `json:"roleArn"`
	StateReason          string                `json:"stateReason,omitempty"`
	DesiredState         string                `json:"desiredState"`
	CurrentState         string                `json:"currentState"`
	AccountID            string                `json:"accountID"`
	Region               string                `json:"region"`
	ARN                  string                `json:"arn"`
	Name                 string                `json:"name"`
}

func (p *Pipe) effectiveBatchSize() int {
	if sp := p.SourceParameters; sp != nil {
		if sp.SqsQueueParameters != nil && sp.SqsQueueParameters.BatchSize > 0 {
			return sp.SqsQueueParameters.BatchSize
		}
		if sp.KinesisStreamParameters != nil && sp.KinesisStreamParameters.BatchSize > 0 {
			return sp.KinesisStreamParameters.BatchSize
		}
		if sp.DynamoDBStreamParameters != nil && sp.DynamoDBStreamParameters.BatchSize > 0 {
			return sp.DynamoDBStreamParameters.BatchSize
		}
		if sp.ManagedStreamingKafkaParameters != nil && sp.ManagedStreamingKafkaParameters.BatchSize > 0 {
			return sp.ManagedStreamingKafkaParameters.BatchSize
		}
		if sp.SelfManagedKafkaParameters != nil && sp.SelfManagedKafkaParameters.BatchSize > 0 {
			return sp.SelfManagedKafkaParameters.BatchSize
		}
		if sp.RabbitMQBrokerParameters != nil && sp.RabbitMQBrokerParameters.BatchSize > 0 {
			return sp.RabbitMQBrokerParameters.BatchSize
		}
		if sp.ActiveMQBrokerParameters != nil && sp.ActiveMQBrokerParameters.BatchSize > 0 {
			return sp.ActiveMQBrokerParameters.BatchSize
		}
	}

	return pipeDefaultBatchSize
}

func clonePipe(p *Pipe) *Pipe {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)
	if p.SourceParameters != nil {
		sp := *p.SourceParameters
		if p.SourceParameters.FilterCriteria != nil {
			fc := *p.SourceParameters.FilterCriteria
			fc.Filters = append([]Filter(nil), p.SourceParameters.FilterCriteria.Filters...)
			sp.FilterCriteria = &fc
		}
		if p.SourceParameters.SqsQueueParameters != nil {
			v := *p.SourceParameters.SqsQueueParameters
			sp.SqsQueueParameters = &v
		}
		if p.SourceParameters.KinesisStreamParameters != nil {
			v := *p.SourceParameters.KinesisStreamParameters
			if v.DeadLetterConfig != nil {
				dlc := *v.DeadLetterConfig
				v.DeadLetterConfig = &dlc
			}
			sp.KinesisStreamParameters = &v
		}
		if p.SourceParameters.DynamoDBStreamParameters != nil {
			v := *p.SourceParameters.DynamoDBStreamParameters
			if v.DeadLetterConfig != nil {
				dlc := *v.DeadLetterConfig
				v.DeadLetterConfig = &dlc
			}
			sp.DynamoDBStreamParameters = &v
		}
		if p.SourceParameters.ManagedStreamingKafkaParameters != nil {
			v := *p.SourceParameters.ManagedStreamingKafkaParameters
			sp.ManagedStreamingKafkaParameters = &v
		}
		if p.SourceParameters.SelfManagedKafkaParameters != nil {
			v := *p.SourceParameters.SelfManagedKafkaParameters
			v.AdditionalBootstrapServers = append([]string(nil), p.SourceParameters.SelfManagedKafkaParameters.AdditionalBootstrapServers...)
			sp.SelfManagedKafkaParameters = &v
		}
		if p.SourceParameters.RabbitMQBrokerParameters != nil {
			v := *p.SourceParameters.RabbitMQBrokerParameters
			sp.RabbitMQBrokerParameters = &v
		}
		if p.SourceParameters.ActiveMQBrokerParameters != nil {
			v := *p.SourceParameters.ActiveMQBrokerParameters
			sp.ActiveMQBrokerParameters = &v
		}
		cp.SourceParameters = &sp
	}
	if p.TargetParameters != nil {
		tp := *p.TargetParameters
		if p.TargetParameters.LambdaFunctionParameters != nil {
			v := *p.TargetParameters.LambdaFunctionParameters
			tp.LambdaFunctionParameters = &v
		}
		if p.TargetParameters.StepFunctionStateMachineParameters != nil {
			v := *p.TargetParameters.StepFunctionStateMachineParameters
			tp.StepFunctionStateMachineParameters = &v
		}
		if p.TargetParameters.SqsQueueParameters != nil {
			v := *p.TargetParameters.SqsQueueParameters
			tp.SqsQueueParameters = &v
		}
		if p.TargetParameters.KinesisStreamParameters != nil {
			v := *p.TargetParameters.KinesisStreamParameters
			tp.KinesisStreamParameters = &v
		}
		if p.TargetParameters.CloudWatchLogsParameters != nil {
			v := *p.TargetParameters.CloudWatchLogsParameters
			tp.CloudWatchLogsParameters = &v
		}
		if p.TargetParameters.EventBridgeEventBusParameters != nil {
			v := *p.TargetParameters.EventBridgeEventBusParameters
			v.Resources = append([]string(nil), p.TargetParameters.EventBridgeEventBusParameters.Resources...)
			tp.EventBridgeEventBusParameters = &v
		}
		if p.TargetParameters.RedshiftDataParameters != nil {
			v := *p.TargetParameters.RedshiftDataParameters
			v.Sqls = append([]string(nil), p.TargetParameters.RedshiftDataParameters.Sqls...)
			tp.RedshiftDataParameters = &v
		}
		if p.TargetParameters.SageMakerPipelineParameters != nil {
			v := *p.TargetParameters.SageMakerPipelineParameters
			v.PipelineParameterList = append([]SageMakerPipelineParameter(nil), p.TargetParameters.SageMakerPipelineParameters.PipelineParameterList...)
			tp.SageMakerPipelineParameters = &v
		}
		if p.TargetParameters.BatchJobParameters != nil {
			v := *p.TargetParameters.BatchJobParameters
			if v.ArrayProperties != nil {
				ap := *v.ArrayProperties
				v.ArrayProperties = &ap
			}
			if v.RetryStrategy != nil {
				rs := *v.RetryStrategy
				v.RetryStrategy = &rs
			}
			v.Parameters = maps.Clone(p.TargetParameters.BatchJobParameters.Parameters)
			tp.BatchJobParameters = &v
		}
		if p.TargetParameters.EcsTaskParameters != nil {
			v := *p.TargetParameters.EcsTaskParameters
			tp.EcsTaskParameters = &v
		}
		cp.TargetParameters = &tp
	}
	if p.DeadLetterConfig != nil {
		dlc := *p.DeadLetterConfig
		cp.DeadLetterConfig = &dlc
	}
	if p.EnrichmentParameters != nil {
		ep := *p.EnrichmentParameters
		if p.EnrichmentParameters.HttpParameters != nil {
			hp := *p.EnrichmentParameters.HttpParameters
			hp.HeaderParameters = maps.Clone(p.EnrichmentParameters.HttpParameters.HeaderParameters)
			hp.PathParameterValues = append([]string(nil), p.EnrichmentParameters.HttpParameters.PathParameterValues...)
			hp.QueryStringParameters = maps.Clone(p.EnrichmentParameters.HttpParameters.QueryStringParameters)
			ep.HttpParameters = &hp
		}
		cp.EnrichmentParameters = &ep
	}
	if p.LogConfiguration != nil {
		lc := *p.LogConfiguration
		lc.Destinations = append([]LogDestination(nil), p.LogConfiguration.Destinations...)
		lc.IncludeExecutionData = append([]string(nil), p.LogConfiguration.IncludeExecutionData...)
		cp.LogConfiguration = &lc
	}

	return &cp
}

// InMemoryBackend is the in-memory store for pipes.
type InMemoryBackend struct {
	pipes               map[string]*Pipe
	pipeARNIndex        map[string]string
	enrichmentCallCount map[string]int64 // pipe name → enrichment invocation count
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		pipes:               make(map[string]*Pipe),
		pipeARNIndex:        make(map[string]string),
		enrichmentCallCount: make(map[string]int64),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("pipes"),
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
	Tags                 map[string]string
	SourceParameters     *SourceParameters
	TargetParameters     *TargetParameters
	DeadLetterConfig     *DeadLetterConfig
	LogConfiguration     *LogConfiguration
	EnrichmentParameters *EnrichmentParameters
	Name                 string
	RoleARN              string
	Source               string
	Target               string
	Description          string
	Enrichment           string
	KmsKeyIdentifier     string
	DesiredState         string
}

func (b *InMemoryBackend) CreatePipe(in CreatePipeInput) (*Pipe, error) {
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

	b.mu.Lock("CreatePipe")
	defer b.mu.Unlock()

	if len(b.pipes) >= maxPipesPerAcct {
		return nil, fmt.Errorf(
			"%w: account has reached the maximum number of pipes (%d)",
			ErrValidation,
			maxPipesPerAcct,
		)
	}
	if _, ok := b.pipes[in.Name]; ok {
		return nil, fmt.Errorf("%w: pipe %s already exists", ErrAlreadyExists, in.Name)
	}
	if in.DesiredState == "" {
		in.DesiredState = stateRunning
	}

	now := time.Now()
	pipeARN := arn.Build("pipes", b.region, b.accountID, "pipe/"+in.Name)
	p := &Pipe{
		Name: in.Name, ARN: pipeARN, RoleARN: in.RoleARN,
		Source: in.Source, Target: in.Target, Description: in.Description,
		Enrichment: in.Enrichment, KmsKeyIdentifier: in.KmsKeyIdentifier,
		DesiredState: in.DesiredState, CurrentState: stateCreating,
		AccountID: b.accountID, Region: b.region,
		CreationTime: now, LastModifiedTime: now,
		Tags:                 mergeTags(nil, in.Tags),
		SourceParameters:     in.SourceParameters,
		TargetParameters:     in.TargetParameters,
		DeadLetterConfig:     in.DeadLetterConfig,
		LogConfiguration:     in.LogConfiguration,
		EnrichmentParameters: in.EnrichmentParameters,
	}
	b.pipes[in.Name] = p
	b.pipeARNIndex[pipeARN] = in.Name

	cp := clonePipe(p)
	go b.completeCreateTransition(in.Name, in.DesiredState)

	return cp, nil
}

// completeCreateTransition moves a pipe from CREATING to its desired state after a brief delay.
func (b *InMemoryBackend) completeCreateTransition(name, desiredState string) {
	time.Sleep(stateTransitionDelay)
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
	NextToken string
	Pipes     []*Pipe
}

func (b *InMemoryBackend) ListPipes(f ListPipesFilter) ListPipesResult {
	b.mu.RLock("ListPipes")
	defer b.mu.RUnlock()

	limit := f.Limit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}

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
	SourceParameters     *SourceParameters
	TargetParameters     *TargetParameters
	DeadLetterConfig     *DeadLetterConfig
	LogConfiguration     *LogConfiguration
	EnrichmentParameters *EnrichmentParameters
	RoleARN              string
	Target               string
	Description          string
	Enrichment           string
	KmsKeyIdentifier     string
	DesiredState         string
}

func (b *InMemoryBackend) UpdatePipe(name string, in UpdatePipeInput) (*Pipe, error) {
	if err := validateDesiredState(in.DesiredState); err != nil {
		return nil, err
	}
	b.mu.Lock("UpdatePipe")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
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
	if in.EnrichmentParameters != nil {
		p.EnrichmentParameters = in.EnrichmentParameters
	}

	prevDesiredState := p.DesiredState
	if in.DesiredState != "" {
		prevDesiredState = in.DesiredState
	}
	p.CurrentState = stateUpdating
	p.LastModifiedTime = time.Now()
	cp := clonePipe(p)

	go b.completeUpdateTransition(name, prevDesiredState)

	return cp, nil
}

// completeUpdateTransition moves a pipe from UPDATING to its desired state after a brief delay.
func (b *InMemoryBackend) completeUpdateTransition(name, desiredState string) {
	time.Sleep(stateTransitionDelay)
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

	go b.completeDeleteTransition(name)

	return cp, nil
}

// completeDeleteTransition removes the pipe after it has been marked DELETING.
func (b *InMemoryBackend) completeDeleteTransition(name string) {
	time.Sleep(stateTransitionDelay)
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

func (b *InMemoryBackend) StartPipe(name string) (*Pipe, error) {
	b.mu.Lock("StartPipe")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
	if p.DesiredState == stateRunning {
		return nil, fmt.Errorf("%w: pipe %s already has desired state RUNNING", ErrValidation, name)
	}
	p.DesiredState = stateRunning
	// Transition through STARTING → RUNNING to simulate AWS behavior.
	p.CurrentState = stateStarting
	p.StateReason = ""
	p.LastModifiedTime = time.Now()
	cp := clonePipe(p)

	// Complete the transition to RUNNING asynchronously.
	go b.completeStartTransition(name)

	return cp, nil
}

// completeStartTransition moves a pipe from STARTING to RUNNING after a brief delay.
func (b *InMemoryBackend) completeStartTransition(name string) {
	time.Sleep(stateTransitionDelay)
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

func (b *InMemoryBackend) StopPipe(name string) (*Pipe, error) {
	b.mu.Lock("StopPipe")
	defer b.mu.Unlock()
	p, ok := b.pipes[name]
	if !ok {
		return nil, fmt.Errorf("%w: pipe %s not found", ErrNotFound, name)
	}
	if p.DesiredState == stateStopped {
		return nil, fmt.Errorf("%w: pipe %s already has desired state STOPPED", ErrValidation, name)
	}
	p.DesiredState = stateStopped
	// Transition through STOPPING → STOPPED to simulate AWS behavior.
	p.CurrentState = stateStopping
	p.StateReason = ""
	p.LastModifiedTime = time.Now()
	cp := clonePipe(p)

	// Complete the transition to STOPPED asynchronously.
	go b.completeStopTransition(name)

	return cp, nil
}

// completeStopTransition moves a pipe from STOPPING to STOPPED after a brief delay.
func (b *InMemoryBackend) completeStopTransition(name string) {
	time.Sleep(stateTransitionDelay)
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

func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	if err := validateTags(kv); err != nil {
		return err
	}
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()
	name, ok := b.pipeARNIndex[resourceARN]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}
	p := b.pipes[name]
	merged := mergeTags(p.Tags, kv)
	if len(merged) > maxTagsPerPipe {
		return fmt.Errorf("%w: pipe would exceed %d tags limit", ErrValidation, maxTagsPerPipe)
	}
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
		Pipes     map[string]*Pipe `json:"pipes"`
		AccountID string           `json:"accountID"`
		Region    string           `json:"region"`
	}
	s := snap{Pipes: b.pipes, AccountID: b.accountID, Region: b.region}
	data, err := json.Marshal(s)
	if err != nil {
		return nil
	}

	return data
}

func (b *InMemoryBackend) Restore(data []byte) error {
	type snap struct {
		Pipes     map[string]*Pipe `json:"pipes"`
		AccountID string           `json:"accountID"`
		Region    string           `json:"region"`
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

	return nil
}

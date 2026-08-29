package eventbridge

import "time"

// EventBus represents an EventBridge event bus.
//
// DeadLetterConfig/KmsKeyIdentifier/LogConfig are real CreateEventBus/
// UpdateEventBus/DescribeEventBus members (eventbridge@v1.48.4
// deserializers.go's DescribeEventBusOutput case list) previously discarded
// entirely on input and never echoed back -- absent from real AWS's plain
// "EventBus" type used by ListEventBuses (deserializers.go's
// awsAwsjson11_deserializeDocumentEventBus case list has neither), so they
// are Describe/Create/Update-only, matching eventBusResponse's narrower List
// shape in handler_event_buses.go.
type EventBus struct {
	CreatedTime      time.Time         `json:"CreatedTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime,omitzero"`
	DeadLetterConfig *DeadLetterConfig `json:"DeadLetterConfig,omitempty"`
	LogConfig        *LogConfig        `json:"LogConfig,omitempty"`
	Name             string            `json:"Name"`
	Arn              string            `json:"Arn"`
	Description      string            `json:"Description,omitempty"`
	// Policy is NOT persisted on this struct -- the resource-based policy is
	// stored separately (InMemoryBackend.busePolicies, keyed by bus) since
	// EventBusPolicy carries no bus-name field of its own (see store_setup.go's
	// "What is NOT converted" note). Handlers populate this field at
	// Describe/List response time by calling GetEventBusPolicy so callers get
	// the same JSON shape AWS returns (DescribeEventBusOutput.Policy /
	// types.EventBus.Policy), without a second, driftable source of truth.
	Policy           string `json:"Policy,omitempty"`
	KmsKeyIdentifier string `json:"KmsKeyIdentifier,omitempty"`
}

// LogConfig holds the event-bus-level logging configuration
// (types.LogConfig, eventbridge@v1.48.4 types.go:929).
type LogConfig struct {
	IncludeDetail string `json:"IncludeDetail,omitempty"`
	Level         string `json:"Level,omitempty"`
}

// Rule represents an EventBridge rule.
type Rule struct {
	Name               string `json:"Name"`
	Arn                string `json:"Arn"`
	EventBusName       string `json:"EventBusName"`
	EventPattern       string `json:"EventPattern,omitempty"`
	State              string `json:"State"`
	Description        string `json:"Description,omitempty"`
	ScheduleExpression string `json:"ScheduleExpression,omitempty"`
	RoleArn            string `json:"RoleArn,omitempty"`
	ManagedBy          string `json:"ManagedBy,omitempty"`
	// CreatedBy is the account ID of the caller that created the rule
	// (aws-sdk-go-v2/service/eventbridge@v1.48.4 DescribeRuleOutput.CreatedBy).
	// Not present on types.Rule (the shape ListRulesOutput.Rules uses), so
	// handler_rules.go's DescribeRule response strips this field back out of
	// ListRules -- only DescribeRule echoes it.
	CreatedBy       string `json:"CreatedBy,omitempty"`
	compiledPattern *compiledPattern
	indexKeys       []ruleIndexKey
}

// DeadLetterConfig holds the dead-letter queue configuration for a target.
type DeadLetterConfig struct {
	Arn string `json:"Arn,omitempty"`
}

// RetryPolicy holds the retry configuration for a target.
type RetryPolicy struct {
	MaximumEventAgeInSeconds int `json:"MaximumEventAgeInSeconds,omitempty"`
	MaximumRetryAttempts     int `json:"MaximumRetryAttempts,omitempty"`
}

// BatchParameters holds batching configuration for a target (e.g. SQS).
type BatchParameters struct {
	ArrayProperties *BatchArrayProperties `json:"ArrayProperties,omitempty"`
	RetryStrategy   *BatchRetryStrategy   `json:"RetryStrategy,omitempty"`
	JobDefinition   string                `json:"JobDefinition,omitempty"`
	JobName         string                `json:"JobName,omitempty"`
}

// BatchArrayProperties defines the size of the array for a batch job.
type BatchArrayProperties struct {
	Size int `json:"Size,omitempty"`
}

// BatchRetryStrategy holds the retry configuration for a target's AWS Batch
// job (types.BatchRetryStrategy, eventbridge@v1.48.4 types.go:159) -- real,
// but previously absent here entirely, so a real client's BatchParameters.
// RetryStrategy was silently dropped on PutTargets and never echoed back by
// ListTargetsByRule.
type BatchRetryStrategy struct {
	Attempts int32 `json:"Attempts,omitempty"`
}

// Target represents an EventBridge rule target.
type Target struct {
	InputTransformer            *InputTransformer            `json:"InputTransformer,omitempty"`
	DeadLetterConfig            *DeadLetterConfig            `json:"DeadLetterConfig,omitempty"`
	RetryPolicy                 *RetryPolicy                 `json:"RetryPolicy,omitempty"`
	BatchParameters             *BatchParameters             `json:"BatchParameters,omitempty"`
	AppSyncParameters           *AppSyncParameters           `json:"AppSyncParameters,omitempty"`
	EcsParameters               *EcsParameters               `json:"EcsParameters,omitempty"`
	HTTPParameters              *HTTPParameters              `json:"HttpParameters,omitempty"`
	KinesisParameters           *KinesisParameters           `json:"KinesisParameters,omitempty"`
	RedshiftDataParameters      *RedshiftDataParameters      `json:"RedshiftDataParameters,omitempty"`
	RunCommandParameters        *RunCommandParameters        `json:"RunCommandParameters,omitempty"`
	SageMakerPipelineParameters *SageMakerPipelineParameters `json:"SageMakerPipelineParameters,omitempty"`
	SqsParameters               *SqsParameters               `json:"SqsParameters,omitempty"`
	ID                          string                       `json:"Id"`
	Arn                         string                       `json:"Arn"`
	RoleArn                     string                       `json:"RoleArn,omitempty"`
	Input                       string                       `json:"Input,omitempty"`
	InputPath                   string                       `json:"InputPath,omitempty"`
}

// InputTransformer holds input transformer configuration for a target.
type InputTransformer struct {
	InputPathsMap map[string]string `json:"InputPathsMap,omitempty"`
	InputTemplate string            `json:"InputTemplate"`
}

// AppSyncParameters holds the GraphQL operation to invoke when the target is
// an AppSync API.
type AppSyncParameters struct {
	GraphQLOperation string `json:"GraphQLOperation,omitempty"`
}

// EcsParameters holds the parameters used to run an Amazon ECS task when the
// event target is an ECS cluster.
type EcsParameters struct {
	NetworkConfiguration     *NetworkConfiguration          `json:"NetworkConfiguration,omitempty"`
	PropagateTags            string                         `json:"PropagateTags,omitempty"`
	TaskDefinitionArn        string                         `json:"TaskDefinitionArn"`
	Group                    string                         `json:"Group,omitempty"`
	LaunchType               string                         `json:"LaunchType,omitempty"`
	PlatformVersion          string                         `json:"PlatformVersion,omitempty"`
	ReferenceID              string                         `json:"ReferenceId,omitempty"`
	PlacementConstraints     []PlacementConstraint          `json:"PlacementConstraints,omitempty"`
	PlacementStrategy        []PlacementStrategy            `json:"PlacementStrategy,omitempty"`
	Tags                     []EcsTag                       `json:"Tags,omitempty"`
	CapacityProviderStrategy []CapacityProviderStrategyItem `json:"CapacityProviderStrategy,omitempty"`
	TaskCount                int32                          `json:"TaskCount,omitempty"`
	EnableECSManagedTags     bool                           `json:"EnableECSManagedTags,omitempty"`
	EnableExecuteCommand     bool                           `json:"EnableExecuteCommand,omitempty"`
}

// NetworkConfiguration specifies the awsvpc network configuration for an ECS
// target task.
type NetworkConfiguration struct {
	AwsvpcConfiguration *AwsVpcConfiguration `json:"AwsvpcConfiguration,omitempty"`
}

// AwsVpcConfiguration specifies the subnets, security groups, and public-IP
// assignment for an ECS target task using the awsvpc network mode.
type AwsVpcConfiguration struct {
	AssignPublicIP string   `json:"AssignPublicIp,omitempty"`
	Subnets        []string `json:"Subnets"`
	SecurityGroups []string `json:"SecurityGroups,omitempty"`
}

// CapacityProviderStrategyItem is a single entry in an ECS target's capacity
// provider strategy.
type CapacityProviderStrategyItem struct {
	CapacityProvider string `json:"CapacityProvider"`
	Base             int32  `json:"Base,omitempty"`
	Weight           int32  `json:"Weight,omitempty"`
}

// PlacementConstraint is a single ECS task placement constraint.
type PlacementConstraint struct {
	Expression string `json:"Expression,omitempty"`
	Type       string `json:"Type,omitempty"`
}

// PlacementStrategy is a single ECS task placement strategy rule.
type PlacementStrategy struct {
	Field string `json:"Field,omitempty"`
	Type  string `json:"Type,omitempty"`
}

// EcsTag is a key/value tag applied to an ECS target task (distinct from the
// EventBridge resource-tag maps used by tags.go, which model bus/rule/etc.
// tagging, not the per-task tags forwarded to ECS RunTask).
type EcsTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value,omitempty"`
}

// HTTPParameters holds the headers, path parameters, and query-string values
// to add when the target is an API Gateway API or EventBridge ApiDestination.
type HTTPParameters struct {
	HeaderParameters      map[string]string `json:"HeaderParameters,omitempty"`
	QueryStringParameters map[string]string `json:"QueryStringParameters,omitempty"`
	PathParameterValues   []string          `json:"PathParameterValues,omitempty"`
}

// KinesisParameters specifies the partition-key JSON path for a Kinesis Data
// Stream target.
type KinesisParameters struct {
	PartitionKeyPath string `json:"PartitionKeyPath"`
}

// RedshiftDataParameters holds the Redshift Data API ExecuteStatement
// parameters for an Amazon Redshift cluster target.
type RedshiftDataParameters struct {
	Database         string   `json:"Database"`
	DBUser           string   `json:"DbUser,omitempty"`
	SecretManagerArn string   `json:"SecretManagerArn,omitempty"`
	SQL              string   `json:"Sql,omitempty"`
	StatementName    string   `json:"StatementName,omitempty"`
	Sqls             []string `json:"Sqls,omitempty"`
	WithEvent        bool     `json:"WithEvent,omitempty"`
}

// RunCommandParameters holds the EC2 Run Command targets for a target rule.
type RunCommandParameters struct {
	RunCommandTargets []RunCommandTarget `json:"RunCommandTargets"`
}

// RunCommandTarget selects EC2 instances by tag or instance ID for Run
// Command.
type RunCommandTarget struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// SageMakerPipelineParameters holds the pipeline parameter overrides used to
// start a SageMaker AI Model Building Pipeline execution.
type SageMakerPipelineParameters struct {
	PipelineParameterList []SageMakerPipelineParameter `json:"PipelineParameterList,omitempty"`
}

// SageMakerPipelineParameter is a single name/value pipeline parameter
// override.
type SageMakerPipelineParameter struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// SqsParameters holds the FIFO message-group ID to use when the target is an
// SQS FIFO queue.
type SqsParameters struct {
	MessageGroupID string `json:"MessageGroupId,omitempty"`
}

// EventEntry represents a single event to publish.
type EventEntry struct {
	Time         *time.Time `json:"Time,omitempty"`
	Source       string     `json:"Source"`
	DetailType   string     `json:"DetailType"`
	Detail       string     `json:"Detail"`
	EventBusName string     `json:"EventBusName,omitempty"`
	Resources    []string   `json:"Resources,omitempty"`
}

// EventResultEntry is returned per event in a PutEvents response.
type EventResultEntry struct {
	EventID      string `json:"EventId,omitempty"`
	ErrorCode    string `json:"ErrorCode,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
}

// PutRuleInput is the input for PutRule.
type PutRuleInput struct {
	Tags               map[string]string `json:"Tags,omitempty"`
	Name               string            `json:"Name"`
	EventBusName       string            `json:"EventBusName,omitempty"`
	EventPattern       string            `json:"EventPattern,omitempty"`
	State              string            `json:"State,omitempty"`
	Description        string            `json:"Description,omitempty"`
	ScheduleExpression string            `json:"ScheduleExpression,omitempty"`
	RoleArn            string            `json:"RoleArn,omitempty"`
	// ManagedBy is deliberately NOT wire-visible (json:"-"): the real AWS
	// PutRuleInput has no ManagedBy member at all -- it is a server-populated,
	// Describe/List-only field (see Rule.ManagedBy / DescribeRuleOutput.ManagedBy).
	// Exposing it as a settable JSON field let any client forge a rule's
	// ManagedBy on the public PutRule API, which real AWS never allows. The Go
	// field itself is kept only as an internal seeding hook for a future
	// same-process AWS-service integration (e.g. EventBridge Scheduler) that
	// wants to mark a rule it creates as service-managed; PutRule still rejects
	// (ManagedRuleException) any attempt to modify a rule that already has one.
	ManagedBy string `json:"-"`
}

// FailedEntry describes a target or event that failed to process.
type FailedEntry struct {
	TargetID     string `json:"TargetId,omitempty"`
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

// EventLogEntry is an entry in the internal event log.
type EventLogEntry struct {
	Time         time.Time `json:"time"`
	ID           string    `json:"id"`
	Source       string    `json:"source"`
	DetailType   string    `json:"detailType"`
	Detail       string    `json:"detail"`
	EventBusName string    `json:"eventBusName"`
}

// EventSource represents a partner event source.
type EventSource struct {
	Arn            string    `json:"Arn"`
	CreatedBy      string    `json:"CreatedBy"`
	CreationTime   time.Time `json:"CreationTime"`
	ExpirationTime time.Time `json:"ExpirationTime,omitzero"`
	Name           string    `json:"Name"`
	State          string    `json:"State"` // PENDING, ACTIVE, DELETED
}

// Replay represents an EventBridge replay.
type Replay struct {
	EventStartTime  time.Time `json:"EventStartTime,omitzero"`
	EventEndTime    time.Time `json:"EventEndTime,omitzero"`
	ReplayStartTime time.Time `json:"ReplayStartTime,omitzero"`
	ReplayEndTime   time.Time `json:"ReplayEndTime,omitzero"`
	// Destination is echoed by DescribeReplay (real AWS's DescribeReplayOutput
	// has a Destination member) but NOT by ListReplays (real AWS's
	// ListReplaysOutput uses types.Replay, which has no Destination field) --
	// see handler_replays.go's describeReplayResponse vs replayListResponse.
	Destination    *ReplayDestination `json:"-"`
	ReplayName     string             `json:"ReplayName"`
	ReplayArn      string             `json:"ReplayArn"`
	EventSourceArn string             `json:"EventSourceArn"`
	State          string             `json:"State"` // STARTING, RUNNING, CANCELLING, COMPLETED, CANCELLED, FAILED
	// Description is the free-text description supplied at StartReplay
	// (StartReplayInput.Description), echoed back by DescribeReplay only --
	// same Describe-only visibility as Destination, and distinct from
	// StateReason below (a system-set explanation of the current state, not
	// the user-supplied description -- these were previously conflated).
	Description string `json:"-"`
	StateReason string `json:"StateReason,omitempty"`
}

// APIDestination represents an EventBridge API destination.
type APIDestination struct {
	CreationTime                 time.Time `json:"CreationTime"`
	LastModifiedTime             time.Time `json:"LastModifiedTime"`
	APIDestinationArn            string    `json:"ApiDestinationArn"`
	APIDestinationState          string    `json:"ApiDestinationState"`
	ConnectionArn                string    `json:"ConnectionArn"`
	Description                  string    `json:"Description,omitempty"`
	HTTPMethod                   string    `json:"HttpMethod"`
	InvocationEndpoint           string    `json:"InvocationEndpoint"`
	Name                         string    `json:"Name"`
	InvocationRateLimitPerSecond int       `json:"InvocationRateLimitPerSecond,omitempty"`
}

// Archive represents an EventBridge archive.
type Archive struct {
	CreationTime     time.Time `json:"CreationTime"`
	ArchiveName      string    `json:"ArchiveName"`
	ArchiveArn       string    `json:"ArchiveArn"`
	Description      string    `json:"Description,omitempty"`
	EventPattern     string    `json:"EventPattern,omitempty"`
	EventSourceArn   string    `json:"EventSourceArn"`
	State            string    `json:"State"`
	StateReason      string    `json:"StateReason,omitempty"`
	KmsKeyIdentifier string    `json:"KmsKeyIdentifier,omitempty"`
	EventCount       int64     `json:"EventCount"`
	RetentionDays    int       `json:"RetentionDays,omitempty"`
	SizeBytes        int64     `json:"SizeBytes"`
}

// Connection represents an EventBridge connection.
type Connection struct {
	AuthParameters *ConnectionAuthParameters `json:"AuthParameters,omitempty"`
	// authSecret holds the un-masked credentials used to sign outbound
	// API-destination requests. AWS keeps these in Secrets Manager and never
	// returns them from Describe/List; the exported AuthParameters above are
	// always masked. This field is unexported so it is never serialized into
	// API responses or persistence snapshots.
	authSecret         *ConnectionAuthParameters
	ConnectionArn      string    `json:"ConnectionArn"`
	AuthorizationType  string    `json:"AuthorizationType"`
	ConnectionState    string    `json:"ConnectionState"`
	CreationTime       time.Time `json:"CreationTime"`
	Description        string    `json:"Description,omitempty"`
	LastAuthorizedTime time.Time `json:"LastAuthorizedTime,omitzero"`
	LastModifiedTime   time.Time `json:"LastModifiedTime"`
	Name               string    `json:"Name"`
	SecretArn          string    `json:"SecretArn,omitempty"`
	StateReason        string    `json:"StateReason,omitempty"`
}

// Endpoint represents an EventBridge global endpoint.
type Endpoint struct {
	CreationTime      time.Time          `json:"CreationTime"`
	LastModifiedTime  time.Time          `json:"LastModifiedTime"`
	ReplicationConfig *ReplicationConfig `json:"ReplicationConfig,omitempty"`
	RoutingConfig     *RoutingConfig     `json:"RoutingConfig,omitempty"`
	RoleArn           string             `json:"RoleArn,omitempty"`
	EndpointURL       string             `json:"EndpointUrl"`
	Name              string             `json:"Name"`
	EndpointID        string             `json:"EndpointId"`
	Description       string             `json:"Description,omitempty"`
	Arn               string             `json:"Arn"`
	State             string             `json:"State"`
	StateReason       string             `json:"StateReason,omitempty"`
	EventBuses        []EndpointEventBus `json:"EventBuses,omitempty"`
}

// EndpointEventBus associates an event bus with an endpoint.
type EndpointEventBus struct {
	EventBusArn string `json:"EventBusArn"`
}

// ReplicationConfig defines replication settings for an endpoint.
type ReplicationConfig struct {
	State string `json:"State"` // ENABLED, DISABLED
}

// RoutingConfig defines routing configuration for an endpoint.
type RoutingConfig struct {
	FailoverConfig *FailoverConfig `json:"FailoverConfig"`
}

// FailoverConfig defines failover settings.
type FailoverConfig struct {
	Primary   *Primary   `json:"Primary"`
	Secondary *Secondary `json:"Secondary"`
}

// Primary defines the primary region health check.
type Primary struct {
	HealthCheck string `json:"HealthCheck"`
}

// Secondary defines the secondary region route.
type Secondary struct {
	Route string `json:"Route"`
}

// PartnerEventSource represents a partner event source.
type PartnerEventSource struct {
	Arn     string `json:"Arn"`
	Name    string `json:"Name"`
	Account string `json:"Account,omitempty"`
}

// PartnerEventSourceAccountInfo mirrors aws-sdk-go-v2/service/eventbridge's
// types.PartnerEventSourceAccount, ListPartnerEventSourceAccountsOutput's
// element type.
type PartnerEventSourceAccountInfo struct {
	CreationTime   time.Time `json:"CreationTime,omitzero"`
	ExpirationTime time.Time `json:"ExpirationTime,omitzero"`
	Account        string    `json:"Account,omitempty"`
	State          string    `json:"State,omitempty"`
}

// CreateAPIDestinationInput is the input for CreateAPIDestination.
type CreateAPIDestinationInput struct {
	ConnectionArn                string `json:"ConnectionArn"`
	Description                  string `json:"Description,omitempty"`
	HTTPMethod                   string `json:"HttpMethod"`
	InvocationEndpoint           string `json:"InvocationEndpoint"`
	Name                         string `json:"Name"`
	InvocationRateLimitPerSecond int    `json:"InvocationRateLimitPerSecond,omitempty"`
}

// CreateArchiveInput is the input for CreateArchive.
type CreateArchiveInput struct {
	ArchiveName      string `json:"ArchiveName"`
	Description      string `json:"Description,omitempty"`
	EventPattern     string `json:"EventPattern,omitempty"`
	EventSourceArn   string `json:"EventSourceArn"`
	KmsKeyIdentifier string `json:"KmsKeyIdentifier,omitempty"`
	RetentionDays    int    `json:"RetentionDays,omitempty"`
}

// ConnectionAuthParameters holds the auth credentials for a connection.
type ConnectionAuthParameters struct {
	BasicAuthParameters      *ConnectionBasicAuthParameters  `json:"BasicAuthParameters,omitempty"`
	APIKeyAuthParameters     *ConnectionAPIKeyAuthParameters `json:"ApiKeyAuthParameters,omitempty"`
	OAuthParameters          *ConnectionOAuthParameters      `json:"OAuthParameters,omitempty"`
	InvocationHTTPParameters *ConnectionHTTPParameters       `json:"InvocationHttpParameters,omitempty"`
}

// ConnectionBasicAuthParameters holds Basic auth credentials.
type ConnectionBasicAuthParameters struct {
	Username string `json:"Username"`
	Password string `json:"Password,omitempty"`
}

// ConnectionAPIKeyAuthParameters holds API key auth credentials.
type ConnectionAPIKeyAuthParameters struct {
	APIKeyName  string `json:"ApiKeyName"`
	APIKeyValue string `json:"ApiKeyValue,omitempty"`
}

// ConnectionOAuthParameters holds OAuth credentials.
type ConnectionOAuthParameters struct {
	ClientParameters      *ConnectionOAuthClientParameters `json:"ClientParameters,omitempty"`
	OAuthHTTPParameters   *ConnectionHTTPParameters        `json:"OAuthHttpParameters,omitempty"`
	AuthorizationEndpoint string                           `json:"AuthorizationEndpoint"`
	HTTPMethod            string                           `json:"HttpMethod"`
}

// ConnectionOAuthClientParameters holds OAuth client ID and secret.
type ConnectionOAuthClientParameters struct {
	ClientID     string `json:"ClientID"`
	ClientSecret string `json:"ClientSecret,omitempty"`
}

// ConnectionHTTPParameters holds custom HTTP body/header/query-string parameters.
type ConnectionHTTPParameters struct {
	BodyParameters        []ConnectionBodyParameter        `json:"BodyParameters,omitempty"`
	HeaderParameters      []ConnectionHeaderParameter      `json:"HeaderParameters,omitempty"`
	QueryStringParameters []ConnectionQueryStringParameter `json:"QueryStringParameters,omitempty"`
}

// ConnectionBodyParameter holds a single body parameter key/value pair.
type ConnectionBodyParameter struct {
	Key           string `json:"Key"`
	Value         string `json:"Value,omitempty"`
	IsValueSecret bool   `json:"IsValueSecret,omitempty"`
}

// ConnectionHeaderParameter holds a single header key/value pair.
type ConnectionHeaderParameter struct {
	Key           string `json:"Key"`
	Value         string `json:"Value,omitempty"`
	IsValueSecret bool   `json:"IsValueSecret,omitempty"`
}

// ConnectionQueryStringParameter holds a single query-string key/value pair.
type ConnectionQueryStringParameter struct {
	Key           string `json:"Key"`
	Value         string `json:"Value,omitempty"`
	IsValueSecret bool   `json:"IsValueSecret,omitempty"`
}

// CreateConnectionInput is the input for CreateConnection.
type CreateConnectionInput struct {
	AuthorizationType string                    `json:"AuthorizationType"`
	AuthParameters    *ConnectionAuthParameters `json:"AuthParameters,omitempty"`
	Description       string                    `json:"Description,omitempty"`
	Name              string                    `json:"Name"`
}

// CreateEndpointInput is the input for CreateEndpoint.
type CreateEndpointInput struct {
	ReplicationConfig *ReplicationConfig `json:"ReplicationConfig,omitempty"`
	RoutingConfig     *RoutingConfig     `json:"RoutingConfig,omitempty"`
	Description       string             `json:"Description,omitempty"`
	Name              string             `json:"Name"`
	RoleArn           string             `json:"RoleArn,omitempty"`
	EventBuses        []EndpointEventBus `json:"EventBuses"`
}

// UpdateArchiveInput is the input for UpdateArchive.
type UpdateArchiveInput struct {
	ArchiveName      string `json:"ArchiveName"`
	Description      string `json:"Description,omitempty"`
	EventPattern     string `json:"EventPattern,omitempty"`
	KmsKeyIdentifier string `json:"KmsKeyIdentifier,omitempty"`
	RetentionDays    int    `json:"RetentionDays,omitempty"`
}

// UpdateConnectionInput is the input for UpdateConnection.
type UpdateConnectionInput struct {
	AuthorizationType string                    `json:"AuthorizationType,omitempty"`
	AuthParameters    *ConnectionAuthParameters `json:"AuthParameters,omitempty"`
	Description       string                    `json:"Description,omitempty"`
	Name              string                    `json:"Name"`
}

// UpdateEndpointInput is the input for UpdateEndpoint.
type UpdateEndpointInput struct {
	ReplicationConfig *ReplicationConfig `json:"ReplicationConfig,omitempty"`
	RoutingConfig     *RoutingConfig     `json:"RoutingConfig,omitempty"`
	Description       string             `json:"Description,omitempty"`
	Name              string             `json:"Name"`
	RoleArn           string             `json:"RoleArn,omitempty"`
	EventBuses        []EndpointEventBus `json:"EventBuses,omitempty"`
}

// UpdateAPIDestinationInput is the input for UpdateApiDestination.
type UpdateAPIDestinationInput struct {
	ConnectionArn                string `json:"ConnectionArn,omitempty"`
	Description                  string `json:"Description,omitempty"`
	HTTPMethod                   string `json:"HttpMethod,omitempty"`
	InvocationEndpoint           string `json:"InvocationEndpoint,omitempty"`
	Name                         string `json:"Name"`
	InvocationRateLimitPerSecond int    `json:"InvocationRateLimitPerSecond,omitempty"`
}

// ReplayDestination specifies the destination for a replay.
type ReplayDestination struct {
	Arn string `json:"Arn"`
	// FilterArns restricts replay delivery to only these rule ARNs on the
	// destination bus. When empty, replayed events are delivered to every
	// ENABLED rule on the destination bus whose EventPattern matches (AWS's
	// default "replay to all matching rules" behaviour).
	FilterArns []string `json:"FilterArns,omitempty"`
}

// StartReplayInput is the input for StartReplay.
type StartReplayInput struct {
	EventEndTime   time.Time          `json:"EventEndTime"`
	EventStartTime time.Time          `json:"EventStartTime"`
	Destination    *ReplayDestination `json:"Destination,omitempty"`
	Description    string             `json:"Description,omitempty"`
	EventSourceArn string             `json:"EventSourceArn"`
	ReplayName     string             `json:"ReplayName"`
}

// UpdateEventBusInput is the input for UpdateEventBus.
type UpdateEventBusInput struct {
	DeadLetterConfig *DeadLetterConfig `json:"DeadLetterConfig,omitempty"`
	LogConfig        *LogConfig        `json:"LogConfig,omitempty"`
	Description      string            `json:"Description,omitempty"`
	KmsKeyIdentifier string            `json:"KmsKeyIdentifier,omitempty"`
	Name             string            `json:"Name"`
}

// Condition limits a PutPermission grant to accounts fulfilling a certain
// condition, such as membership in an AWS organization (real SDK:
// aws-sdk-go-v2/service/eventbridge@v1.48.4 types.Condition -- Type/Key/Value
// all required, e.g. {"Type":"StringEquals","Key":"aws:PrincipalOrgID",
// "Value":"o-1234567890"}).
type Condition struct {
	Type  string `json:"Type"`
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// PutPermissionInput is the input for PutPermission.
type PutPermissionInput struct {
	Condition    *Condition `json:"Condition,omitempty"`
	Policy       string     `json:"Policy,omitempty"`
	Action       string     `json:"Action,omitempty"`
	EventBusName string     `json:"EventBusName,omitempty"`
	Principal    string     `json:"Principal,omitempty"`
	StatementID  string     `json:"StatementId,omitempty"`
}

// RemovePermissionInput is the input for RemovePermission.
type RemovePermissionInput struct {
	EventBusName         string `json:"EventBusName,omitempty"`
	StatementID          string `json:"StatementId,omitempty"`
	RemoveAllPermissions bool   `json:"RemoveAllPermissions,omitempty"`
}

// EventBusPolicyStatement is a single statement in an event bus resource policy.
// Condition uses the standard IAM policy JSON shape (a map from condition
// operator, e.g. "StringEquals", to a map of condition key to value) --
// PutPermission's own Condition parameter is flattened into this shape, same
// as real AWS does when it renders the bus's resource policy document.
type EventBusPolicyStatement struct {
	Condition map[string]map[string]string `json:"Condition,omitempty"`
	Principal any                          `json:"Principal"`
	Action    string                       `json:"Action"`
	Effect    string                       `json:"Effect"`
	Sid       string                       `json:"Sid"`
}

// EventBusPolicy is the resource-based policy attached to an event bus.
type EventBusPolicy struct {
	Statements map[string]*EventBusPolicyStatement `json:"Statements"`
}

// GetEventBusPolicyInput is the input for GetEventBusPolicy.
type GetEventBusPolicyInput struct {
	EventBusName string `json:"EventBusName,omitempty"`
}

// PutEventBusPolicyInput is the input for PutEventBusPolicy (sets raw policy JSON).
type PutEventBusPolicyInput struct {
	EventBusName string `json:"EventBusName,omitempty"`
	Policy       string `json:"Policy"`
}

// ---------------------------------------------------------------------------
// Schema Registry models
// ---------------------------------------------------------------------------

// SchemaRegistry represents an EventBridge Schema Registry.
type SchemaRegistry struct {
	Tags         map[string]string `json:"Tags,omitempty"`
	RegistryArn  string            `json:"RegistryArn"`
	RegistryName string            `json:"RegistryName"`
	Description  string            `json:"Description,omitempty"`
}

// Schema represents a schema within a registry.
type Schema struct {
	LastModified       time.Time         `json:"LastModified"`
	VersionCreatedDate time.Time         `json:"VersionCreatedDate"`
	Tags               map[string]string `json:"Tags,omitempty"`
	SchemaArn          string            `json:"SchemaArn"`
	SchemaName         string            `json:"SchemaName"`
	SchemaVersion      string            `json:"SchemaVersion"`
	RegistryName       string            `json:"RegistryName"`
	Description        string            `json:"Description,omitempty"`
	Type               string            `json:"Type"`
	Content            string            `json:"Content"`
}

// SchemaVersion represents a specific version of a schema.
type SchemaVersion struct {
	CreatedDate   time.Time `json:"CreatedDate"`
	SchemaArn     string    `json:"SchemaArn"`
	SchemaName    string    `json:"SchemaName"`
	SchemaVersion string    `json:"SchemaVersion"`
	RegistryName  string    `json:"RegistryName"`
	Type          string    `json:"Type"`
	Content       string    `json:"Content"`
}

// CodeBinding represents a generated code binding for a schema.
type CodeBinding struct {
	CreationDate  time.Time `json:"CreationDate"`
	LastModified  time.Time `json:"LastModified"`
	Language      string    `json:"Language"`
	SchemaVersion string    `json:"SchemaVersion"`
	Status        string    `json:"Status"` // CREATE_COMPLETE, CREATE_IN_PROGRESS, CREATE_FAILED
}

// CreateRegistryInput is the input for CreateRegistry.
type CreateRegistryInput struct {
	Tags         map[string]string `json:"Tags,omitempty"`
	RegistryName string            `json:"RegistryName"`
	Description  string            `json:"Description,omitempty"`
}

// UpdateRegistryInput is the input for UpdateRegistry.
type UpdateRegistryInput struct {
	RegistryName string `json:"RegistryName"`
	Description  string `json:"Description,omitempty"`
}

// CreateSchemaInput is the input for CreateSchema.
type CreateSchemaInput struct {
	Tags         map[string]string `json:"Tags,omitempty"`
	RegistryName string            `json:"RegistryName"`
	SchemaName   string            `json:"SchemaName"`
	Type         string            `json:"Type"`
	Content      string            `json:"Content"`
	Description  string            `json:"Description,omitempty"`
}

// UpdateSchemaInput is the input for UpdateSchema (creates a new version).
type UpdateSchemaInput struct {
	RegistryName  string `json:"RegistryName"`
	SchemaName    string `json:"SchemaName"`
	Type          string `json:"Type,omitempty"`
	Content       string `json:"Content,omitempty"`
	Description   string `json:"Description,omitempty"`
	ClientTokenID string `json:"ClientTokenId,omitempty"`
}

// PutCodeBindingInput is the input for PutCodeBinding.
type PutCodeBindingInput struct {
	RegistryName  string `json:"RegistryName"`
	SchemaName    string `json:"SchemaName"`
	Language      string `json:"Language"`
	SchemaVersion string `json:"SchemaVersion,omitempty"`
}

// DescribeCodeBindingInput is the input for DescribeCodeBinding.
type DescribeCodeBindingInput struct {
	RegistryName  string `json:"RegistryName"`
	SchemaName    string `json:"SchemaName"`
	Language      string `json:"Language"`
	SchemaVersion string `json:"SchemaVersion,omitempty"`
}

// ListCodeBindingsInput is the input for ListCodeBindings.
type ListCodeBindingsInput struct {
	RegistryName  string `json:"RegistryName"`
	SchemaName    string `json:"SchemaName"`
	SchemaVersion string `json:"SchemaVersion,omitempty"`
	NextToken     string `json:"NextToken,omitempty"`
}

// GetDiscoveredSchemaInput is the input for GetDiscoveredSchema.
type GetDiscoveredSchemaInput struct {
	Type   string   `json:"Type"`
	Events []string `json:"Events"`
}

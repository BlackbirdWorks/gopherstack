package eventbridge

import "time"

// EventBus represents an EventBridge event bus.
type EventBus struct {
	CreatedTime time.Time `json:"CreatedTime"`
	Name        string    `json:"Name"`
	Arn         string    `json:"Arn"`
	Description string    `json:"Description,omitempty"`
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
	compiledPattern    *compiledPattern
	indexKeys          []ruleIndexKey
}

// Target represents an EventBridge rule target.
type Target struct {
	InputTransformer *InputTransformer `json:"InputTransformer,omitempty"`
	DeadLetterConfig *DeadLetterConfig `json:"DeadLetterConfig,omitempty"`
	RetryPolicy      *RetryPolicy      `json:"RetryPolicy,omitempty"`
	BatchParameters  *BatchParameters  `json:"BatchParameters,omitempty"`
	HTTPParameters   *HTTPParameters   `json:"HttpParameters,omitempty"`
	ID               string            `json:"Id"`
	Arn              string            `json:"Arn"`
	RoleArn          string            `json:"RoleArn,omitempty"`
	Input            string            `json:"Input,omitempty"`
	InputPath        string            `json:"InputPath,omitempty"`
}

// DeadLetterConfig configures a target-level SQS dead-letter queue.
type DeadLetterConfig struct {
	Arn string `json:"Arn,omitempty"`
}

// RetryPolicy configures target delivery retries.
type RetryPolicy struct {
	MaximumEventAgeInSeconds int `json:"MaximumEventAgeInSeconds,omitempty"`
	MaximumRetryAttempts     int `json:"MaximumRetryAttempts,omitempty"`
}

// BatchParameters configures target batching for services that support it.
type BatchParameters struct {
	BatchSize   int `json:"BatchSize,omitempty"`
	BatchWindow int `json:"BatchWindow,omitempty"`
}

// HTTPParameters configures API destination path, query, and header values.
type HTTPParameters struct {
	PathParameterValues   []string          `json:"PathParameterValues,omitempty"`
	QueryStringParameters map[string]string `json:"QueryStringParameters,omitempty"`
	HeaderParameters      map[string]string `json:"HeaderParameters,omitempty"`
}

// InputTransformer holds input transformer configuration for a target.
type InputTransformer struct {
	InputPathsMap map[string]string `json:"InputPathsMap,omitempty"`
	InputTemplate string            `json:"InputTemplate"`
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
	Name               string `json:"Name"`
	EventBusName       string `json:"EventBusName,omitempty"`
	EventPattern       string `json:"EventPattern,omitempty"`
	State              string `json:"State,omitempty"`
	Description        string `json:"Description,omitempty"`
	ScheduleExpression string `json:"ScheduleExpression,omitempty"`
	RoleArn            string `json:"RoleArn,omitempty"`
	ManagedBy          string `json:"ManagedBy,omitempty"`
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
	ReplayName      string    `json:"ReplayName"`
	ReplayArn       string    `json:"ReplayArn"`
	EventSourceArn  string    `json:"EventSourceArn"`
	State           string    `json:"State"` // STARTING, RUNNING, CANCELLING, COMPLETED, CANCELLED, FAILED
	StateReason     string    `json:"StateReason,omitempty"`
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

// EventBusPolicy stores resource policy statements for an event bus.
type EventBusPolicy struct {
	EventBusName string                     `json:"EventBusName"`
	Policy       string                     `json:"Policy,omitempty"`
	Statements   map[string]PolicyStatement `json:"Statements,omitempty"`
}

// PolicyStatement is the subset of EventBridge policy statement fields used by
// the in-memory authorization simulation.
type PolicyStatement struct {
	StatementID string `json:"Sid,omitempty"`
	Action      string `json:"Action,omitempty"`
	Principal   string `json:"Principal,omitempty"`
	Effect      string `json:"Effect,omitempty"`
}

// Pipe represents an EventBridge Pipe.
type Pipe struct {
	CreationTime         time.Time `json:"CreationTime"`
	LastModifiedTime     time.Time `json:"LastModifiedTime"`
	Name                 string    `json:"Name"`
	Arn                  string    `json:"Arn"`
	RoleArn              string    `json:"RoleArn"`
	Source               string    `json:"Source"`
	Target               string    `json:"Target"`
	Description          string    `json:"Description,omitempty"`
	DesiredState         string    `json:"DesiredState,omitempty"`
	CurrentState         string    `json:"CurrentState"`
	StateReason          string    `json:"StateReason,omitempty"`
	Enrichment           string    `json:"Enrichment,omitempty"`
	FilterCriteria       any       `json:"FilterCriteria,omitempty"`
	SourceParameters     any       `json:"SourceParameters,omitempty"`
	TargetParameters     any       `json:"TargetParameters,omitempty"`
	EnrichmentParameters any       `json:"EnrichmentParameters,omitempty"`
}

// Archive represents an EventBridge archive.
type Archive struct {
	CreationTime   time.Time `json:"CreationTime"`
	ArchiveName    string    `json:"ArchiveName"`
	ArchiveArn     string    `json:"ArchiveArn"`
	Description    string    `json:"Description,omitempty"`
	EventPattern   string    `json:"EventPattern,omitempty"`
	EventSourceArn string    `json:"EventSourceArn"`
	State          string    `json:"State"`
	StateReason    string    `json:"StateReason,omitempty"`
	EventCount     int64     `json:"EventCount"`
	RetentionDays  int       `json:"RetentionDays,omitempty"`
	SizeBytes      int64     `json:"SizeBytes"`
}

// Connection represents an EventBridge connection.
type Connection struct {
	ConnectionArn      string    `json:"ConnectionArn"`
	AuthorizationType  string    `json:"AuthorizationType"`
	AuthParameters     *ConnectionAuthParameters `json:"AuthParameters,omitempty"`
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
	ArchiveName    string `json:"ArchiveName"`
	Description    string `json:"Description,omitempty"`
	EventPattern   string `json:"EventPattern,omitempty"`
	EventSourceArn string `json:"EventSourceArn"`
	RetentionDays  int    `json:"RetentionDays,omitempty"`
}

// CreateConnectionInput is the input for CreateConnection.
type CreateConnectionInput struct {
	AuthorizationType string `json:"AuthorizationType"`
	Description       string `json:"Description,omitempty"`
	Name              string `json:"Name"`
	AuthParameters    *ConnectionAuthParameters `json:"AuthParameters,omitempty"`
}

// ConnectionAuthParameters stores API destination connection credentials.
type ConnectionAuthParameters struct {
	APIKeyAuthParameters *APIKeyAuthParameters `json:"ApiKeyAuthParameters,omitempty"`
	BasicAuthParameters  *BasicAuthParameters  `json:"BasicAuthParameters,omitempty"`
	OAuthParameters      *OAuthParameters      `json:"OAuthParameters,omitempty"`
}

// APIKeyAuthParameters configures API key auth.
type APIKeyAuthParameters struct {
	APIKeyName  string `json:"ApiKeyName"`
	APIKeyValue string `json:"ApiKeyValue"`
}

// BasicAuthParameters configures HTTP Basic auth.
type BasicAuthParameters struct {
	Username string `json:"Username"`
	Password string `json:"Password"`
}

// OAuthParameters configures OAuth client credentials auth.
type OAuthParameters struct {
	AuthorizationEndpoint string            `json:"AuthorizationEndpoint"`
	HTTPMethod            string            `json:"HttpMethod"`
	ClientParameters      OAuthClientParams `json:"ClientParameters"`
	OAuthHTTPParameters   *HTTPParameters   `json:"OAuthHttpParameters,omitempty"`
}

// OAuthClientParams stores OAuth client credentials.
type OAuthClientParams struct {
	ClientID     string `json:"ClientID"`
	ClientSecret string `json:"ClientSecret"`
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
	ArchiveName   string `json:"ArchiveName"`
	Description   string `json:"Description,omitempty"`
	EventPattern  string `json:"EventPattern,omitempty"`
	RetentionDays int    `json:"RetentionDays,omitempty"`
}

// UpdateConnectionInput is the input for UpdateConnection.
type UpdateConnectionInput struct {
	AuthorizationType string `json:"AuthorizationType,omitempty"`
	Description       string `json:"Description,omitempty"`
	Name              string `json:"Name"`
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
	Description string `json:"Description,omitempty"`
	Name        string `json:"Name"`
}

// PutPermissionInput is the input for PutPermission.
type PutPermissionInput struct {
	Action       string `json:"Action,omitempty"`
	EventBusName string `json:"EventBusName,omitempty"`
	Principal    string `json:"Principal,omitempty"`
	StatementID  string `json:"StatementId,omitempty"`
}

// RemovePermissionInput is the input for RemovePermission.
type RemovePermissionInput struct {
	EventBusName string `json:"EventBusName,omitempty"`
	StatementID  string `json:"StatementId,omitempty"`
}

// PutEventBusPolicyInput is the input for PutEventBusPolicy.
type PutEventBusPolicyInput struct {
	EventBusName string `json:"EventBusName,omitempty"`
	Policy       string `json:"Policy"`
	StatementID  string `json:"StatementId,omitempty"`
	Action       string `json:"Action,omitempty"`
	Principal    string `json:"Principal,omitempty"`
}

// GetEventBusPolicyInput is the input for GetEventBusPolicy.
type GetEventBusPolicyInput struct {
	EventBusName string `json:"EventBusName,omitempty"`
}

// CreatePipeInput is the input for CreatePipe.
type CreatePipeInput struct {
	Name                 string `json:"Name"`
	RoleArn              string `json:"RoleArn"`
	Source               string `json:"Source"`
	Target               string `json:"Target"`
	Description          string `json:"Description,omitempty"`
	DesiredState         string `json:"DesiredState,omitempty"`
	Enrichment           string `json:"Enrichment,omitempty"`
	FilterCriteria       any    `json:"FilterCriteria,omitempty"`
	SourceParameters     any    `json:"SourceParameters,omitempty"`
	TargetParameters     any    `json:"TargetParameters,omitempty"`
	EnrichmentParameters any    `json:"EnrichmentParameters,omitempty"`
}

// UpdatePipeInput is the input for UpdatePipe.
type UpdatePipeInput struct {
	Name                 string `json:"Name"`
	RoleArn              string `json:"RoleArn,omitempty"`
	Source               string `json:"Source,omitempty"`
	Target               string `json:"Target,omitempty"`
	Description          string `json:"Description,omitempty"`
	DesiredState         string `json:"DesiredState,omitempty"`
	Enrichment           string `json:"Enrichment,omitempty"`
	FilterCriteria       any    `json:"FilterCriteria,omitempty"`
	SourceParameters     any    `json:"SourceParameters,omitempty"`
	TargetParameters     any    `json:"TargetParameters,omitempty"`
	EnrichmentParameters any    `json:"EnrichmentParameters,omitempty"`
}

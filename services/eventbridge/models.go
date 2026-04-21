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
	compiledPattern    *compiledPattern
	indexKeys          []ruleIndexKey
}

// Target represents an EventBridge rule target.
type Target struct {
	InputTransformer *InputTransformer `json:"InputTransformer,omitempty"`
	ID               string            `json:"Id"`
	Arn              string            `json:"Arn"`
	RoleArn          string            `json:"RoleArn,omitempty"`
	Input            string            `json:"Input,omitempty"`
	InputPath        string            `json:"InputPath,omitempty"`
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

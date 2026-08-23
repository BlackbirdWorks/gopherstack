package stepfunctions

// StateMachine represents a Step Functions state machine.
// Timestamp fields use float64 (Unix epoch seconds) as required by the
// AWS JSON 1.0 protocol used by Step Functions.
type StateMachine struct {
	EncryptionConfiguration *EncryptionConfiguration `json:"encryptionConfiguration,omitempty"`
	TracingConfiguration    *TracingConfiguration    `json:"tracingConfiguration,omitempty"`
	LoggingConfiguration    *LoggingConfiguration    `json:"loggingConfiguration,omitempty"`
	Name                    string                   `json:"name"`
	StateMachineArn         string                   `json:"stateMachineArn"`
	Type                    string                   `json:"type"`
	Status                  string                   `json:"status"`
	Definition              string                   `json:"definition"`
	RoleArn                 string                   `json:"roleArn"`
	// RevisionId is an opaque token that changes every time Definition,
	// RoleArn, or the tracing/logging/encryption configuration changes --
	// AWS: "Use the revisionId parameter to compare between versions of a
	// state machine configuration ... without performing a diff of the
	// properties". Not set until the first Update (matches AWS returning a
	// null/absent revisionId on a freshly created, never-updated machine).
	RevisionID   string  `json:"revisionId,omitempty"`
	CreationDate float64 `json:"creationDate"`
	UpdatedDate  float64 `json:"updatedDate,omitempty"`
}

// EncryptionConfiguration configures KMS encryption for a state machine.
type EncryptionConfiguration struct {
	KMSKeyID                     string `json:"kmsKeyId,omitempty"`
	Type                         string `json:"type,omitempty"`
	KMSDataKeyReusePeriodSeconds int    `json:"kmsDataKeyReusePeriodSeconds,omitempty"`
}

// TracingConfiguration controls AWS X-Ray tracing for a state machine.
type TracingConfiguration struct {
	Enabled bool `json:"enabled"`
}

// LoggingConfiguration controls CloudWatch Logs export for a state machine.
type LoggingConfiguration struct {
	Level                string               `json:"level,omitempty"`
	Destinations         []LoggingDestination `json:"destinations,omitempty"`
	IncludeExecutionData bool                 `json:"includeExecutionData,omitempty"`
}

// LoggingDestination references a CloudWatch Logs log group destination.
type LoggingDestination struct {
	CloudWatchLogsLogGroup *CloudWatchLogsLogGroup `json:"cloudWatchLogsLogGroup,omitempty"`
}

// CloudWatchLogsLogGroup names a destination CloudWatch Logs log group.
type CloudWatchLogsLogGroup struct {
	LogGroupArn string `json:"logGroupArn,omitempty"`
}

// CloudWatchEventsExecutionDataDetails contains details about execution data.
type CloudWatchEventsExecutionDataDetails struct {
	Truncated bool `json:"truncated"`
}

// Execution represents a state machine execution.
//
// history holds the execution's history events inline (Phase 3.3: this
// replaces the backend's former separate `map[string]*HistoryEvent` history
// map). It is deliberately unexported: *Execution is returned directly as
// the wire body for DescribeExecution/StartExecution/etc., and AWS's real
// DescribeExecution response has no "history" field -- history is only ever
// retrieved via GetExecutionHistory. Being unexported also means it is
// skipped by every encoding/json.Marshal of *Execution, including the one
// [store.Table.Snapshot] would otherwise perform; persistence.go's
// executionSnapshot DTO adds it back as an ordinary exported field solely for
// the on-disk snapshot round trip. See persistence.go for details.
type Execution struct {
	InputDetails    *CloudWatchEventsExecutionDataDetails `json:"inputDetails,omitempty"`
	OutputDetails   *CloudWatchEventsExecutionDataDetails `json:"outputDetails,omitempty"`
	RedriveDate     *float64                              `json:"redriveDate,omitempty"`
	StopDate        *float64                              `json:"stopDate,omitempty"`
	Status          string                                `json:"status"`
	ExecutionArn    string                                `json:"executionArn"`
	StateMachineArn string                                `json:"stateMachineArn"`
	// StateMachineVersionArn is set only when this execution was started
	// with a version-qualified or alias-qualified stateMachineArn (AWS:
	// "If you start an execution from a StartExecution request without
	// specifying a state machine version or alias ARN, Step Functions
	// returns a null value").
	StateMachineVersionArn string `json:"stateMachineVersionArn,omitempty"`
	// StateMachineAliasArn is set only when this execution was started with
	// an alias-qualified stateMachineArn (null for version ARNs and
	// unqualified ARNs alike).
	StateMachineAliasArn string          `json:"stateMachineAliasArn,omitempty"`
	Name                 string          `json:"name"`
	Input                string          `json:"input,omitempty"`
	Output               string          `json:"output,omitempty"`
	Error                string          `json:"error,omitempty"`
	Cause                string          `json:"cause,omitempty"`
	TraceHeader          string          `json:"traceHeader,omitempty"`
	RedriveStatus        string          `json:"redriveStatus,omitempty"`
	RedriveStatusReason  string          `json:"redriveStatusReason,omitempty"`
	MapRunArn            string          `json:"mapRunArn,omitempty"`
	history              []*HistoryEvent `json:"-"`
	StartDate            float64         `json:"startDate"`
	RedriveCount         int             `json:"redriveCount,omitempty"`
}

// HistoryEvent represents a single event in execution history.
type HistoryEvent struct {
	StateEnteredEventDetails  *StateEnteredEventDetails  `json:"stateEnteredEventDetails,omitempty"`
	StateExitedEventDetails   *StateExitedEventDetails   `json:"stateExitedEventDetails,omitempty"`
	TaskScheduledEventDetails *TaskScheduledEventDetails `json:"taskScheduledEventDetails,omitempty"`
	TaskStartedEventDetails   *TaskStartedEventDetails   `json:"taskStartedEventDetails,omitempty"`
	TaskSubmittedEventDetails *TaskSubmittedEventDetails `json:"taskSubmittedEventDetails,omitempty"`
	TaskSucceededEventDetails *TaskSucceededEventDetails `json:"taskSucceededEventDetails,omitempty"`
	TaskFailedEventDetails    *TaskFailedEventDetails    `json:"taskFailedEventDetails,omitempty"`
	Type                      string                     `json:"type"` // e.g. "ExecutionStarted", "ExecutionSucceeded"
	Timestamp                 float64                    `json:"timestamp"`
	ID                        int64                      `json:"id"`
	PreviousEventID           int64                      `json:"previousEventId"`
}

// StateEnteredEventDetails holds details for state-entered events.
type StateEnteredEventDetails struct {
	Name  string `json:"name"`
	Input string `json:"input,omitempty"`
}

// StateExitedEventDetails holds details for state-exited events.
type StateExitedEventDetails struct {
	Name   string `json:"name"`
	Output string `json:"output,omitempty"`
}

// HistoryEventExecutionDataDetails contains details about execution data.
type HistoryEventExecutionDataDetails struct {
	Truncated bool `json:"truncated"`
}

// TaskScheduledEventDetails holds details for TaskScheduled history events.
// Resource/ResourceType/Region/Parameters are all required on the real
// TaskScheduledEventDetails (sfn@v1.45.4 types.go:1311-1339).
type TaskScheduledEventDetails struct {
	TimeoutInSeconds   *int64 `json:"timeoutInSeconds,omitempty"`
	HeartbeatInSeconds *int64 `json:"heartbeatInSeconds,omitempty"`
	Resource           string `json:"resource"`
	ResourceType       string `json:"resourceType"`
	Region             string `json:"region"`
	Parameters         string `json:"parameters"`
}

// TaskStartedEventDetails holds details for TaskStarted history events.
type TaskStartedEventDetails struct {
	Resource     string `json:"resource,omitempty"`
	ResourceType string `json:"resourceType,omitempty"`
}

// TaskSubmittedEventDetails holds details for TaskSubmitted history events.
type TaskSubmittedEventDetails struct {
	OutputDetails *HistoryEventExecutionDataDetails `json:"outputDetails,omitempty"`
	Resource      string                            `json:"resource,omitempty"`
	ResourceType  string                            `json:"resourceType,omitempty"`
	Output        string                            `json:"output,omitempty"`
}

// TaskSucceededEventDetails holds details for TaskSucceeded history events.
// Resource/ResourceType are required (sfn@v1.45.4 types.go:1431-1450).
type TaskSucceededEventDetails struct {
	OutputDetails *HistoryEventExecutionDataDetails `json:"outputDetails,omitempty"`
	Resource      string                            `json:"resource"`
	ResourceType  string                            `json:"resourceType"`
	Output        string                            `json:"output,omitempty"`
}

// TaskFailedEventDetails holds details for TaskFailed history events.
// Resource/ResourceType are required (sfn@v1.45.4 types.go:1289-1307).
type TaskFailedEventDetails struct {
	Error        string `json:"error,omitempty"`
	Cause        string `json:"cause,omitempty"`
	Resource     string `json:"resource"`
	ResourceType string `json:"resourceType"`
}

// Activity represents an AWS Step Functions activity resource.
type Activity struct {
	EncryptionConfiguration *EncryptionConfiguration `json:"encryptionConfiguration,omitempty"`
	Name                    string                   `json:"name"`
	ActivityArn             string                   `json:"activityArn"`
	CreationDate            float64                  `json:"creationDate"`
}

// ActivityTask represents a task polled from an activity queue.
type ActivityTask struct {
	TaskToken string `json:"taskToken"`
	Input     string `json:"input"`
}

// SyncExecutionResult holds the result of a synchronous (EXPRESS) execution.
type SyncExecutionResult struct {
	ExecutionArn    string  `json:"executionArn"`
	StateMachineArn string  `json:"stateMachineArn"`
	Name            string  `json:"name"`
	Status          string  `json:"status"`
	Input           string  `json:"input,omitempty"`
	Output          string  `json:"output,omitempty"`
	Error           string  `json:"error,omitempty"`
	Cause           string  `json:"cause,omitempty"`
	StopDate        float64 `json:"stopDate"`
	StartDate       float64 `json:"startDate"`
}

// StateMachineVersion represents an immutable versioned snapshot of a state machine.
type StateMachineVersion struct {
	StateMachineVersionArn string  `json:"stateMachineVersionArn"`
	StateMachineArn        string  `json:"stateMachineArn"`
	Name                   string  `json:"name"`
	Definition             string  `json:"definition"`
	RoleArn                string  `json:"roleArn"`
	Type                   string  `json:"type"`
	Status                 string  `json:"status"`
	Description            string  `json:"description,omitempty"`
	RevisionID             string  `json:"revisionId,omitempty"`
	CreationDate           float64 `json:"creationDate"`
}

// StateMachineAlias represents a routing alias for one or more state machine versions.
type StateMachineAlias struct {
	StateMachineAliasArn string               `json:"stateMachineAliasArn"`
	Name                 string               `json:"name"`
	Description          string               `json:"description,omitempty"`
	RoutingConfiguration []AliasRoutingConfig `json:"routingConfiguration"`
	CreationDate         float64              `json:"creationDate"`
	UpdatedDate          float64              `json:"updatedDate,omitempty"`
}

// AliasRoutingConfig represents a weighted routing target for a state machine alias.
type AliasRoutingConfig struct {
	StateMachineVersionArn string `json:"stateMachineVersionArn"`
	Weight                 int    `json:"weight"`
}

// MapRun represents an AWS Step Functions Map Run (a Map state parallel execution group).
type MapRun struct {
	StopDate        *float64         `json:"stopDate,omitempty"`
	RedriveDate     *float64         `json:"redriveDate,omitempty"`
	MapRunArn       string           `json:"mapRunArn"`
	ExecutionArn    string           `json:"executionArn"`
	StateMachineArn string           `json:"stateMachineArn"`
	Status          string           `json:"status"`
	ItemCounts      MapRunItemCounts `json:"itemCounts"`
	// ExecutionCounts is required on DescribeMapRunOutput (sfn@v1.45.4
	// api_op_DescribeMapRun.go:57) but this backend never spawns separate
	// child workflow executions per Map item (DISTRIBUTED mode runs inline,
	// same as INLINE) -- so it stays genuinely zero-valued rather than
	// fabricating a mapping onto ItemCounts. See PARITY.md.
	ExecutionCounts            MapRunExecutionCounts `json:"executionCounts"`
	StartDate                  float64               `json:"startDate"`
	ToleratedFailurePercentage float64               `json:"toleratedFailurePercentage,omitempty"`
	MaxConcurrency             int                   `json:"maxConcurrency,omitempty"`
	ToleratedFailureCount      int                   `json:"toleratedFailureCount,omitempty"`
	RedriveCount               int                   `json:"redriveCount,omitempty"`
}

// MapRunItemCounts holds item-level counts for a Map Run.
type MapRunItemCounts struct {
	Total                 int `json:"total"`
	Succeeded             int `json:"succeeded"`
	Failed                int `json:"failed"`
	Pending               int `json:"pending"`
	Running               int `json:"running"`
	Aborted               int `json:"aborted"`
	TimedOut              int `json:"timedOut"`
	ResultsWritten        int `json:"resultsWritten"`
	FailuresNotRedrivable int `json:"failuresNotRedrivable,omitempty"`
	PendingRedrive        int `json:"pendingRedrive,omitempty"`
}

// MapRunExecutionCounts holds child-workflow-execution counts for a Map Run
// (sfn@v1.45.4 types.go:841-906). Shape mirrors MapRunItemCounts.
type MapRunExecutionCounts struct {
	Total                 int `json:"total"`
	Succeeded             int `json:"succeeded"`
	Failed                int `json:"failed"`
	Pending               int `json:"pending"`
	Running               int `json:"running"`
	Aborted               int `json:"aborted"`
	TimedOut              int `json:"timedOut"`
	ResultsWritten        int `json:"resultsWritten"`
	FailuresNotRedrivable int `json:"failuresNotRedrivable,omitempty"`
	PendingRedrive        int `json:"pendingRedrive,omitempty"`
}

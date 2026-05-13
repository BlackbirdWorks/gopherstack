package stepfunctions

// StateMachine represents a Step Functions state machine.
// Timestamp fields use float64 (Unix epoch seconds) as required by the
// AWS JSON 1.0 protocol used by Step Functions.
type StateMachine struct {
	EncryptionConfiguration *EncryptionConfiguration `json:"encryptionConfiguration,omitempty"`
	Name                    string                   `json:"name"`
	StateMachineArn         string                   `json:"stateMachineArn"`
	Type                    string                   `json:"type"`
	Status                  string                   `json:"status"`
	Definition              string                   `json:"definition"`
	RoleArn                 string                   `json:"roleArn"`
	CreationDate            float64                  `json:"creationDate"`
	UpdatedDate             float64                  `json:"updatedDate,omitempty"`
}

// EncryptionConfiguration configures KMS encryption for a state machine.
type EncryptionConfiguration struct {
	KMSKeyID                     string `json:"kmsKeyId,omitempty"`
	Type                         string `json:"type,omitempty"`
	KMSDataKeyReusePeriodSeconds int    `json:"kmsDataKeyReusePeriodSeconds,omitempty"`
}

// Execution represents a state machine execution.
type Execution struct {
	StopDate        *float64 `json:"stopDate,omitempty"`
	ExecutionArn    string   `json:"executionArn"`
	StateMachineArn string   `json:"stateMachineArn"`
	Name            string   `json:"name"`
	Status          string   `json:"status"` // "RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"
	Input           string   `json:"input,omitempty"`
	Output          string   `json:"output,omitempty"`
	Error           string   `json:"error,omitempty"`
	Cause           string   `json:"cause,omitempty"`
	StartDate       float64  `json:"startDate"`
}

// HistoryEvent represents a single event in execution history.
type HistoryEvent struct {
	StateEnteredEventDetails *StateEnteredEventDetails `json:"stateEnteredEventDetails,omitempty"`
	StateExitedEventDetails  *StateExitedEventDetails  `json:"stateExitedEventDetails,omitempty"`
	Type                     string                    `json:"type"` // e.g. "ExecutionStarted", "ExecutionSucceeded"
	Timestamp                float64                   `json:"timestamp"`
	ID                       int64                     `json:"id"`
	PreviousEventID          int64                     `json:"previousEventId"`
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

// Activity represents an AWS Step Functions activity resource.
type Activity struct {
	Name         string  `json:"name"`
	ActivityArn  string  `json:"activityArn"`
	CreationDate float64 `json:"creationDate"`
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

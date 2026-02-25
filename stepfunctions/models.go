package stepfunctions

// StateMachine represents a Step Functions state machine.
// Timestamp fields use float64 (Unix epoch seconds) as required by the
// AWS JSON 1.0 protocol used by Step Functions.
type StateMachine struct {
	Name            string  `json:"name"`
	StateMachineArn string  `json:"stateMachineArn"`
	Type            string  `json:"type"`   // "STANDARD" or "EXPRESS"
	Status          string  `json:"status"` // "ACTIVE", "DELETING"
	Definition      string  `json:"definition"`
	RoleArn         string  `json:"roleArn"`
	CreationDate    float64 `json:"creationDate"`
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

// ErrorResponse is the standard error format.
type ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

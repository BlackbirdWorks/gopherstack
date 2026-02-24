package stepfunctions

import "time"

// StateMachine represents a Step Functions state machine.
type StateMachine struct {
	CreationDate    time.Time `json:"creationDate"`
	Name            string    `json:"name"`
	StateMachineArn string    `json:"stateMachineArn"`
	Type            string    `json:"type"`   // "STANDARD" or "EXPRESS"
	Status          string    `json:"status"` // "ACTIVE", "DELETING"
	Definition      string    `json:"definition"`
	RoleArn         string    `json:"roleArn"`
}

// Execution represents a state machine execution.
type Execution struct {
	StartDate       time.Time  `json:"startDate"`
	StopDate        *time.Time `json:"stopDate,omitempty"`
	ExecutionArn    string     `json:"executionArn"`
	StateMachineArn string     `json:"stateMachineArn"`
	Name            string     `json:"name"`
	Status          string     `json:"status"` // "RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"
	Input           string     `json:"input,omitempty"`
	Output          string     `json:"output,omitempty"`
	Error           string     `json:"error,omitempty"`
	Cause           string     `json:"cause,omitempty"`
}

// HistoryEvent represents a single event in execution history.
type HistoryEvent struct {
	Timestamp       time.Time `json:"timestamp"`
	Type            string    `json:"type"` // e.g. "ExecutionStarted", "ExecutionSucceeded"
	ID              int64     `json:"id"`
	PreviousEventID int64     `json:"previousEventId"`
}

// ErrorResponse is the standard error format.
type ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

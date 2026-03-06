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

// FailedEntry describes a target or event that failed to process.

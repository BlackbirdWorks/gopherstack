package swf

import (
	"encoding/json"
	"maps"
	"strings"
)

const (
	// maxWorkflowExecutions is the maximum number of workflow executions retained.
	maxWorkflowExecutions = 10_000

	statusDeprecated     = "DEPRECATED"
	statusRegistered     = "REGISTERED"
	statusRunning        = "RUNNING"
	statusTerminated     = "TERMINATED"
	statusCanceled       = "CANCELED"
	statusCompleted      = "COMPLETED"
	statusFailed         = "FAILED"
	statusTimedOut       = "TIMED_OUT"
	statusContinuedAsNew = "CONTINUED_AS_NEW"

	defaultAccountID = "123456789012"
	defaultRegion    = "us-east-1"
	maxTags          = 50
	maxTagKeyLen     = 128
	maxTagValueLen   = 256

	milliDivisor = 1000.0

	retentionNone     = "NONE"
	attrDetails       = "details"
	attrInput         = "input"
	attrReason        = "reason"
	attrName          = "name"
	attrScheduledEvID = "scheduledEventId"
	attrStartedEvID   = "startedEventId"

	// Attribute-map key literals shared across decision_tasks.go,
	// decision_orchestration.go, workflow_executions.go, activity_tasks.go,
	// and signals.go's event-attribute/history-event-attribute maps.
	attrWorkflowID      = "workflowId"
	attrRunID           = "runId"
	attrCause           = "cause"
	attrControl         = "control"
	attrChildPolicy     = "childPolicy"
	attrTaskList        = "taskList"
	attrWorkflowType    = "workflowType"
	attrVersion         = "version"
	attrExecToCloseTO   = "executionStartToCloseTimeout"
	attrTaskToCloseTO   = "taskStartToCloseTimeout"
	attrLambdaRole      = "lambdaRole"
	attrTagList         = "tagList"
	attrDTCEventID      = "decisionTaskCompletedEventId"
	attrResult          = "result"
	attrSignalName      = "signalName"
	attrTimerID         = "timerId"
	attrInitiatedEvID   = "initiatedEventId"
	attrWorkflowExec    = "workflowExecution"
	causeOpNotPermitted = "OPERATION_NOT_PERMITTED"

	// attrTimeoutType/timeoutTypeStartToClose back
	// WorkflowExecutionTimedOutEventAttributes.timeoutType (timeout_sweep.go)
	// -- confirmed against aws-sdk-go-v2/service/swf@v1.37.4's
	// WorkflowExecutionTimeoutType enum, whose only defined value is
	// START_TO_CLOSE.
	attrTimeoutType         = "timeoutType"
	timeoutTypeStartToClose = "START_TO_CLOSE"

	// Child policy values, shared by StartWorkflowExecution/registration
	// defaulting (store.go, workflow_types.go) and the TerminateWorkflowExecution
	// child-policy cascade (workflow_executions.go).
	childPolicyTerminate     = "TERMINATE"
	childPolicyRequestCancel = "REQUEST_CANCEL"
	childPolicyAbandon       = "ABANDON"

	causeChildPolicyApplied = "CHILD_POLICY_APPLIED"
	causeOperatorInitiated  = "OPERATOR_INITIATED"
)

// WorkflowTypeDefaults holds the registered defaults for a workflow type.
type WorkflowTypeDefaults struct {
	DefaultTaskList                     string `json:"defaultTaskList,omitempty"`
	DefaultTaskPriority                 string `json:"defaultTaskPriority,omitempty"`
	DefaultTaskStartToCloseTimeout      string `json:"defaultTaskStartToCloseTimeout,omitempty"`
	DefaultExecutionStartToCloseTimeout string `json:"defaultExecutionStartToCloseTimeout,omitempty"`
	DefaultChildPolicy                  string `json:"defaultChildPolicy,omitempty"`
	DefaultLambdaRole                   string `json:"defaultLambdaRole,omitempty"`
}

// ActivityTypeDefaults holds the registered defaults for an activity type.
type ActivityTypeDefaults struct {
	DefaultTaskList                   string `json:"defaultTaskList,omitempty"`
	DefaultTaskPriority               string `json:"defaultTaskPriority,omitempty"`
	DefaultTaskHeartbeatTimeout       string `json:"defaultTaskHeartbeatTimeout,omitempty"`
	DefaultTaskScheduleToCloseTimeout string `json:"defaultTaskScheduleToCloseTimeout,omitempty"`
	DefaultTaskScheduleToStartTimeout string `json:"defaultTaskScheduleToStartTimeout,omitempty"`
	DefaultTaskStartToCloseTimeout    string `json:"defaultTaskStartToCloseTimeout,omitempty"`
}

// HistoryEvent is a single event in a workflow execution's history.
// The Attributes map holds the event-type-specific payload which is serialised
// under the key "<eventType>EventAttributes" per the AWS SWF JSON protocol.
type HistoryEvent struct {
	Attributes map[string]any `json:"-"`
	EventType  string         `json:"eventType"`
	EventID    int64          `json:"eventId"`
	Timestamp  float64        `json:"eventTimestamp"`
}

// MarshalJSON emits the event-type-specific attributes alongside the standard fields.
func (e HistoryEvent) MarshalJSON() ([]byte, error) {
	m := map[string]any{
		"eventType":      e.EventType,
		"eventId":        e.EventID,
		"eventTimestamp": e.Timestamp,
	}
	maps.Copy(m, e.Attributes)

	return json.Marshal(m)
}

// UnmarshalJSON restores a HistoryEvent from its JSON representation,
// capturing any unknown keys (the attributes block) into Attributes.
func (e *HistoryEvent) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if v, ok := raw["eventType"]; ok {
		_ = json.Unmarshal(v, &e.EventType)
	}
	if v, ok := raw["eventId"]; ok {
		_ = json.Unmarshal(v, &e.EventID)
	}
	if v, ok := raw["eventTimestamp"]; ok {
		_ = json.Unmarshal(v, &e.Timestamp)
	}
	known := map[string]bool{
		"eventType": true, "eventId": true, "eventTimestamp": true,
	}
	e.Attributes = make(map[string]any)
	for k, v := range raw {
		if !known[k] {
			var x any
			_ = json.Unmarshal(v, &x)
			e.Attributes[k] = x
		}
	}

	return nil
}

// eventAttrKey returns the attribute key name for a given event type,
// e.g. "WorkflowExecutionStarted" → "workflowExecutionStartedEventAttributes".
func eventAttrKey(eventType string) string {
	if eventType == "" {
		return ""
	}

	return strings.ToLower(eventType[:1]) + eventType[1:] + "EventAttributes"
}

// ActivityTaskActivityType is the activity type reference within an activity task.
type ActivityTaskActivityType struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ActivityTask represents a pending activity task returned by PollForActivityTask.
type ActivityTask struct {
	TaskToken        string                   `json:"taskToken"`
	ActivityID       string                   `json:"activityId"`
	ActivityType     ActivityTaskActivityType `json:"activityType"`
	Input            string                   `json:"input,omitempty"`
	WorkflowID       string                   `json:"workflowId"`
	RunID            string                   `json:"runId"`
	StartedEventID   int64                    `json:"startedEventId"`
	ScheduledEventID int64                    `json:"scheduledEventId"`
}

// DecisionTask represents a pending decision task returned by PollForDecisionTask.
type DecisionTask struct {
	TaskToken              string         `json:"taskToken"`
	WorkflowID             string         `json:"workflowId"`
	RunID                  string         `json:"runId"`
	NextPageToken          string         `json:"nextPageToken,omitempty"`
	WorkflowTypeName       string         `json:"workflowTypeName,omitempty"`
	WorkflowTypeVersion    string         `json:"workflowTypeVersion,omitempty"`
	Events                 []HistoryEvent `json:"events"`
	StartedEventID         int64          `json:"startedEventId"`
	PreviousStartedEventID int64          `json:"previousStartedEventId"`
	ScheduledEventID       int64          `json:"-"`
}

// Domain represents an SWF domain.
type Domain struct {
	Name                                   string `json:"name"`
	Description                            string `json:"description"`
	Status                                 string `json:"status"` // REGISTERED or DEPRECATED
	Arn                                    string `json:"arn,omitempty"`
	WorkflowExecutionRetentionPeriodInDays string `json:"workflowExecutionRetentionPeriodInDays"`
}

// ActivityType represents an SWF activity type.
type ActivityType struct {
	Defaults        ActivityTypeDefaults `json:"defaults"`
	Description     string               `json:"description"`
	Domain          string               `json:"domain"`
	Name            string               `json:"name"`
	Version         string               `json:"version"`
	Status          string               `json:"status"`
	CreationDate    float64              `json:"creationDate"`
	DeprecationDate float64              `json:"deprecationDate,omitempty"`
}

// WorkflowType represents an SWF workflow type.
type WorkflowType struct {
	Defaults        WorkflowTypeDefaults `json:"defaults"`
	Description     string               `json:"description"`
	Domain          string               `json:"domain"`
	Name            string               `json:"name"`
	Version         string               `json:"version"`
	Status          string               `json:"status"`
	CreationDate    float64              `json:"creationDate"`
	DeprecationDate float64              `json:"deprecationDate,omitempty"`
}

// WorkflowExecution represents an SWF workflow execution.
type WorkflowExecution struct {
	TimerStartedEventIDs         map[string]int64 `json:"-"`
	ParentRunID                  string           `json:"parentRunID,omitempty"`
	WorkflowTypeName             string           `json:"workflowTypeName,omitempty"`
	TaskList                     string           `json:"taskList,omitempty"`
	CloseStatus                  string           `json:"closeStatus,omitempty"`
	LatestExecutionContext       string           `json:"latestExecutionContext,omitempty"`
	TaskStartToCloseTimeout      string           `json:"taskStartToCloseTimeout,omitempty"`
	ChildPolicy                  string           `json:"childPolicy,omitempty"`
	WorkflowID                   string           `json:"workflowID"`
	Input                        string           `json:"input,omitempty"`
	LambdaRole                   string           `json:"lambdaRole,omitempty"`
	RunID                        string           `json:"runID"`
	Status                       string           `json:"status"`
	TaskPriority                 string           `json:"taskPriority,omitempty"`
	ExecutionStartToCloseTimeout string           `json:"executionStartToCloseTimeout,omitempty"`
	ParentWorkflowID             string           `json:"parentWorkflowID,omitempty"`
	WorkflowTypeVersion          string           `json:"workflowTypeVersion,omitempty"`
	Domain                       string           `json:"domain"`
	OpenTimerIDs                 []string         `json:"openTimerIDs,omitempty"`
	TagList                      []string         `json:"tagList,omitempty"`
	StartTimestamp               float64          `json:"startTimestamp"`
	CloseTimestamp               float64          `json:"closeTimestamp,omitempty"`
	ParentInitiatedEventID       int64            `json:"parentInitiatedEventID,omitempty"`
	ParentStartedEventID         int64            `json:"parentStartedEventID,omitempty"`
	CancelRequested              bool             `json:"cancelRequested,omitempty"`
}

// StartWorkflowExecutionInput holds all parameters for starting a workflow execution.
type StartWorkflowExecutionInput struct {
	Input                        string
	WorkflowID                   string
	RunID                        string
	WorkflowTypeName             string
	WorkflowTypeVersion          string
	TaskList                     string
	Domain                       string
	ChildPolicy                  string
	LambdaRole                   string
	ExecutionStartToCloseTimeout string
	TaskStartToCloseTimeout      string
	TaskPriority                 string
	TagList                      []string
}

// activeActivityTaskRecord tracks an activity task that has been dispatched to a poller.
// TaskToken has no wire-visible home on this record (the token is the caller-facing
// identity, never round-tripped through an AWS response body here); it exists purely so
// store.Table's keyFn can derive a key from the value (see store_setup.go). It is tagged
// json:"-" because activeActivityTasks is a "dirty" table -- persistence.go instead
// round-trips it through a dedicated activeActivityTaskDTO that carries the token as a
// real JSON field, so it survives the round trip despite being excluded here.
type activeActivityTaskRecord struct {
	ActivityType     ActivityTaskActivityType
	Domain           string
	WorkflowID       string
	RunID            string
	ActivityID       string
	TaskList         string
	TaskToken        string `json:"-"`
	ScheduledEventID int64
	StartedEventID   int64
}

// activeDecisionTaskRecord tracks a decision task token dispatched to a poller.
// TaskToken carries the same store.Table keyFn / persistence caveats as
// [activeActivityTaskRecord.TaskToken] above.
type activeDecisionTaskRecord struct {
	Domain           string
	WorkflowID       string
	RunID            string
	TaskToken        string `json:"-"`
	ScheduledEventID int64
	StartedEventID   int64
}

// CompleteWorkflowExecutionDecisionAttrs holds attributes for CompleteWorkflowExecution.
type CompleteWorkflowExecutionDecisionAttrs struct {
	Result string
}

// FailWorkflowExecutionDecisionAttrs holds attributes for FailWorkflowExecution.
type FailWorkflowExecutionDecisionAttrs struct {
	Reason  string
	Details string
}

// CancelWorkflowExecutionDecisionAttrs holds attributes for CancelWorkflowExecution.
type CancelWorkflowExecutionDecisionAttrs struct {
	Details string
}

// ScheduleActivityTaskDecisionAttrs holds attributes for ScheduleActivityTask.
type ScheduleActivityTaskDecisionAttrs struct {
	ActivityType           ActivityTaskActivityType
	ActivityID             string
	Input                  string
	TaskList               string
	ScheduleToCloseTimeout string
	ScheduleToStartTimeout string
	StartToCloseTimeout    string
	HeartbeatTimeout       string
}

// RequestCancelActivityTaskDecisionAttrs holds attributes for RequestCancelActivityTask.
type RequestCancelActivityTaskDecisionAttrs struct {
	ActivityID string
}

// StartTimerDecisionAttrs holds attributes for StartTimer.
type StartTimerDecisionAttrs struct {
	TimerID            string
	StartToFireTimeout string
}

// CancelTimerDecisionAttrs holds attributes for CancelTimer.
type CancelTimerDecisionAttrs struct {
	TimerID string
}

// RecordMarkerDecisionAttrs holds attributes for RecordMarker.
type RecordMarkerDecisionAttrs struct {
	MarkerName string
	Details    string
}

// WorkflowTypeRef identifies a workflow type by name/version within a decision's
// attributes (mirrors the wire-level workflowType field shape).
type WorkflowTypeRef struct {
	Name    string
	Version string
}

// ContinueAsNewWorkflowExecutionDecisionAttrs holds attributes for
// ContinueAsNewWorkflowExecution. All fields are optional overrides: an empty
// field falls back to the current execution's WorkflowType-registered
// defaults, exactly like StartWorkflowExecution.
type ContinueAsNewWorkflowExecutionDecisionAttrs struct {
	Input                        string
	ExecutionStartToCloseTimeout string
	TaskList                     string
	TaskStartToCloseTimeout      string
	TaskPriority                 string
	ChildPolicy                  string
	LambdaRole                   string
	WorkflowTypeVersion          string
	TagList                      []string
}

// StartChildWorkflowExecutionDecisionAttrs holds attributes for StartChildWorkflowExecution.
type StartChildWorkflowExecutionDecisionAttrs struct {
	WorkflowID                   string
	WorkflowType                 WorkflowTypeRef
	Control                      string
	Input                        string
	ExecutionStartToCloseTimeout string
	TaskList                     string
	TaskPriority                 string
	TaskStartToCloseTimeout      string
	ChildPolicy                  string
	LambdaRole                   string
	TagList                      []string
}

// SignalExternalWorkflowExecutionDecisionAttrs holds attributes for SignalExternalWorkflowExecution.
type SignalExternalWorkflowExecutionDecisionAttrs struct {
	WorkflowID string
	RunID      string
	SignalName string
	Input      string
	Control    string
}

// RequestCancelExternalWorkflowExecutionDecisionAttrs holds attributes for
// RequestCancelExternalWorkflowExecution.
type RequestCancelExternalWorkflowExecutionDecisionAttrs struct {
	WorkflowID string
	RunID      string
	Control    string
}

// Decision represents a single decision returned by a decider.
type Decision struct {
	CompleteWorkflowExecutionAttrs              *CompleteWorkflowExecutionDecisionAttrs
	FailWorkflowExecutionAttrs                  *FailWorkflowExecutionDecisionAttrs
	CancelWorkflowExecutionAttrs                *CancelWorkflowExecutionDecisionAttrs
	ScheduleActivityTaskAttrs                   *ScheduleActivityTaskDecisionAttrs
	RequestCancelActivityTaskAttrs              *RequestCancelActivityTaskDecisionAttrs
	StartTimerAttrs                             *StartTimerDecisionAttrs
	CancelTimerAttrs                            *CancelTimerDecisionAttrs
	RecordMarkerAttrs                           *RecordMarkerDecisionAttrs
	ContinueAsNewWorkflowExecutionAttrs         *ContinueAsNewWorkflowExecutionDecisionAttrs
	StartChildWorkflowExecutionAttrs            *StartChildWorkflowExecutionDecisionAttrs
	SignalExternalWorkflowExecutionAttrs        *SignalExternalWorkflowExecutionDecisionAttrs
	RequestCancelExternalWorkflowExecutionAttrs *RequestCancelExternalWorkflowExecutionDecisionAttrs
	DecisionType                                string
}

package swf

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var (
	// ErrNotFound is returned when a resource is not found.
	ErrNotFound = awserr.New("UnknownResourceFault", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("DomainAlreadyExistsFault", awserr.ErrAlreadyExists)
	// ErrDeprecated is returned when trying to re-register a deprecated domain.
	ErrDeprecated = errors.New("DomainDeprecatedFault")
	// ErrTypeAlreadyExists is returned when a workflow or activity type already exists.
	ErrTypeAlreadyExists = errors.New("TypeAlreadyExistsFault")
	// ErrTypeDeprecated is returned when a type is already deprecated.
	ErrTypeDeprecated = errors.New("TypeDeprecatedFault")
	// ErrValidation is returned when a request parameter fails validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrTooManyTags is returned when tag limits are exceeded.
	ErrTooManyTags = awserr.New("TooManyTagsFault", awserr.ErrInvalidParameter)
	// ErrOperationNotPermitted is returned for disallowed operations.
	ErrOperationNotPermitted = awserr.New("OperationNotPermittedFault", awserr.ErrConflict)
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
)

// swfARNRegex matches arn:aws:swf:{region}:{account}:/domain/{name}.
var swfARNRegex = regexp.MustCompile(`^arn:aws:swf:[^:]+:[^:]+:/domain/(.+)$`)

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
}

// Domain represents an SWF domain.
type Domain struct {
	Name                                   string `json:"name"`
	Description                            string `json:"description"`
	Status                                 string `json:"status"` // REGISTERED or DEPRECATED
	WorkflowExecutionRetentionPeriodInDays string `json:"workflowExecutionRetentionPeriodInDays"`
}

// ActivityType represents an SWF activity type.
type ActivityType struct {
	Defaults     ActivityTypeDefaults `json:"defaults"`
	Description  string               `json:"description"`
	Domain       string               `json:"domain"`
	Name         string               `json:"name"`
	Version      string               `json:"version"`
	Status       string               `json:"status"`
	CreationDate float64              `json:"creationDate"`
}

// WorkflowType represents an SWF workflow type.
type WorkflowType struct {
	Defaults     WorkflowTypeDefaults `json:"defaults"`
	Description  string               `json:"description"`
	Domain       string               `json:"domain"`
	Name         string               `json:"name"`
	Version      string               `json:"version"`
	Status       string               `json:"status"`
	CreationDate float64              `json:"creationDate"`
}

// WorkflowExecution represents an SWF workflow execution.
type WorkflowExecution struct {
	WorkflowTypeVersion          string   `json:"workflowTypeVersion,omitempty"`
	LambdaRole                   string   `json:"lambdaRole,omitempty"`
	RunID                        string   `json:"runID"`
	TaskList                     string   `json:"taskList,omitempty"`
	CloseStatus                  string   `json:"closeStatus,omitempty"`
	LatestExecutionContext       string   `json:"latestExecutionContext,omitempty"`
	TaskPriority                 string   `json:"taskPriority,omitempty"`
	WorkflowTypeName             string   `json:"workflowTypeName,omitempty"`
	WorkflowID                   string   `json:"workflowID"`
	Input                        string   `json:"input,omitempty"`
	Status                       string   `json:"status"`
	TaskStartToCloseTimeout      string   `json:"taskStartToCloseTimeout,omitempty"`
	ChildPolicy                  string   `json:"childPolicy,omitempty"`
	Domain                       string   `json:"domain"`
	ExecutionStartToCloseTimeout string   `json:"executionStartToCloseTimeout,omitempty"`
	TagList                      []string `json:"tagList,omitempty"`
	CloseTimestamp               float64  `json:"closeTimestamp,omitempty"`
	StartTimestamp               float64  `json:"startTimestamp"`
	CancelRequested              bool     `json:"cancelRequested,omitempty"`
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
type activeActivityTaskRecord struct {
	ActivityType     ActivityTaskActivityType
	Domain           string
	WorkflowID       string
	RunID            string
	ActivityID       string
	TaskList         string
	ScheduledEventID int64
	StartedEventID   int64
}

// activeDecisionTaskRecord tracks a decision task token dispatched to a poller.
type activeDecisionTaskRecord struct {
	Domain     string
	WorkflowID string
	RunID      string
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

// Decision represents a single decision returned by a decider.
type Decision struct {
	CompleteWorkflowExecutionAttrs *CompleteWorkflowExecutionDecisionAttrs
	FailWorkflowExecutionAttrs     *FailWorkflowExecutionDecisionAttrs
	CancelWorkflowExecutionAttrs   *CancelWorkflowExecutionDecisionAttrs
	ScheduleActivityTaskAttrs      *ScheduleActivityTaskDecisionAttrs
	DecisionType                   string
}

// InMemoryBackend is the in-memory store for SWF resources.
type InMemoryBackend struct {
	domains             map[string]*Domain
	workflows           map[string]*WorkflowType             // key: domain+":"+name+":"+version
	activities          map[string]*ActivityType             // key: domain+":"+name+":"+version
	executions          map[string]*WorkflowExecution        // key: domain+":"+workflowID
	history             map[string][]HistoryEvent            // key: domain+":"+workflowID
	activityQueues      map[string][]*ActivityTask           // key: domain+":"+taskList
	decisionQueues      map[string][]*DecisionTask           // key: domain+":"+taskList
	activeActivityTasks map[string]*activeActivityTaskRecord // key: taskToken
	activeDecisionTasks map[string]*activeDecisionTaskRecord // key: taskToken
	tags                map[string]map[string]string         // key: resourceARN
	mu                  *lockmetrics.RWMutex
	executionOrder      []string // FIFO order of execution keys for eviction
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		domains:             make(map[string]*Domain),
		workflows:           make(map[string]*WorkflowType),
		activities:          make(map[string]*ActivityType),
		executions:          make(map[string]*WorkflowExecution),
		history:             make(map[string][]HistoryEvent),
		activityQueues:      make(map[string][]*ActivityTask),
		decisionQueues:      make(map[string][]*DecisionTask),
		activeActivityTasks: make(map[string]*activeActivityTaskRecord),
		activeDecisionTasks: make(map[string]*activeDecisionTaskRecord),
		tags:                make(map[string]map[string]string),
		mu:                  lockmetrics.New("swf"),
	}
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.domains = make(map[string]*Domain)
	b.workflows = make(map[string]*WorkflowType)
	b.activities = make(map[string]*ActivityType)
	b.executions = make(map[string]*WorkflowExecution)
	b.history = make(map[string][]HistoryEvent)
	b.activityQueues = make(map[string][]*ActivityTask)
	b.decisionQueues = make(map[string][]*DecisionTask)
	b.activeActivityTasks = make(map[string]*activeActivityTaskRecord)
	b.activeDecisionTasks = make(map[string]*activeDecisionTaskRecord)
	b.tags = make(map[string]map[string]string)
	b.executionOrder = nil
}

// appendHistoryEventLocked appends a history event for a workflow execution.
// attrs is the event-type-specific attributes map; pass nil if none.
// Caller must hold the write lock.
func (b *InMemoryBackend) appendHistoryEventLocked(
	domain, workflowID, eventType string,
	attrs map[string]any,
) int64 {
	key := domain + ":" + workflowID
	events := b.history[key]
	eventID := int64(len(events)) + 1
	ev := HistoryEvent{
		EventID:   eventID,
		EventType: eventType,
		Timestamp: float64(time.Now().UnixMilli()) / milliDivisor,
	}
	if len(attrs) > 0 {
		ev.Attributes = attrs
	}
	b.history[key] = append(events, ev)

	return eventID
}

// AccountID returns the account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return defaultAccountID }

// domainARN constructs the SWF ARN for a domain.
func domainARN(region, account, name string) string {
	return fmt.Sprintf("arn:aws:swf:%s:%s:/domain/%s", region, account, name)
}

// validateChildPolicy returns an error if policy is not a valid SWF child policy.
// An empty string is allowed (means "use default").
func validateChildPolicy(policy string) error {
	switch policy {
	case "", "TERMINATE", "REQUEST_CANCEL", "ABANDON":
		return nil
	}

	return fmt.Errorf(
		"%w: invalid childPolicy %q, must be TERMINATE, REQUEST_CANCEL, or ABANDON",
		ErrValidation,
		policy,
	)
}

// validateDuration returns an error if d is not a valid SWF duration string.
// Valid values: positive integer (seconds) or "NONE". Empty string means unset.
func validateDuration(d string) error {
	if d == "" || d == retentionNone {
		return nil
	}
	n, err := strconv.Atoi(d)
	if err != nil || n < 0 {
		return fmt.Errorf(
			"%w: invalid duration %q, must be a non-negative integer or NONE",
			ErrValidation,
			d,
		)
	}

	return nil
}

// validateRetention returns an error if retention is not a valid SWF retention period.
// Valid values: "0"-"90" or "NONE".
func validateRetention(retention string) error {
	if retention == "" || retention == retentionNone {
		return nil
	}
	n, err := strconv.Atoi(retention)
	if err != nil || n < 0 || n > 90 {
		return fmt.Errorf(
			"%w: invalid workflowExecutionRetentionPeriodInDays %q, must be 0-90 or NONE",
			ErrValidation, retention,
		)
	}

	return nil
}

// validateRegistrationStatus returns an error if status is not REGISTERED or DEPRECATED.
func validateRegistrationStatus(status string) error {
	switch status {
	case statusRegistered, statusDeprecated:
		return nil
	case "":
		return nil
	}

	return fmt.Errorf(
		"%w: invalid registrationStatus %q, must be REGISTERED or DEPRECATED",
		ErrValidation, status,
	)
}

// EnqueueActivityTaskInternal seeds an activity task in a task list for testing.
func (b *InMemoryBackend) EnqueueActivityTaskInternal(
	domain, taskList, activityID, activityName, activityVersion, input, workflowID, runID string,
) {
	b.mu.Lock("EnqueueActivityTaskInternal")
	defer b.mu.Unlock()

	key := domain + ":" + taskList
	b.activityQueues[key] = append(b.activityQueues[key], &ActivityTask{
		ActivityID:   activityID,
		ActivityType: ActivityTaskActivityType{Name: activityName, Version: activityVersion},
		Input:        input,
		WorkflowID:   workflowID,
		RunID:        runID,
	})
}

// EnqueueDecisionTaskInternal seeds a decision task in a task list for testing.
func (b *InMemoryBackend) EnqueueDecisionTaskInternal(domain, taskList, workflowID, runID string) {
	b.mu.Lock("EnqueueDecisionTaskInternal")
	defer b.mu.Unlock()

	key := domain + ":" + taskList
	b.decisionQueues[key] = append(b.decisionQueues[key], &DecisionTask{
		WorkflowID: workflowID,
		RunID:      runID,
	})
}

// AddWorkflowTypeInternal seeds a workflow type directly for testing.
func (b *InMemoryBackend) AddWorkflowTypeInternal(domain, name, version, status string) {
	b.mu.Lock("AddWorkflowTypeInternal")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	b.workflows[key] = &WorkflowType{Domain: domain, Name: name, Version: version, Status: status}
}

// AddActivityTypeInternal seeds an activity type directly for testing.
func (b *InMemoryBackend) AddActivityTypeInternal(domain, name, version, status string) {
	b.mu.Lock("AddActivityTypeInternal")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	b.activities[key] = &ActivityType{Domain: domain, Name: name, Version: version, Status: status}
}

// RegisterDomain registers a new SWF domain with the given retention period.
// retention must be "0"-"90" or "NONE" (empty defaults to "NONE").
func (b *InMemoryBackend) RegisterDomain(name, description, retention string) error {
	if name == "" {
		return fmt.Errorf("%w: name is required", ErrValidation)
	}
	if retention == "" {
		retention = retentionNone
	}
	if err := validateRetention(retention); err != nil {
		return err
	}

	b.mu.Lock("RegisterDomain")
	defer b.mu.Unlock()

	if d, ok := b.domains[name]; ok {
		if d.Status == statusDeprecated {
			return fmt.Errorf("%w: %s", ErrDeprecated, name)
		}

		return fmt.Errorf("%w: %s", ErrAlreadyExists, name)
	}

	b.domains[name] = &Domain{
		Name:                                   name,
		Description:                            description,
		Status:                                 statusRegistered,
		WorkflowExecutionRetentionPeriodInDays: retention,
	}

	return nil
}

// ListDomains returns all domains with the given registrationStatus.
// An empty status returns all domains.
func (b *InMemoryBackend) ListDomains(registrationStatus string) ([]Domain, error) {
	if err := validateRegistrationStatus(registrationStatus); err != nil {
		return nil, err
	}

	b.mu.RLock("ListDomains")
	defer b.mu.RUnlock()

	out := make([]Domain, 0, len(b.domains))
	for _, d := range b.domains {
		if registrationStatus == "" || d.Status == registrationStatus {
			out = append(out, *d)
		}
	}

	return out, nil
}

// DescribeDomain returns the details of a registered SWF domain.
func (b *InMemoryBackend) DescribeDomain(name string) (*Domain, error) {
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	d, ok := b.domains[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	cp := *d

	return &cp, nil
}

// DeprecateDomain marks a domain as deprecated.
func (b *InMemoryBackend) DeprecateDomain(name string) error {
	b.mu.Lock("DeprecateDomain")
	defer b.mu.Unlock()

	d, ok := b.domains[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if d.Status == statusDeprecated {
		return fmt.Errorf("%w: %s", ErrDeprecated, name)
	}
	d.Status = statusDeprecated

	return nil
}

// UndeprecateDomain re-activates a deprecated domain.
func (b *InMemoryBackend) UndeprecateDomain(name string) error {
	b.mu.Lock("UndeprecateDomain")
	defer b.mu.Unlock()

	d, ok := b.domains[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if d.Status == statusRegistered {
		return fmt.Errorf("%w: domain %s is not deprecated", ErrValidation, name)
	}
	d.Status = statusRegistered

	return nil
}

// RegisterWorkflowType registers a new workflow type with optional default settings.
func (b *InMemoryBackend) RegisterWorkflowType(
	domain, name, version, description string,
	defaults WorkflowTypeDefaults,
) error {
	if domain == "" {
		return fmt.Errorf("%w: domain is required", ErrValidation)
	}
	if name == "" {
		return fmt.Errorf("%w: name is required", ErrValidation)
	}
	if version == "" {
		return fmt.Errorf("%w: version is required", ErrValidation)
	}
	if err := validateChildPolicy(defaults.DefaultChildPolicy); err != nil {
		return err
	}
	if err := validateDuration(defaults.DefaultTaskStartToCloseTimeout); err != nil {
		return err
	}
	if err := validateDuration(defaults.DefaultExecutionStartToCloseTimeout); err != nil {
		return err
	}

	b.mu.Lock("RegisterWorkflowType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	if _, ok := b.workflows[key]; ok {
		return fmt.Errorf("%w: %s/%s", ErrTypeAlreadyExists, name, version)
	}

	b.workflows[key] = &WorkflowType{
		Domain:       domain,
		Name:         name,
		Version:      version,
		Status:       statusRegistered,
		Description:  description,
		CreationDate: float64(time.Now().UnixMilli()) / milliDivisor,
		Defaults:     defaults,
	}

	return nil
}

// ListWorkflowTypes returns workflow types for a domain, optionally filtered by registrationStatus.
func (b *InMemoryBackend) ListWorkflowTypes(
	domain, registrationStatus string,
) ([]WorkflowType, error) {
	if err := validateRegistrationStatus(registrationStatus); err != nil {
		return nil, err
	}

	b.mu.RLock("ListWorkflowTypes")
	defer b.mu.RUnlock()

	out := make([]WorkflowType, 0, len(b.workflows))
	for _, wt := range b.workflows {
		if wt.Domain != domain {
			continue
		}
		if registrationStatus != "" && wt.Status != registrationStatus {
			continue
		}
		out = append(out, *wt)
	}

	return out, nil
}

// DescribeWorkflowType returns the details of a workflow type.
func (b *InMemoryBackend) DescribeWorkflowType(
	domain, name, version string,
) (*WorkflowType, error) {
	b.mu.RLock("DescribeWorkflowType")
	defer b.mu.RUnlock()

	key := domain + ":" + name + ":" + version
	wt, ok := b.workflows[key]
	if !ok {
		return nil, fmt.Errorf("%w: workflow type %s/%s not found", ErrNotFound, name, version)
	}
	cp := *wt

	return &cp, nil
}

// DeprecateWorkflowType marks a workflow type as deprecated.
func (b *InMemoryBackend) DeprecateWorkflowType(domain, name, version string) error {
	b.mu.Lock("DeprecateWorkflowType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	wt, ok := b.workflows[key]
	if !ok {
		return fmt.Errorf("%w: workflow type %s/%s not found", ErrNotFound, name, version)
	}
	if wt.Status == statusDeprecated {
		return fmt.Errorf("%w: workflow type %s/%s", ErrTypeDeprecated, name, version)
	}
	wt.Status = statusDeprecated

	return nil
}

// UndeprecateWorkflowType re-activates a deprecated workflow type.
func (b *InMemoryBackend) UndeprecateWorkflowType(domain, name, version string) error {
	b.mu.Lock("UndeprecateWorkflowType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	wt, ok := b.workflows[key]
	if !ok {
		return fmt.Errorf("%w: workflow type %s/%s not found", ErrNotFound, name, version)
	}
	if wt.Status == statusRegistered {
		return fmt.Errorf("%w: workflow type %s/%s is not deprecated", ErrValidation, name, version)
	}
	wt.Status = statusRegistered

	return nil
}

// DeleteWorkflowType removes a deprecated workflow type.
func (b *InMemoryBackend) DeleteWorkflowType(domain, name, version string) error {
	b.mu.Lock("DeleteWorkflowType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	wt, ok := b.workflows[key]
	if !ok {
		return fmt.Errorf("%w: workflow type %s/%s not found", ErrNotFound, name, version)
	}
	if wt.Status != statusDeprecated {
		return fmt.Errorf(
			"%w: workflow type %s/%s must be deprecated before deletion",
			ErrTypeDeprecated, name, version,
		)
	}
	delete(b.workflows, key)

	return nil
}

// RegisterActivityType registers a new activity type with optional default settings.
func (b *InMemoryBackend) RegisterActivityType(
	domain, name, version, description string,
	defaults ActivityTypeDefaults,
) error {
	if domain == "" {
		return fmt.Errorf("%w: domain is required", ErrValidation)
	}
	if name == "" {
		return fmt.Errorf("%w: name is required", ErrValidation)
	}
	if version == "" {
		return fmt.Errorf("%w: version is required", ErrValidation)
	}
	if err := validateDuration(defaults.DefaultTaskHeartbeatTimeout); err != nil {
		return err
	}
	if err := validateDuration(defaults.DefaultTaskScheduleToCloseTimeout); err != nil {
		return err
	}
	if err := validateDuration(defaults.DefaultTaskScheduleToStartTimeout); err != nil {
		return err
	}
	if err := validateDuration(defaults.DefaultTaskStartToCloseTimeout); err != nil {
		return err
	}

	b.mu.Lock("RegisterActivityType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	if _, ok := b.activities[key]; ok {
		return fmt.Errorf("%w: activity type %s/%s", ErrTypeAlreadyExists, name, version)
	}

	b.activities[key] = &ActivityType{
		Domain:       domain,
		Name:         name,
		Version:      version,
		Status:       statusRegistered,
		Description:  description,
		CreationDate: float64(time.Now().UnixMilli()) / milliDivisor,
		Defaults:     defaults,
	}

	return nil
}

// ListActivityTypes returns activity types for a domain, optionally filtered by registrationStatus.
func (b *InMemoryBackend) ListActivityTypes(
	domain, registrationStatus string,
) ([]ActivityType, error) {
	if err := validateRegistrationStatus(registrationStatus); err != nil {
		return nil, err
	}

	b.mu.RLock("ListActivityTypes")
	defer b.mu.RUnlock()

	out := make([]ActivityType, 0, len(b.activities))
	for _, at := range b.activities {
		if at.Domain != domain {
			continue
		}
		if registrationStatus != "" && at.Status != registrationStatus {
			continue
		}
		out = append(out, *at)
	}

	return out, nil
}

// DescribeActivityType returns the details of an activity type.
func (b *InMemoryBackend) DescribeActivityType(
	domain, name, version string,
) (*ActivityType, error) {
	b.mu.RLock("DescribeActivityType")
	defer b.mu.RUnlock()

	key := domain + ":" + name + ":" + version
	at, ok := b.activities[key]
	if !ok {
		return nil, fmt.Errorf("%w: activity type %s/%s not found", ErrNotFound, name, version)
	}
	cp := *at

	return &cp, nil
}

// DeprecateActivityType marks an activity type as deprecated.
func (b *InMemoryBackend) DeprecateActivityType(domain, name, version string) error {
	b.mu.Lock("DeprecateActivityType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	at, ok := b.activities[key]
	if !ok {
		return fmt.Errorf("%w: activity type %s/%s not found", ErrNotFound, name, version)
	}
	if at.Status == statusDeprecated {
		return fmt.Errorf("%w: activity type %s/%s", ErrTypeDeprecated, name, version)
	}
	at.Status = statusDeprecated

	return nil
}

// UndeprecateActivityType re-activates a deprecated activity type.
func (b *InMemoryBackend) UndeprecateActivityType(domain, name, version string) error {
	b.mu.Lock("UndeprecateActivityType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	at, ok := b.activities[key]
	if !ok {
		return fmt.Errorf("%w: activity type %s/%s not found", ErrNotFound, name, version)
	}
	if at.Status == statusRegistered {
		return fmt.Errorf("%w: activity type %s/%s is not deprecated", ErrValidation, name, version)
	}
	at.Status = statusRegistered

	return nil
}

// DeleteActivityType removes a deprecated activity type.
func (b *InMemoryBackend) DeleteActivityType(domain, name, version string) error {
	b.mu.Lock("DeleteActivityType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	at, ok := b.activities[key]
	if !ok {
		return fmt.Errorf("%w: activity type %s/%s not found", ErrNotFound, name, version)
	}
	if at.Status != statusDeprecated {
		return fmt.Errorf(
			"%w: activity type %s/%s must be deprecated before deletion",
			ErrTypeDeprecated, name, version,
		)
	}
	delete(b.activities, key)

	return nil
}

// ExecutionFilter holds optional filters for counting/listing executions.
type ExecutionFilter struct {
	OldestDate          *time.Time
	LatestDate          *time.Time
	CloseOldestDate     *time.Time
	CloseLatestDate     *time.Time
	WorkflowID          string
	WorkflowTypeName    string
	WorkflowTypeVersion string
	Tag                 string
	CloseStatus         string
}

func (f ExecutionFilter) matchOpen(e *WorkflowExecution) bool {
	if e.Status != statusRunning {
		return false
	}
	if f.WorkflowID != "" && e.WorkflowID != f.WorkflowID {
		return false
	}
	if f.WorkflowTypeName != "" && e.WorkflowTypeName != f.WorkflowTypeName {
		return false
	}
	if f.WorkflowTypeVersion != "" && e.WorkflowTypeVersion != f.WorkflowTypeVersion {
		return false
	}
	if f.Tag != "" && !slices.Contains(e.TagList, f.Tag) {
		return false
	}
	st := time.Unix(int64(e.StartTimestamp), 0)
	if f.OldestDate != nil && st.Before(*f.OldestDate) {
		return false
	}
	if f.LatestDate != nil && st.After(*f.LatestDate) {
		return false
	}

	return true
}

//nolint:gocognit,cyclop // sequential date/filter checks are inherently complex
func (f ExecutionFilter) matchClosed(
	e *WorkflowExecution,
) bool {
	if e.Status == statusRunning {
		return false
	}
	if f.WorkflowID != "" && e.WorkflowID != f.WorkflowID {
		return false
	}
	if f.WorkflowTypeName != "" && e.WorkflowTypeName != f.WorkflowTypeName {
		return false
	}
	if f.WorkflowTypeVersion != "" && e.WorkflowTypeVersion != f.WorkflowTypeVersion {
		return false
	}
	if f.Tag != "" && !slices.Contains(e.TagList, f.Tag) {
		return false
	}
	if f.CloseStatus != "" && e.CloseStatus != f.CloseStatus {
		return false
	}
	if f.OldestDate != nil || f.LatestDate != nil {
		st := time.Unix(int64(e.StartTimestamp), 0)
		if f.OldestDate != nil && st.Before(*f.OldestDate) {
			return false
		}
		if f.LatestDate != nil && st.After(*f.LatestDate) {
			return false
		}
	}
	if f.CloseOldestDate != nil || f.CloseLatestDate != nil {
		if e.CloseTimestamp == 0 {
			return false
		}
		ct := time.Unix(int64(e.CloseTimestamp), 0)
		if f.CloseOldestDate != nil && ct.Before(*f.CloseOldestDate) {
			return false
		}
		if f.CloseLatestDate != nil && ct.After(*f.CloseLatestDate) {
			return false
		}
	}

	return true
}

// CountOpenWorkflowExecutions counts RUNNING workflow executions in a domain, applying filters.
func (b *InMemoryBackend) CountOpenWorkflowExecutions(domain string, filter ExecutionFilter) int {
	b.mu.RLock("CountOpenWorkflowExecutions")
	defer b.mu.RUnlock()

	count := 0
	for _, e := range b.executions {
		if e.Domain != domain {
			continue
		}
		if filter.matchOpen(e) {
			count++
		}
	}

	return count
}

// CountClosedWorkflowExecutions counts non-RUNNING workflow executions in a domain, applying filters.
func (b *InMemoryBackend) CountClosedWorkflowExecutions(domain string, filter ExecutionFilter) int {
	b.mu.RLock("CountClosedWorkflowExecutions")
	defer b.mu.RUnlock()

	count := 0
	for _, e := range b.executions {
		if e.Domain != domain {
			continue
		}
		if filter.matchClosed(e) {
			count++
		}
	}

	return count
}

// CountPendingActivityTasks returns the number of pending activity tasks for a task list.
func (b *InMemoryBackend) CountPendingActivityTasks(domain, taskList string) int {
	b.mu.RLock("CountPendingActivityTasks")
	defer b.mu.RUnlock()

	return len(b.activityQueues[domain+":"+taskList])
}

// CountPendingDecisionTasks returns the number of pending decision tasks for a task list.
func (b *InMemoryBackend) CountPendingDecisionTasks(domain, taskList string) int {
	b.mu.RLock("CountPendingDecisionTasks")
	defer b.mu.RUnlock()

	return len(b.decisionQueues[domain+":"+taskList])
}

// StartWorkflowExecution starts a new workflow execution.
// It validates that the referenced WorkflowType exists and is REGISTERED.
//
//nolint:gocognit,cyclop,nestif,funlen // sequential parameter validation is inherently complex
func (b *InMemoryBackend) StartWorkflowExecution(
	input StartWorkflowExecutionInput,
) (*WorkflowExecution, error) {
	if input.Domain == "" {
		return nil, fmt.Errorf("%w: domain is required", ErrValidation)
	}
	if input.WorkflowID == "" {
		return nil, fmt.Errorf("%w: workflowId is required", ErrValidation)
	}
	if err := validateChildPolicy(input.ChildPolicy); err != nil {
		return nil, err
	}
	if err := validateDuration(input.ExecutionStartToCloseTimeout); err != nil {
		return nil, err
	}
	if err := validateDuration(input.TaskStartToCloseTimeout); err != nil {
		return nil, err
	}

	b.mu.Lock("StartWorkflowExecution")
	defer b.mu.Unlock()

	if _, ok := b.domains[input.Domain]; !ok {
		return nil, fmt.Errorf("%w: domain %s not found", ErrNotFound, input.Domain)
	}

	// Validate workflow type if provided.
	taskList := input.TaskList
	childPolicy := input.ChildPolicy
	execTimeout := input.ExecutionStartToCloseTimeout
	taskTimeout := input.TaskStartToCloseTimeout
	lambdaRole := input.LambdaRole

	if input.WorkflowTypeName != "" {
		key := input.Domain + ":" + input.WorkflowTypeName + ":" + input.WorkflowTypeVersion
		wt, ok := b.workflows[key]
		if !ok {
			return nil, fmt.Errorf("%w: workflow type %s/%s not found",
				ErrNotFound, input.WorkflowTypeName, input.WorkflowTypeVersion)
		}
		if wt.Status == statusDeprecated {
			return nil, fmt.Errorf("%w: workflow type %s/%s is deprecated",
				ErrTypeDeprecated, input.WorkflowTypeName, input.WorkflowTypeVersion)
		}
		// Apply type defaults when not overridden.
		if taskList == "" {
			taskList = wt.Defaults.DefaultTaskList
		}
		if childPolicy == "" {
			childPolicy = wt.Defaults.DefaultChildPolicy
		}
		if execTimeout == "" {
			execTimeout = wt.Defaults.DefaultExecutionStartToCloseTimeout
		}
		if taskTimeout == "" {
			taskTimeout = wt.Defaults.DefaultTaskStartToCloseTimeout
		}
		if lambdaRole == "" {
			lambdaRole = wt.Defaults.DefaultLambdaRole
		}
	}

	if childPolicy == "" {
		childPolicy = "TERMINATE"
	}

	runID := input.RunID
	if runID == "" {
		runID = uuid.New().String()
	}

	key := input.Domain + ":" + input.WorkflowID

	if _, exists := b.executions[key]; !exists {
		b.executionOrder = append(b.executionOrder, key)
		if len(b.executionOrder) >= maxWorkflowExecutions {
			oldest := b.executionOrder[0]
			b.executionOrder = b.executionOrder[1:]
			delete(b.executions, oldest)
			delete(b.history, oldest)
		}
	}

	now := float64(time.Now().UnixMilli()) / milliDivisor
	exec := &WorkflowExecution{
		Domain:                       input.Domain,
		WorkflowID:                   input.WorkflowID,
		RunID:                        runID,
		Status:                       statusRunning,
		StartTimestamp:               now,
		WorkflowTypeName:             input.WorkflowTypeName,
		WorkflowTypeVersion:          input.WorkflowTypeVersion,
		TaskList:                     taskList,
		Input:                        input.Input,
		TagList:                      input.TagList,
		ChildPolicy:                  childPolicy,
		LambdaRole:                   lambdaRole,
		ExecutionStartToCloseTimeout: execTimeout,
		TaskStartToCloseTimeout:      taskTimeout,
		TaskPriority:                 input.TaskPriority,
	}
	b.executions[key] = exec

	attrKey := eventAttrKey("WorkflowExecutionStarted")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrInput:     input.Input,
			"childPolicy": childPolicy,
			"taskList":    map[string]any{attrName: taskList},
			"workflowType": map[string]any{
				attrName:  input.WorkflowTypeName,
				"version": input.WorkflowTypeVersion,
			},
			"executionStartToCloseTimeout": execTimeout,
			"taskStartToCloseTimeout":      taskTimeout,
			"lambdaRole":                   lambdaRole,
			"tagList":                      input.TagList,
		},
	}
	b.appendHistoryEventLocked(input.Domain, input.WorkflowID, "WorkflowExecutionStarted", attrs)

	cp := *exec

	return &cp, nil
}

// TerminateWorkflowExecution terminates a running workflow execution.
// runID is optional; if provided, it must match. reason and details are stored in history.
func (b *InMemoryBackend) TerminateWorkflowExecution(
	domain, workflowID, runID, reason, details string,
) error {
	b.mu.Lock("TerminateWorkflowExecution")
	defer b.mu.Unlock()

	key := domain + ":" + workflowID
	exec, ok := b.executions[key]
	if !ok {
		return fmt.Errorf("%w: execution %s/%s not found", ErrNotFound, domain, workflowID)
	}
	if exec.Status != statusRunning {
		return fmt.Errorf("%w: execution %s/%s is not running", ErrValidation, domain, workflowID)
	}
	if runID != "" && exec.RunID != runID {
		return fmt.Errorf(
			"%w: runId %s does not match current run %s",
			ErrNotFound,
			runID,
			exec.RunID,
		)
	}

	exec.Status = statusTerminated
	exec.CloseStatus = statusTerminated
	exec.CloseTimestamp = float64(time.Now().UnixMilli()) / milliDivisor

	attrKey := eventAttrKey("WorkflowExecutionTerminated")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrReason:    reason,
			attrDetails:   details,
			"cause":       "OPERATOR_INITIATED",
			"childPolicy": exec.ChildPolicy,
		},
	}
	b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionTerminated", attrs)

	return nil
}

// DescribeWorkflowExecution returns a workflow execution.
func (b *InMemoryBackend) DescribeWorkflowExecution(
	domain, workflowID string,
) (*WorkflowExecution, error) {
	b.mu.RLock("DescribeWorkflowExecution")
	defer b.mu.RUnlock()

	key := domain + ":" + workflowID
	exec, ok := b.executions[key]
	if !ok {
		return nil, fmt.Errorf("%w: execution %s/%s not found", ErrNotFound, domain, workflowID)
	}
	cp := *exec

	return &cp, nil
}

// GetWorkflowExecutionHistory returns history events for a workflow execution,
// supporting pagination and reverse ordering.
func (b *InMemoryBackend) GetWorkflowExecutionHistory(
	domain, workflowID string,
	maxPageSize int,
	nextPageToken string,
	reverseOrder bool,
) ([]HistoryEvent, string) {
	b.mu.RLock("GetWorkflowExecutionHistory")
	defer b.mu.RUnlock()

	events := b.history[domain+":"+workflowID]
	if len(events) == 0 {
		return []HistoryEvent{}, ""
	}

	cp := make([]HistoryEvent, len(events))
	copy(cp, events)

	if reverseOrder {
		for i, j := 0, len(cp)-1; i < j; i, j = i+1, j-1 {
			cp[i], cp[j] = cp[j], cp[i]
		}
	}

	const maxDefault = 1000
	p := page.New(cp, nextPageToken, maxPageSize, maxDefault)

	return p.Data, p.Next
}

// openCountsLocked returns open activity/decision/timer counts for an execution.
// Caller must hold at least RLock.
func (b *InMemoryBackend) openCountsLocked(domain, workflowID string) map[string]int {
	activityCount := 0
	for _, rec := range b.activeActivityTasks {
		if rec.Domain == domain && rec.WorkflowID == workflowID {
			activityCount++
		}
	}
	decisionCount := 0
	for _, q := range b.decisionQueues {
		for _, t := range q {
			if t.WorkflowID == workflowID {
				decisionCount++
			}
		}
	}

	return map[string]int{
		"openActivityTasks":           activityCount,
		"openDecisionTasks":           decisionCount,
		"openTimers":                  0,
		"openChildWorkflowExecutions": 0,
	}
}

// ListOpenWorkflowExecutions returns all running executions in a domain matching the filter.
func (b *InMemoryBackend) ListOpenWorkflowExecutions(
	domain string,
	filter ExecutionFilter,
) []WorkflowExecution {
	b.mu.RLock("ListOpenWorkflowExecutions")
	defer b.mu.RUnlock()

	out := make([]WorkflowExecution, 0, len(b.executions))
	for _, e := range b.executions {
		if e.Domain != domain {
			continue
		}
		if filter.matchOpen(e) {
			out = append(out, *e)
		}
	}

	return out
}

// ListClosedWorkflowExecutions returns all closed executions in a domain matching the filter.
func (b *InMemoryBackend) ListClosedWorkflowExecutions(
	domain string,
	filter ExecutionFilter,
) []WorkflowExecution {
	b.mu.RLock("ListClosedWorkflowExecutions")
	defer b.mu.RUnlock()

	out := make([]WorkflowExecution, 0, len(b.executions))
	for _, e := range b.executions {
		if e.Domain != domain {
			continue
		}
		if filter.matchClosed(e) {
			out = append(out, *e)
		}
	}

	return out
}

// ListTagsForResource returns tags for a resource ARN.
// Returns an error if the ARN is not a valid SWF domain ARN or the domain does not exist.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if err := b.validateDomainARNLocked(resourceARN); err != nil {
		return nil, err
	}

	tags := b.tags[resourceARN]
	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp, nil
}

// TagResource adds or updates tags on a resource.
// Validates ARN format, domain existence, tag count and key/value length limits.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be 1-%d characters", ErrValidation, maxTagKeyLen)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value must be 0-%d characters",
				ErrValidation,
				maxTagValueLen,
			)
		}
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if err := b.validateDomainARNLocked(resourceARN); err != nil {
		return err
	}

	existing := b.tags[resourceARN]
	merged := len(existing)
	for k := range tags {
		if _, alreadyPresent := existing[k]; !alreadyPresent {
			merged++
		}
	}
	if merged > maxTags {
		return fmt.Errorf("%w: resource cannot have more than %d tags", ErrTooManyTags, maxTags)
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if err := b.validateDomainARNLocked(resourceARN); err != nil {
		return err
	}

	if b.tags[resourceARN] != nil {
		for _, k := range tagKeys {
			delete(b.tags[resourceARN], k)
		}
	}

	return nil
}

// validateDomainARNLocked validates a SWF ARN. Caller must hold at least RLock.
func (b *InMemoryBackend) validateDomainARNLocked(arn string) error {
	m := swfARNRegex.FindStringSubmatch(arn)
	if m == nil {
		return fmt.Errorf("%w: invalid SWF domain ARN: %s", ErrValidation, arn)
	}
	domainName := m[1]
	if _, ok := b.domains[domainName]; !ok {
		return fmt.Errorf("%w: domain %s not found for ARN %s", ErrNotFound, domainName, arn)
	}

	return nil
}

// PollForActivityTask returns the next available activity task for a task list, or nil if none.
func (b *InMemoryBackend) PollForActivityTask(domain, taskList string) *ActivityTask {
	b.mu.Lock("PollForActivityTask")
	defer b.mu.Unlock()

	key := domain + ":" + taskList
	queue := b.activityQueues[key]
	if len(queue) == 0 {
		return nil
	}

	task := *queue[0]
	b.activityQueues[key] = queue[1:]
	token := uuid.New().String()
	task.TaskToken = token

	// Emit ActivityTaskStarted event and record active task.
	startedEventID := b.appendHistoryEventLocked(
		domain, task.WorkflowID, "ActivityTaskStarted",
		map[string]any{
			eventAttrKey("ActivityTaskStarted"): map[string]any{
				attrScheduledEvID: task.ScheduledEventID,
			},
		},
	)
	task.StartedEventID = startedEventID

	b.activeActivityTasks[token] = &activeActivityTaskRecord{
		Domain:           domain,
		WorkflowID:       task.WorkflowID,
		RunID:            task.RunID,
		ActivityID:       task.ActivityID,
		ActivityType:     task.ActivityType,
		ScheduledEventID: task.ScheduledEventID,
		StartedEventID:   startedEventID,
		TaskList:         taskList,
	}

	return &task
}

// PollForDecisionTask returns the next available decision task for a task list, or nil if none.
func (b *InMemoryBackend) PollForDecisionTask(
	domain, taskList string,
	maxPageSize int,
	nextPageToken string,
) *DecisionTask {
	b.mu.Lock("PollForDecisionTask")
	defer b.mu.Unlock()

	key := domain + ":" + taskList
	queue := b.decisionQueues[key]
	if len(queue) == 0 {
		return nil
	}

	task := *queue[0]
	b.decisionQueues[key] = queue[1:]
	task.TaskToken = uuid.New().String()

	b.activeDecisionTasks[task.TaskToken] = &activeDecisionTaskRecord{
		Domain:     domain,
		WorkflowID: task.WorkflowID,
		RunID:      task.RunID,
	}

	histEvents := b.history[domain+":"+task.WorkflowID]
	if len(histEvents) > 0 {
		cp := make([]HistoryEvent, len(histEvents))
		copy(cp, histEvents)
		const maxDefault = 1000
		p := page.New(cp, nextPageToken, maxPageSize, maxDefault)
		task.Events = p.Data
		task.NextPageToken = p.Next
	}

	// Populate workflow type from execution if known.
	if exec, ok := b.executions[domain+":"+task.WorkflowID]; ok {
		task.WorkflowTypeName = exec.WorkflowTypeName
		task.WorkflowTypeVersion = exec.WorkflowTypeVersion
	}

	return &task
}

// RecordActivityTaskHeartbeat acknowledges a heartbeat for an activity task token.
// Returns true if cancel has been requested for the workflow; always false in this emulator.
func (b *InMemoryBackend) RecordActivityTaskHeartbeat(taskToken string) (bool, error) {
	b.mu.RLock("RecordActivityTaskHeartbeat")
	defer b.mu.RUnlock()

	rec, ok := b.activeActivityTasks[taskToken]
	if !ok {
		return false, fmt.Errorf("%w: task token %s not found", ErrNotFound, taskToken)
	}

	exec, ok := b.executions[rec.Domain+":"+rec.WorkflowID]
	if !ok {
		return false, nil
	}

	return exec.CancelRequested, nil
}

// RequestCancelWorkflowExecution requests cancellation of a running execution.
// runID is optional; if provided, it must match.
func (b *InMemoryBackend) RequestCancelWorkflowExecution(domain, workflowID, runID string) error {
	b.mu.Lock("RequestCancelWorkflowExecution")
	defer b.mu.Unlock()

	key := domain + ":" + workflowID
	exec, ok := b.executions[key]
	if !ok {
		return fmt.Errorf("%w: execution %s/%s not found", ErrNotFound, domain, workflowID)
	}
	if exec.Status != statusRunning {
		return fmt.Errorf("%w: execution %s/%s is not running", ErrValidation, domain, workflowID)
	}
	if runID != "" && exec.RunID != runID {
		return fmt.Errorf(
			"%w: runId %s does not match current run %s",
			ErrNotFound,
			runID,
			exec.RunID,
		)
	}

	exec.CancelRequested = true

	attrKey := eventAttrKey("WorkflowExecutionCancelRequested")
	attrs := map[string]any{
		attrKey: map[string]any{
			"cause": "OPERATOR_INITIATED",
		},
	}
	b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionCancelRequested", attrs)

	// Enqueue a decision task so the workflow decider can react.
	if exec.TaskList != "" {
		qkey := domain + ":" + exec.TaskList
		b.decisionQueues[qkey] = append(b.decisionQueues[qkey], &DecisionTask{
			WorkflowID: workflowID,
			RunID:      exec.RunID,
		})
	}

	return nil
}

// RespondActivityTaskCanceled marks an activity task as canceled.
func (b *InMemoryBackend) RespondActivityTaskCanceled(taskToken, details string) error {
	b.mu.Lock("RespondActivityTaskCanceled")
	defer b.mu.Unlock()

	rec, ok := b.activeActivityTasks[taskToken]
	if !ok {
		return fmt.Errorf("%w: task token %s not found", ErrNotFound, taskToken)
	}
	delete(b.activeActivityTasks, taskToken)

	attrKey := eventAttrKey("ActivityTaskCanceled")
	attrs := map[string]any{
		attrKey: map[string]any{
			"details":         details,
			attrScheduledEvID: rec.ScheduledEventID,
			attrStartedEvID:   rec.StartedEventID,
		},
	}
	b.appendHistoryEventLocked(rec.Domain, rec.WorkflowID, "ActivityTaskCanceled", attrs)
	b.enqueueDecisionTaskLocked(rec.Domain, rec.WorkflowID)

	return nil
}

// RespondActivityTaskCompleted marks an activity task as completed.
func (b *InMemoryBackend) RespondActivityTaskCompleted(taskToken, result string) error {
	b.mu.Lock("RespondActivityTaskCompleted")
	defer b.mu.Unlock()

	rec, ok := b.activeActivityTasks[taskToken]
	if !ok {
		return fmt.Errorf("%w: task token %s not found", ErrNotFound, taskToken)
	}
	delete(b.activeActivityTasks, taskToken)

	attrKey := eventAttrKey("ActivityTaskCompleted")
	attrs := map[string]any{
		attrKey: map[string]any{
			"result":          result,
			attrScheduledEvID: rec.ScheduledEventID,
			attrStartedEvID:   rec.StartedEventID,
		},
	}
	b.appendHistoryEventLocked(rec.Domain, rec.WorkflowID, "ActivityTaskCompleted", attrs)
	b.enqueueDecisionTaskLocked(rec.Domain, rec.WorkflowID)

	return nil
}

// RespondActivityTaskFailed marks an activity task as failed.
func (b *InMemoryBackend) RespondActivityTaskFailed(taskToken, reason, details string) error {
	b.mu.Lock("RespondActivityTaskFailed")
	defer b.mu.Unlock()

	rec, ok := b.activeActivityTasks[taskToken]
	if !ok {
		return fmt.Errorf("%w: task token %s not found", ErrNotFound, taskToken)
	}
	delete(b.activeActivityTasks, taskToken)

	attrKey := eventAttrKey("ActivityTaskFailed")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrReason:        reason,
			attrDetails:       details,
			attrScheduledEvID: rec.ScheduledEventID,
			attrStartedEvID:   rec.StartedEventID,
		},
	}
	b.appendHistoryEventLocked(rec.Domain, rec.WorkflowID, "ActivityTaskFailed", attrs)
	b.enqueueDecisionTaskLocked(rec.Domain, rec.WorkflowID)

	return nil
}

// RespondDecisionTaskCompleted processes a completed decision task and applies decisions.
func (b *InMemoryBackend) RespondDecisionTaskCompleted(
	taskToken, executionContext string,
	decisions []Decision,
) error {
	b.mu.Lock("RespondDecisionTaskCompleted")
	defer b.mu.Unlock()

	rec, ok := b.activeDecisionTasks[taskToken]
	if !ok {
		return fmt.Errorf("%w: decision task token %s not found", ErrNotFound, taskToken)
	}
	delete(b.activeDecisionTasks, taskToken)

	key := rec.Domain + ":" + rec.WorkflowID
	exec, ok := b.executions[key]
	if !ok {
		return nil
	}

	if executionContext != "" {
		exec.LatestExecutionContext = executionContext
	}

	b.appendHistoryEventLocked(rec.Domain, rec.WorkflowID, "DecisionTaskCompleted", map[string]any{
		eventAttrKey("DecisionTaskCompleted"): map[string]any{
			"executionContext": executionContext,
		},
	})

	for _, d := range decisions {
		b.processDecisionLocked(rec.Domain, rec.WorkflowID, exec, d)
	}

	return nil
}

// processDecisionLocked applies a single decision to an execution.
// Caller must hold the write lock.
func (b *InMemoryBackend) processDecisionLocked(domain, workflowID string, exec *WorkflowExecution, d Decision) {
	now := float64(time.Now().UnixMilli()) / milliDivisor

	switch d.DecisionType {
	case "CompleteWorkflowExecution":
		result := ""
		if d.CompleteWorkflowExecutionAttrs != nil {
			result = d.CompleteWorkflowExecutionAttrs.Result
		}
		exec.Status = statusCompleted
		exec.CloseStatus = statusCompleted
		exec.CloseTimestamp = now
		b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionCompleted", map[string]any{
			eventAttrKey("WorkflowExecutionCompleted"): map[string]any{"result": result},
		})

	case "FailWorkflowExecution":
		reason, details := "", ""
		if d.FailWorkflowExecutionAttrs != nil {
			reason = d.FailWorkflowExecutionAttrs.Reason
			details = d.FailWorkflowExecutionAttrs.Details
		}
		exec.Status = statusFailed
		exec.CloseStatus = statusFailed
		exec.CloseTimestamp = now
		b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionFailed", map[string]any{
			eventAttrKey("WorkflowExecutionFailed"): map[string]any{
				attrReason: reason, attrDetails: details,
			},
		})

	case "CancelWorkflowExecution":
		details := ""
		if d.CancelWorkflowExecutionAttrs != nil {
			details = d.CancelWorkflowExecutionAttrs.Details
		}
		exec.Status = statusCanceled
		exec.CloseStatus = statusCanceled
		exec.CloseTimestamp = now
		b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionCanceled", map[string]any{
			eventAttrKey("WorkflowExecutionCanceled"): map[string]any{attrDetails: details},
		})

	case "ContinueAsNewWorkflowExecution":
		exec.Status = statusContinuedAsNew
		exec.CloseStatus = statusContinuedAsNew
		exec.CloseTimestamp = now
		b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionContinuedAsNew", map[string]any{
			eventAttrKey("WorkflowExecutionContinuedAsNew"): map[string]any{},
		})

	case "ScheduleActivityTask":
		if d.ScheduleActivityTaskAttrs == nil {
			return
		}
		attrs := d.ScheduleActivityTaskAttrs
		taskList := attrs.TaskList
		if taskList == "" {
			taskList = exec.TaskList
		}
		scheduledEventID := b.appendHistoryEventLocked(domain, workflowID, "ActivityTaskScheduled", map[string]any{
			eventAttrKey("ActivityTaskScheduled"): map[string]any{
				"activityType": map[string]any{
					attrName:  attrs.ActivityType.Name,
					"version": attrs.ActivityType.Version,
				},
				"activityId": attrs.ActivityID,
				attrInput:    attrs.Input,
				"taskList":   map[string]any{attrName: taskList},
			},
		})
		qkey := domain + ":" + taskList
		b.activityQueues[qkey] = append(b.activityQueues[qkey], &ActivityTask{
			ActivityID:       attrs.ActivityID,
			ActivityType:     attrs.ActivityType,
			Input:            attrs.Input,
			WorkflowID:       workflowID,
			RunID:            exec.RunID,
			ScheduledEventID: scheduledEventID,
		})
	}
}

// enqueueDecisionTaskLocked adds a decision task for the execution's task list.
// Caller must hold the write lock.
func (b *InMemoryBackend) enqueueDecisionTaskLocked(domain, workflowID string) {
	exec, ok := b.executions[domain+":"+workflowID]
	if !ok || exec.TaskList == "" {
		return
	}
	key := domain + ":" + exec.TaskList
	b.decisionQueues[key] = append(b.decisionQueues[key], &DecisionTask{
		WorkflowID: workflowID,
		RunID:      exec.RunID,
	})
}

// SignalWorkflowExecution sends a signal to a workflow execution, recording it in history.
func (b *InMemoryBackend) SignalWorkflowExecution(
	domain, workflowID, runID, signalName, input string,
) error {
	b.mu.Lock("SignalWorkflowExecution")
	defer b.mu.Unlock()

	key := domain + ":" + workflowID
	exec, ok := b.executions[key]
	if !ok {
		return fmt.Errorf("%w: execution %s/%s not found", ErrNotFound, domain, workflowID)
	}
	if runID != "" && exec.RunID != runID {
		return fmt.Errorf(
			"%w: runId %s does not match current run %s",
			ErrNotFound,
			runID,
			exec.RunID,
		)
	}
	if exec.Status != statusRunning {
		return fmt.Errorf("%w: execution %s/%s is not running", ErrValidation, domain, workflowID)
	}

	attrKey := eventAttrKey("WorkflowExecutionSignaled")
	attrs := map[string]any{
		attrKey: map[string]any{
			"signalName": signalName,
			"input":      input,
		},
	}
	b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionSignaled", attrs)

	// Enqueue a decision task so the workflow decider can react.
	b.enqueueDecisionTaskLocked(domain, workflowID)

	return nil
}

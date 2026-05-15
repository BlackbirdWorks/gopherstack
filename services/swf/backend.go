package swf

import (
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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
)

const (
	// maxWorkflowExecutions is the maximum number of workflow executions retained.
	// Oldest executions are evicted when this limit is exceeded.
	maxWorkflowExecutions = 10_000

	// statusDeprecated is the status string for deprecated resources.
	statusDeprecated = "DEPRECATED"

	// statusRegistered is the status string for registered resources.
	statusRegistered = "REGISTERED"

	// statusRunning is the status string for running workflow executions.
	statusRunning = "RUNNING"

	// statusTerminated is the status string for terminated workflow executions.
	statusTerminated = "TERMINATED"

	// defaultAccountID is used when no account ID is provided.
	defaultAccountID = "123456789012"
)

// HistoryEvent is a single event in a workflow execution's history.
type HistoryEvent struct {
	EventType string  `json:"eventType"`
	EventID   int64   `json:"eventId"`
	Timestamp float64 `json:"eventTimestamp"`
}

// ActivityTaskActivityType is the activity type reference within an activity task.
type ActivityTaskActivityType struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ActivityTask represents a pending activity task returned by PollForActivityTask.
type ActivityTask struct {
	TaskToken      string                   `json:"taskToken"`
	ActivityID     string                   `json:"activityId"`
	ActivityType   ActivityTaskActivityType `json:"activityType"`
	Input          string                   `json:"input,omitempty"`
	WorkflowID     string                   `json:"workflowId"`
	RunID          string                   `json:"runId"`
	StartedEventID int64                    `json:"startedEventId"`
}

// DecisionTask represents a pending decision task returned by PollForDecisionTask.
type DecisionTask struct {
	TaskToken      string         `json:"taskToken"`
	WorkflowID     string         `json:"workflowId"`
	RunID          string         `json:"runId"`
	Events         []HistoryEvent `json:"events"`
	StartedEventID int64          `json:"startedEventId"`
}

// Domain represents an SWF domain.
type Domain struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Status      string `json:"status"` // REGISTERED or DEPRECATED
}

// ActivityType represents an SWF activity type.
type ActivityType struct {
	Description string `json:"description"`
	Domain      string `json:"domain"`
	Name        string `json:"name"`
	Version     string `json:"version"`
	Status      string `json:"status"`
}

// WorkflowType represents an SWF workflow type.
type WorkflowType struct {
	Description string `json:"description"`
	Domain      string `json:"domain"`
	Name        string `json:"name"`
	Version     string `json:"version"`
	Status      string `json:"status"`
}

// WorkflowExecution represents an SWF workflow execution.
type WorkflowExecution struct {
	Domain         string  `json:"domain"`
	WorkflowID     string  `json:"workflowID"`
	RunID          string  `json:"runID"`
	Status         string  `json:"status"`
	CloseStatus    string  `json:"closeStatus,omitempty"`
	StartTimestamp float64 `json:"startTimestamp"`
	CloseTimestamp float64 `json:"closeTimestamp,omitempty"`
}

// InMemoryBackend is the in-memory store for SWF resources.
type InMemoryBackend struct {
	domains        map[string]*Domain
	workflows      map[string]*WorkflowType      // key: domain+":"+name+":"+version
	activities     map[string]*ActivityType      // key: domain+":"+name+":"+version
	executions     map[string]*WorkflowExecution // key: domain+":"+workflowID
	history        map[string][]HistoryEvent     // key: domain+":"+workflowID
	activityQueues map[string][]*ActivityTask    // key: domain+":"+taskList
	decisionQueues map[string][]*DecisionTask    // key: domain+":"+taskList
	tags           map[string]map[string]string  // key: resourceARN
	mu             *lockmetrics.RWMutex
	executionOrder []string // FIFO order of execution keys for eviction
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		domains:        make(map[string]*Domain),
		workflows:      make(map[string]*WorkflowType),
		activities:     make(map[string]*ActivityType),
		executions:     make(map[string]*WorkflowExecution),
		history:        make(map[string][]HistoryEvent),
		activityQueues: make(map[string][]*ActivityTask),
		decisionQueues: make(map[string][]*DecisionTask),
		tags:           make(map[string]map[string]string),
		mu:             lockmetrics.New("swf"),
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
	b.tags = make(map[string]map[string]string)
	b.executionOrder = nil
}

// appendHistoryEventLocked appends a history event for a workflow execution.
// Caller must hold the write lock.
func (b *InMemoryBackend) appendHistoryEventLocked(domain, workflowID, eventType string) {
	key := domain + ":" + workflowID
	events := b.history[key]
	b.history[key] = append(events, HistoryEvent{
		EventID:   int64(len(events)) + 1,
		EventType: eventType,
		Timestamp: float64(time.Now().Unix()),
	})
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

// AccountID returns the account ID for this backend.
func (b *InMemoryBackend) AccountID() string { return defaultAccountID }

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

// RegisterDomain registers a new SWF domain.
func (b *InMemoryBackend) RegisterDomain(name, description string) error {
	if name == "" {
		return fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("RegisterDomain")
	defer b.mu.Unlock()

	if d, ok := b.domains[name]; ok {
		if d.Status == statusDeprecated {
			return fmt.Errorf("%w: %s", ErrDeprecated, name)
		}

		return fmt.Errorf("%w: %s", ErrAlreadyExists, name)
	}

	b.domains[name] = &Domain{Name: name, Description: description, Status: statusRegistered}

	return nil
}

// ListDomains returns all domains with the given status.
func (b *InMemoryBackend) ListDomains(registrationStatus string) []Domain {
	b.mu.RLock("ListDomains")
	defer b.mu.RUnlock()

	out := make([]Domain, 0, len(b.domains))
	for _, d := range b.domains {
		if registrationStatus == "" || d.Status == registrationStatus {
			out = append(out, *d)
		}
	}

	return out
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

// RegisterWorkflowType registers a new workflow type.
func (b *InMemoryBackend) RegisterWorkflowType(domain, name, version, description string) error {
	if domain == "" {
		return fmt.Errorf("%w: domain is required", ErrValidation)
	}

	if name == "" {
		return fmt.Errorf("%w: name is required", ErrValidation)
	}

	if version == "" {
		return fmt.Errorf("%w: version is required", ErrValidation)
	}

	b.mu.Lock("RegisterWorkflowType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	if _, ok := b.workflows[key]; ok {
		return fmt.Errorf("%w: %s/%s", ErrTypeAlreadyExists, name, version)
	}

	b.workflows[key] = &WorkflowType{
		Domain:      domain,
		Name:        name,
		Version:     version,
		Status:      statusRegistered,
		Description: description,
	}

	return nil
}

// ListWorkflowTypes returns workflow types for a domain, optionally filtered by registrationStatus.
func (b *InMemoryBackend) ListWorkflowTypes(domain, registrationStatus string) []WorkflowType {
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

	return out
}

// DescribeWorkflowType returns the details of a workflow type.
func (b *InMemoryBackend) DescribeWorkflowType(domain, name, version string) (*WorkflowType, error) {
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
			ErrTypeDeprecated,
			name,
			version,
		)
	}

	delete(b.workflows, key)

	return nil
}

// RegisterActivityType registers a new activity type.
func (b *InMemoryBackend) RegisterActivityType(domain, name, version, description string) error {
	if domain == "" {
		return fmt.Errorf("%w: domain is required", ErrValidation)
	}

	if name == "" {
		return fmt.Errorf("%w: name is required", ErrValidation)
	}

	if version == "" {
		return fmt.Errorf("%w: version is required", ErrValidation)
	}

	b.mu.Lock("RegisterActivityType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	if _, ok := b.activities[key]; ok {
		return fmt.Errorf("%w: activity type %s/%s", ErrTypeAlreadyExists, name, version)
	}

	b.activities[key] = &ActivityType{
		Domain:      domain,
		Name:        name,
		Version:     version,
		Status:      statusRegistered,
		Description: description,
	}

	return nil
}

// ListActivityTypes returns activity types for a domain, optionally filtered by registrationStatus.
func (b *InMemoryBackend) ListActivityTypes(domain, registrationStatus string) []ActivityType {
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

	return out
}

// DescribeActivityType returns the details of an activity type.
func (b *InMemoryBackend) DescribeActivityType(domain, name, version string) (*ActivityType, error) {
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
			ErrTypeDeprecated,
			name,
			version,
		)
	}

	delete(b.activities, key)

	return nil
}

// CountOpenWorkflowExecutions counts RUNNING workflow executions in a domain.
func (b *InMemoryBackend) CountOpenWorkflowExecutions(domain string) int {
	b.mu.RLock("CountOpenWorkflowExecutions")
	defer b.mu.RUnlock()

	count := 0
	for _, e := range b.executions {
		if e.Domain == domain && e.Status == statusRunning {
			count++
		}
	}

	return count
}

// CountClosedWorkflowExecutions counts non-RUNNING workflow executions in a domain.
func (b *InMemoryBackend) CountClosedWorkflowExecutions(domain string) int {
	b.mu.RLock("CountClosedWorkflowExecutions")
	defer b.mu.RUnlock()

	count := 0
	for _, e := range b.executions {
		if e.Domain == domain && e.Status != statusRunning {
			count++
		}
	}

	return count
}

// CountPendingActivityTasks returns 0 since task-level tracking is not implemented.
func (b *InMemoryBackend) CountPendingActivityTasks(_ string) int {
	return 0
}

// CountPendingDecisionTasks returns 0 since task-level tracking is not implemented.
func (b *InMemoryBackend) CountPendingDecisionTasks(_ string) int {
	return 0
}

// StartWorkflowExecution starts a new workflow execution.
func (b *InMemoryBackend) StartWorkflowExecution(domain, workflowID, runID string) (*WorkflowExecution, error) {
	if domain == "" {
		return nil, fmt.Errorf("%w: domain is required", ErrValidation)
	}

	if workflowID == "" {
		return nil, fmt.Errorf("%w: workflowId is required", ErrValidation)
	}

	b.mu.Lock("StartWorkflowExecution")
	defer b.mu.Unlock()

	key := domain + ":" + workflowID

	// If this is a new key, track it for eviction and enforce the cap.
	if _, exists := b.executions[key]; !exists {
		b.executionOrder = append(b.executionOrder, key)

		if len(b.executionOrder) >= maxWorkflowExecutions {
			oldest := b.executionOrder[0]
			b.executionOrder = b.executionOrder[1:]
			delete(b.executions, oldest)
		}
	}

	now := float64(time.Now().Unix())
	exec := &WorkflowExecution{
		Domain:         domain,
		WorkflowID:     workflowID,
		RunID:          runID,
		Status:         statusRunning,
		StartTimestamp: now,
	}
	b.executions[key] = exec
	b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionStarted")

	cp := *exec

	return &cp, nil
}

// TerminateWorkflowExecution terminates a running workflow execution.
func (b *InMemoryBackend) TerminateWorkflowExecution(domain, workflowID string) error {
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

	exec.Status = statusTerminated
	exec.CloseStatus = statusTerminated
	exec.CloseTimestamp = float64(time.Now().Unix())
	b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionTerminated")

	return nil
}

// DescribeWorkflowExecution returns a workflow execution.
func (b *InMemoryBackend) DescribeWorkflowExecution(domain, workflowID string) (*WorkflowExecution, error) {
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

// GetWorkflowExecutionHistory returns history events for a workflow execution.
func (b *InMemoryBackend) GetWorkflowExecutionHistory(domain, workflowID string) []HistoryEvent {
	b.mu.RLock("GetWorkflowExecutionHistory")
	defer b.mu.RUnlock()

	events := b.history[domain+":"+workflowID]
	if len(events) == 0 {
		return []HistoryEvent{}
	}

	cp := make([]HistoryEvent, len(events))
	copy(cp, events)

	return cp
}

// ListOpenWorkflowExecutions returns all running executions in a domain.
func (b *InMemoryBackend) ListOpenWorkflowExecutions(domain string) []WorkflowExecution {
	b.mu.RLock("ListOpenWorkflowExecutions")
	defer b.mu.RUnlock()

	out := make([]WorkflowExecution, 0, len(b.executions))
	for _, e := range b.executions {
		if e.Domain == domain && e.Status == statusRunning {
			out = append(out, *e)
		}
	}

	return out
}

// ListClosedWorkflowExecutions returns all closed executions in a domain.
func (b *InMemoryBackend) ListClosedWorkflowExecutions(domain string) []WorkflowExecution {
	b.mu.RLock("ListClosedWorkflowExecutions")
	defer b.mu.RUnlock()

	out := make([]WorkflowExecution, 0, len(b.executions))
	for _, e := range b.executions {
		if e.Domain == domain && e.Status != statusRunning {
			out = append(out, *e)
		}
	}

	return out
}

// ListTagsForResource returns tags for a resource ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags := b.tags[resourceARN]
	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

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

	if b.tags[resourceARN] != nil {
		for _, k := range tagKeys {
			delete(b.tags[resourceARN], k)
		}
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
	task.TaskToken = uuid.New().String()

	return &task
}

// PollForDecisionTask returns the next available decision task for a task list, or nil if none.
func (b *InMemoryBackend) PollForDecisionTask(domain, taskList string) *DecisionTask {
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

	histEvents := b.history[domain+":"+task.WorkflowID]
	if len(histEvents) > 0 {
		task.Events = make([]HistoryEvent, len(histEvents))
		copy(task.Events, histEvents)
	}

	return &task
}

// RecordActivityTaskHeartbeat acknowledges a heartbeat for an activity task token.
// Returns true if cancel has been requested; always false in this emulator.
func (b *InMemoryBackend) RecordActivityTaskHeartbeat(_ string) bool {
	return false
}

// RequestCancelWorkflowExecution requests cancellation of a running execution.
func (b *InMemoryBackend) RequestCancelWorkflowExecution(domain, workflowID string) error {
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

	b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionCancelRequested")

	return nil
}

// RespondActivityTaskCanceled marks an activity task as canceled.
func (b *InMemoryBackend) RespondActivityTaskCanceled(_ string) error {
	return nil
}

// RespondActivityTaskCompleted marks an activity task as completed.
func (b *InMemoryBackend) RespondActivityTaskCompleted(_ string) error {
	return nil
}

// RespondActivityTaskFailed marks an activity task as failed.
func (b *InMemoryBackend) RespondActivityTaskFailed(_ string) error {
	return nil
}

// RespondDecisionTaskCompleted marks a decision task as completed.
func (b *InMemoryBackend) RespondDecisionTaskCompleted(_ string) error {
	return nil
}

// SignalWorkflowExecution sends a signal to a workflow execution, recording it in history.
func (b *InMemoryBackend) SignalWorkflowExecution(domain, workflowID, _, _ string) error {
	b.mu.Lock("SignalWorkflowExecution")
	defer b.mu.Unlock()

	key := domain + ":" + workflowID
	if _, ok := b.executions[key]; !ok {
		return fmt.Errorf("%w: execution %s/%s not found", ErrNotFound, domain, workflowID)
	}

	b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionSignaled")

	return nil
}

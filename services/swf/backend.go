package swf

import (
	"errors"
	"fmt"
	"time"

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
	mu             *lockmetrics.RWMutex
	executionOrder []string // FIFO order of execution keys for eviction
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		domains:    make(map[string]*Domain),
		workflows:  make(map[string]*WorkflowType),
		activities: make(map[string]*ActivityType),
		executions: make(map[string]*WorkflowExecution),
		mu:         lockmetrics.New("swf"),
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
	b.executionOrder = nil
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

	out := make([]WorkflowType, 0)
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

	out := make([]ActivityType, 0)
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

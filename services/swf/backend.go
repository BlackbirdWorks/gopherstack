package swf

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a resource is not found.
	ErrNotFound = awserr.New("UnknownResourceFault", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("DomainAlreadyExistsFault", awserr.ErrAlreadyExists)
	// ErrDeprecated is returned when a deprecated resource is used.
	ErrDeprecated = errors.New("DomainDeprecatedFault")
	// ErrTypeAlreadyExists is returned when a workflow type already exists.
	ErrTypeAlreadyExists = errors.New("TypeAlreadyExistsFault")
	// ErrTypeDeprecated is returned when a type is already deprecated.
	ErrTypeDeprecated = errors.New("TypeDeprecatedFault")
)

// maxWorkflowExecutions is the maximum number of workflow executions retained.
// Oldest executions are evicted when this limit is exceeded.
const maxWorkflowExecutions = 10_000

// statusDeprecated is the status string for deprecated resources.
const statusDeprecated = "DEPRECATED"

// statusRegistered is the status string for registered resources.
const statusRegistered = "REGISTERED"

// statusRunning is the status string for running workflow executions.
const statusRunning = "RUNNING"

// Domain represents an SWF domain.
type Domain struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Status      string `json:"status"` // REGISTERED or DEPRECATED
}

// ActivityType represents an SWF activity type.
type ActivityType struct {
	Domain      string `json:"domain"`
	Name        string `json:"name"`
	Version     string `json:"version"`
	Status      string `json:"status"`
	Description string `json:"description"`
}

// WorkflowType represents an SWF workflow type.
type WorkflowType struct {
	Domain      string `json:"domain"`
	Name        string `json:"name"`
	Version     string `json:"version"`
	Status      string `json:"status"`
	Description string `json:"description"`
}

// WorkflowExecution represents an SWF workflow execution.
type WorkflowExecution struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowID"`
	RunID      string `json:"runID"`
	Status     string `json:"status"`
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

// AddActivityTypeInternal seeds an activity type directly for testing.
func (b *InMemoryBackend) AddActivityTypeInternal(domain, name, version, status string) {
	b.mu.Lock("AddActivityTypeInternal")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	b.activities[key] = &ActivityType{Domain: domain, Name: name, Version: version, Status: status}
}

// RegisterDomain registers a new SWF domain.
func (b *InMemoryBackend) RegisterDomain(name, description string) error {
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

	d.Status = statusDeprecated

	return nil
}

// RegisterWorkflowType registers a new workflow type.
func (b *InMemoryBackend) RegisterWorkflowType(domain, name, version string) error {
	b.mu.Lock("RegisterWorkflowType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	if _, ok := b.workflows[key]; ok {
		return fmt.Errorf("%w: %s/%s", ErrTypeAlreadyExists, name, version)
	}

	b.workflows[key] = &WorkflowType{Domain: domain, Name: name, Version: version, Status: statusRegistered}

	return nil
}

// ListWorkflowTypes returns all workflow types for a domain.
func (b *InMemoryBackend) ListWorkflowTypes(domain string) []WorkflowType {
	b.mu.RLock("ListWorkflowTypes")
	defer b.mu.RUnlock()

	out := make([]WorkflowType, 0)
	for _, wt := range b.workflows {
		if wt.Domain == domain {
			out = append(out, *wt)
		}
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

// DeleteWorkflowType removes a workflow type.
func (b *InMemoryBackend) DeleteWorkflowType(domain, name, version string) error {
	b.mu.Lock("DeleteWorkflowType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	if _, ok := b.workflows[key]; !ok {
		return fmt.Errorf("%w: workflow type %s/%s not found", ErrNotFound, name, version)
	}

	delete(b.workflows, key)

	return nil
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

// DeleteActivityType removes an activity type.
func (b *InMemoryBackend) DeleteActivityType(domain, name, version string) error {
	b.mu.Lock("DeleteActivityType")
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	if _, ok := b.activities[key]; !ok {
		return fmt.Errorf("%w: activity type %s/%s not found", ErrNotFound, name, version)
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

	exec := &WorkflowExecution{Domain: domain, WorkflowID: workflowID, RunID: runID, Status: statusRunning}
	b.executions[key] = exec

	cp := *exec

	return &cp, nil
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

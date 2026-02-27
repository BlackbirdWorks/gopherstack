package swf

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrNotFound is returned when a resource is not found.
	ErrNotFound = errors.New("UnknownResourceFault")
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("DomainAlreadyExistsFault")
	// ErrDeprecated is returned when a deprecated resource is used.
	ErrDeprecated = errors.New("DomainDeprecatedFault")
)

// Domain represents an SWF domain.
type Domain struct {
	Name        string
	Description string
	Status      string // REGISTERED or DEPRECATED
}

// WorkflowType represents an SWF workflow type.
type WorkflowType struct {
	Domain  string
	Name    string
	Version string
	Status  string // REGISTERED
}

// WorkflowExecution represents an SWF workflow execution.
type WorkflowExecution struct {
	Domain     string
	WorkflowID string
	RunID      string
	Status     string // RUNNING
}

// InMemoryBackend is the in-memory store for SWF resources.
type InMemoryBackend struct {
	domains    map[string]*Domain
	workflows  map[string]*WorkflowType // key: domain+":"+name+":"+version
	executions map[string]*WorkflowExecution // key: domain+":"+workflowID
	mu         sync.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		domains:    make(map[string]*Domain),
		workflows:  make(map[string]*WorkflowType),
		executions: make(map[string]*WorkflowExecution),
	}
}

// RegisterDomain registers a new SWF domain.
func (b *InMemoryBackend) RegisterDomain(name, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if d, ok := b.domains[name]; ok {
		if d.Status == "DEPRECATED" {
			return fmt.Errorf("%w: %s", ErrDeprecated, name)
		}

		return fmt.Errorf("%w: %s", ErrAlreadyExists, name)
	}

	b.domains[name] = &Domain{Name: name, Description: description, Status: "REGISTERED"}

	return nil
}

// ListDomains returns all domains with the given status.
func (b *InMemoryBackend) ListDomains(registrationStatus string) []Domain {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]Domain, 0, len(b.domains))
	for _, d := range b.domains {
		if registrationStatus == "" || d.Status == registrationStatus {
			out = append(out, *d)
		}
	}

	return out
}

// DeprecateDomain marks a domain as deprecated.
func (b *InMemoryBackend) DeprecateDomain(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.domains[name]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	d.Status = "DEPRECATED"

	return nil
}

// RegisterWorkflowType registers a new workflow type.
func (b *InMemoryBackend) RegisterWorkflowType(domain, name, version string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := domain + ":" + name + ":" + version
	if _, ok := b.workflows[key]; ok {
		return fmt.Errorf("TypeAlreadyExistsFault: %s/%s", name, version)
	}

	b.workflows[key] = &WorkflowType{Domain: domain, Name: name, Version: version, Status: "REGISTERED"}

	return nil
}

// ListWorkflowTypes returns all workflow types for a domain.
func (b *InMemoryBackend) ListWorkflowTypes(domain string) []WorkflowType {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]WorkflowType, 0)
	for _, wt := range b.workflows {
		if wt.Domain == domain {
			out = append(out, *wt)
		}
	}

	return out
}

// StartWorkflowExecution starts a new workflow execution.
func (b *InMemoryBackend) StartWorkflowExecution(domain, workflowID, runID string) (*WorkflowExecution, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := domain + ":" + workflowID
	exec := &WorkflowExecution{Domain: domain, WorkflowID: workflowID, RunID: runID, Status: "RUNNING"}
	b.executions[key] = exec

	cp := *exec

	return &cp, nil
}

// DescribeWorkflowExecution returns a workflow execution.
func (b *InMemoryBackend) DescribeWorkflowExecution(domain, workflowID string) (*WorkflowExecution, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	key := domain + ":" + workflowID
	exec, ok := b.executions[key]
	if !ok {
		return nil, fmt.Errorf("%w: execution %s/%s not found", ErrNotFound, domain, workflowID)
	}

	cp := *exec

	return &cp, nil
}

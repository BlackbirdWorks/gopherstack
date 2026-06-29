package swf

import "context"

// StorageBackend defines the interface for SWF backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Domain lifecycle
	RegisterDomain(name, description, retention string) error
	DescribeDomain(name string) (*Domain, error)
	ListDomains(registrationStatus string) ([]Domain, error)
	DeprecateDomain(name string) error
	UndeprecateDomain(name string) error

	// WorkflowType lifecycle
	RegisterWorkflowType(domain, name, version, description string, defaults WorkflowTypeDefaults) error
	ListWorkflowTypes(domain, registrationStatus string) ([]WorkflowType, error)
	DescribeWorkflowType(domain, name, version string) (*WorkflowType, error)
	DeprecateWorkflowType(domain, name, version string) error
	UndeprecateWorkflowType(domain, name, version string) error

	// ActivityType lifecycle
	RegisterActivityType(domain, name, version, description string, defaults ActivityTypeDefaults) error
	ListActivityTypes(domain, registrationStatus string) ([]ActivityType, error)
	DescribeActivityType(domain, name, version string) (*ActivityType, error)
	DeprecateActivityType(domain, name, version string) error
	UndeprecateActivityType(domain, name, version string) error

	// Execution counts
	CountOpenWorkflowExecutions(domain string, filter ExecutionFilter) int
	CountClosedWorkflowExecutions(domain string, filter ExecutionFilter) int
	CountPendingActivityTasks(domain, taskList string) int
	CountPendingDecisionTasks(domain, taskList string) int

	// Execution lifecycle
	StartWorkflowExecution(input StartWorkflowExecutionInput) (*WorkflowExecution, error)
	TerminateWorkflowExecution(domain, workflowID, runID, reason, details string) error
	DescribeWorkflowExecution(domain, workflowID string) (*WorkflowExecution, error)
	GetWorkflowExecutionHistory(
		domain, workflowID string,
		maxPageSize int,
		nextPageToken string,
		reverseOrder bool,
	) ([]HistoryEvent, string)
	ListOpenWorkflowExecutions(domain string, filter ExecutionFilter) []WorkflowExecution
	ListClosedWorkflowExecutions(domain string, filter ExecutionFilter) []WorkflowExecution
	RequestCancelWorkflowExecution(domain, workflowID, runID string) error
	SignalWorkflowExecution(domain, workflowID, runID, signalName, input string) error

	// Task polling and responses
	PollForActivityTask(domain, taskList string) *ActivityTask
	PollForDecisionTask(domain, taskList string, maxPageSize int, nextPageToken string) *DecisionTask
	RecordActivityTaskHeartbeat(taskToken string) (bool, error)
	RespondActivityTaskCanceled(taskToken, details string) error
	RespondActivityTaskCompleted(taskToken, result string) error
	RespondActivityTaskFailed(taskToken, reason, details string) error
	RespondDecisionTaskCompleted(taskToken, executionContext string, decisions []Decision) error

	// Resource tagging
	ListTagsForResource(resourceARN string) (map[string]string, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error

	// Backend lifecycle
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

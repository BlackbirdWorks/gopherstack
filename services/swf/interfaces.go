package swf

// StorageBackend defines the interface for SWF backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Domain lifecycle
	RegisterDomain(name, description string) error
	DescribeDomain(name string) (*Domain, error)
	ListDomains(registrationStatus string) []Domain
	DeprecateDomain(name string) error
	UndeprecateDomain(name string) error

	// WorkflowType lifecycle
	RegisterWorkflowType(domain, name, version, description string) error
	ListWorkflowTypes(domain, registrationStatus string) []WorkflowType
	DescribeWorkflowType(domain, name, version string) (*WorkflowType, error)
	DeprecateWorkflowType(domain, name, version string) error
	UndeprecateWorkflowType(domain, name, version string) error
	DeleteWorkflowType(domain, name, version string) error

	// ActivityType lifecycle
	RegisterActivityType(domain, name, version, description string) error
	ListActivityTypes(domain, registrationStatus string) []ActivityType
	DescribeActivityType(domain, name, version string) (*ActivityType, error)
	DeprecateActivityType(domain, name, version string) error
	UndeprecateActivityType(domain, name, version string) error
	DeleteActivityType(domain, name, version string) error

	// Execution counts
	CountOpenWorkflowExecutions(domain string) int
	CountClosedWorkflowExecutions(domain string) int
	CountPendingActivityTasks(taskListName string) int
	CountPendingDecisionTasks(taskListName string) int

	// Execution lifecycle
	StartWorkflowExecution(domain, workflowID, runID string) (*WorkflowExecution, error)
	TerminateWorkflowExecution(domain, workflowID string) error
	DescribeWorkflowExecution(domain, workflowID string) (*WorkflowExecution, error)

	// Backend lifecycle
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

package swf

// StorageBackend defines the interface for SWF backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	RegisterDomain(name, description string) error
	DescribeDomain(name string) (*Domain, error)
	ListDomains(registrationStatus string) []Domain
	DeprecateDomain(name string) error
	RegisterWorkflowType(domain, name, version string) error
	ListWorkflowTypes(domain string) []WorkflowType
	DescribeWorkflowType(domain, name, version string) (*WorkflowType, error)
	DeprecateWorkflowType(domain, name, version string) error
	DeleteWorkflowType(domain, name, version string) error
	DescribeActivityType(domain, name, version string) (*ActivityType, error)
	DeprecateActivityType(domain, name, version string) error
	DeleteActivityType(domain, name, version string) error
	CountOpenWorkflowExecutions(domain string) int
	CountClosedWorkflowExecutions(domain string) int
	CountPendingActivityTasks(taskListName string) int
	CountPendingDecisionTasks(taskListName string) int
	StartWorkflowExecution(domain, workflowID, runID string) (*WorkflowExecution, error)
	DescribeWorkflowExecution(domain, workflowID string) (*WorkflowExecution, error)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

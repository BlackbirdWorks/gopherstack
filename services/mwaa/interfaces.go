package mwaa

// StorageBackend is the interface for the MWAA in-memory backend.
type StorageBackend interface {
	// Environment CRUD
	CreateEnvironment(region, accountID, name string, req *createEnvironmentRequest) (*Environment, error)
	GetEnvironment(name string) (*Environment, error)
	DeleteEnvironment(name string) (*Environment, error)
	UpdateEnvironment(name string, req *updateEnvironmentRequest) (*Environment, error)
	ListEnvironments() ([]string, error)
	ListEnvironmentsPage(nextToken string, pageSize int) ([]string, string, error)

	// Tag operations
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	// REST API / metrics
	InvokeRestAPI(envName string, req *invokeRestAPIRequest) (*InvokeRestAPIResponse, error)
	PublishMetrics(envName string, req *publishMetricsRequest) error
	GetMetrics(envName string) ([]MetricDatum, error)

	// Token operations
	CreateCliToken(envName string) (string, error)
	CreateWebLoginToken(envName string) (string, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

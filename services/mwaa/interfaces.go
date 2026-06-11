package mwaa

import "context"

// StorageBackend is the interface for the MWAA in-memory backend.
// All per-resource operations take a context.Context carrying the request's
// AWS region so resources are isolated per region.
type StorageBackend interface {
	// Environment CRUD
	CreateEnvironment(ctx context.Context, name string, req *createEnvironmentRequest) (*Environment, error)
	GetEnvironment(ctx context.Context, name string) (*Environment, error)
	DeleteEnvironment(ctx context.Context, name string) (*Environment, error)
	UpdateEnvironment(ctx context.Context, name string, req *updateEnvironmentRequest) (*Environment, error)
	ListEnvironments(ctx context.Context) ([]string, error)
	ListEnvironmentsPage(ctx context.Context, nextToken string, pageSize int) ([]string, string, error)

	// Tag operations
	TagResource(ctx context.Context, resourceARN string, tags map[string]string) error
	UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error
	ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error)

	// REST API / metrics
	InvokeRestAPI(ctx context.Context, envName string, req *invokeRestAPIRequest) (*InvokeRestAPIResponse, error)
	PublishMetrics(ctx context.Context, envName string, req *publishMetricsRequest) error
	GetMetrics(ctx context.Context, envName string) ([]MetricDatum, error)

	// Token operations
	CreateCliToken(ctx context.Context, envName string) (string, error)
	CreateWebLoginToken(ctx context.Context, envName string) (string, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

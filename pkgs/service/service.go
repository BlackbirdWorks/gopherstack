package service

import (
	"context"
	"io"

	"github.com/labstack/echo/v5"
)

// Middleware defines a function that wraps an Echo handler.
type Middleware func(echo.HandlerFunc) echo.HandlerFunc

// Service represents an AWS-compatible service that can be registered
// with the service router. Each service provides an Echo handler,
// routing matcher, and observability information.
type Service interface {
	// Name returns the service identifier (e.g., "DynamoDB", "S3").
	// Used in metrics labels and logging.
	Name() string

	// Handler returns the Echo handler function for this service.
	Handler() echo.HandlerFunc

	// RouteMatcher returns a function that determines if an incoming
	// Echo request should be routed to this service. Matchers are
	// evaluated in registration order. Return true to route here,
	// false to continue to next service.
	RouteMatcher() Matcher

	// GetSupportedOperations returns a list of operations this service
	// supports (e.g., ["GetItem", "PutItem", "Query"] for DynamoDB).
	GetSupportedOperations() []string
}

// Matcher determines whether an incoming request should be routed
// to a particular service handler. Matchers are evaluated in
// registration order, and the first match wins.
type Matcher func(c *echo.Context) bool

// ResourceObserver extracts metrics labels from requests for a specific
// service. Used by the metrics wrapper to instrument operations.
type ResourceObserver interface {
	// ExtractOperation returns the operation name (e.g., "GetItem", "PutObject").
	// Used in metrics labels.
	ExtractOperation(c *echo.Context) string

	// ExtractResource returns the resource identifier (e.g., table name, bucket name).
	// Used in metrics labels.
	ExtractResource(c *echo.Context) string
}

// BlobStore defines a generic interface for object storage,
// allowing other services to store data without depending on the S3 package directly.
type BlobStore interface {
	PutObject(ctx context.Context, bucket, key string, body io.Reader) error
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	DeleteObject(ctx context.Context, bucket, key string) error
}

// KeyValueStore defines a generic interface for key-value storage.
type KeyValueStore interface {
	PutItem(ctx context.Context, table string, item map[string]any) error
	GetItem(ctx context.Context, table string, key map[string]any) (map[string]any, error)
}

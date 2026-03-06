package service

import (
	"context"
	"log/slog"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
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
// to a particular service handler. Matchers are evaluated by priority
// (higher priority = evaluated first).
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

// Registerable combines Service and ResourceObserver into a single interface
// for services that want to be registered with the service registry.
// This unified interface simplifies the registration contract.
type Registerable interface {
	Service
	ResourceObserver

	// MatchPriority returns the priority for this service's matcher.
	// Higher values are evaluated first. Examples:
	// - Header-based matchers (DynamoDB): 100
	// - Path-based matchers (Dashboard): 50
	// - Catch-all matchers (S3): 0
	MatchPriority() int
}

// DashboardProvider is an optional interface that services can implement
// to provide a dashboard UI. The dashboard automatically discovers and
// integrates services that implement this interface.
type DashboardProvider interface {
	// DashboardName returns the user-facing name for this service's dashboard tab.
	// Example: "DynamoDB", "S3"
	DashboardName() string

	// DashboardRoutePrefix returns the URL path prefix for this service's dashboard routes.
	// Example: "dynamodb", "s3"
	DashboardRoutePrefix() string

	// RegisterDashboardRoutes registers all dashboard routes for this service
	// under the given Echo group. The group is mounted at /dashboard/{prefix}.
	// The httpClient and endpoint are provided for services that need to make
	// SDK calls back to the service (e.g., to list tables or buckets).
	RegisterDashboardRoutes(
		group *echo.Group,
		httpClient any, // *http.Client
		endpoint string,
	)
}

// ChaosProvider is an optional interface services implement to declare
// their chaos-injectable surface area. Services that implement this interface
// are automatically discovered by the Chaos API via the registry.
type ChaosProvider interface {
	// ChaosServiceName returns the lowercase AWS-style service name used in
	// fault rules (e.g. "s3", "dynamodb", "sqs").
	ChaosServiceName() string

	// ChaosOperations returns all operations that can be fault-injected.
	// Implementations typically delegate to GetSupportedOperations().
	ChaosOperations() []string

	// ChaosRegions returns all regions this service instance handles.
	// Typically returns the configured default region plus any regions
	// that have active resources.
	ChaosRegions() []string
}

// BackgroundWorker is an optional interface that services can implement
// to start background tasks (e.g. async deletion janitors).
type BackgroundWorker interface {
	StartWorker(ctx context.Context) error
}

// AppContext contains shared resources needed by services during initialization.
type AppContext struct {
	Logger     *slog.Logger
	Config     any // The raw configuration object
	JanitorCtx context.Context
	// PortAlloc is the shared port allocator for services that need dedicated ports (e.g. Lambda).
	// May be nil if the port range was not configured.
	PortAlloc *portalloc.Allocator
}

// Provider encapsulates the logic to initialize a service.
type Provider interface {
	// Name returns the name of the service provider.
	Name() string

	// Init initializes the service using the provided application context.
	// It returns the Registerable service, or an error if initialization fails.
	Init(ctx *AppContext) (Registerable, error)
}

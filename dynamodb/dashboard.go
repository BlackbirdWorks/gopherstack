package dynamodb

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// DashboardProvider implements the service.DashboardProvider interface
// to enable DynamoDB dashboard discovery and route registration.
// It wraps a reference to dashboard handler functions.
type DashboardProvider struct {
	// Handlers will be set by the dashboard handler during initialization
	Handlers DashboardHandlers
}

// DashboardHandlers defines the functions needed for DynamoDB dashboard routes.
type DashboardHandlers struct {
	HandleDynamoDB func(http.ResponseWriter, *http.Request, string)
}

// NewDashboardProvider creates a new DynamoDB dashboard provider.
// The handlers must be set by the dashboard initialization code before
// any routes are registered.
func NewDashboardProvider() *DashboardProvider {
	return &DashboardProvider{
		Handlers: DashboardHandlers{},
	}
}

// DashboardName returns the user-facing name for the DynamoDB dashboard tab.
const dynamoDBName = "DynamoDB"

func (p *DashboardProvider) DashboardName() string {
	return dynamoDBName
}

// DashboardRoutePrefix returns the URL path prefix for DynamoDB dashboard routes.
func (p *DashboardProvider) DashboardRoutePrefix() string {
	return "dynamodb"
}

// RegisterDashboardRoutes registers all DynamoDB dashboard routes under the given Echo group.
// The group is mounted at /dashboard/dynamodb by the dashboard handler.
func (p *DashboardProvider) RegisterDashboardRoutes(
	group *echo.Group,
	_ any, // *http.Client (typed inside service)
	_ string,
) {
	// Register catch-all route for DynamoDB paths
	group.Any("*", func(c *echo.Context) error {
		path := c.Param("*")
		p.Handlers.HandleDynamoDB(c.Response(), c.Request(), path)

		return nil
	})

	// Root path redirect
	group.Any("", func(c *echo.Context) error {
		p.Handlers.HandleDynamoDB(c.Response(), c.Request(), "")

		return nil
	})
}

package s3

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// DashboardProvider implements the service.DashboardProvider interface
// to enable S3 dashboard discovery and route registration.
// It wraps a reference to dashboard handler functions.
type DashboardProvider struct {
	// Handlers will be set by the dashboard handler during initialization
	Handlers DashboardHandlers
}

// DashboardHandlers defines the functions needed for S3 dashboard routes.
type DashboardHandlers struct {
	HandleS3 func(http.ResponseWriter, *http.Request, string)
}

// NewDashboardProvider creates a new S3 dashboard provider.
// The handlers must be set by the dashboard initialization code before
// any routes are registered.
func NewDashboardProvider() *DashboardProvider {
	return &DashboardProvider{
		Handlers: DashboardHandlers{},
	}
}

// DashboardName returns the user-facing name for the S3 dashboard tab.
func (p *DashboardProvider) DashboardName() string {
	return "S3"
}

// DashboardRoutePrefix returns the URL path prefix for S3 dashboard routes.
func (p *DashboardProvider) DashboardRoutePrefix() string {
	return "s3"
}

// RegisterDashboardRoutes registers all S3 dashboard routes under the given Echo group.
// The group is mounted at /dashboard/s3 by the dashboard handler.
func (p *DashboardProvider) RegisterDashboardRoutes(
	group *echo.Group,
	_ any, // *http.Client (typed inside service)
	_ string,
) {
	// Register catch-all route for S3 paths
	group.Any("*", func(c *echo.Context) error {
		path := c.Param("*")
		p.Handlers.HandleS3(c.Response(), c.Request(), path)

		return nil
	})

	// Root path redirect
	group.Any("", func(c *echo.Context) error {
		p.Handlers.HandleS3(c.Response(), c.Request(), "")

		return nil
	})
}

package service

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// Router evaluates matchers in order and routes requests to the
// first matching service. Implements centralized routing logic that replaces
// scattered pre-middleware and manual routing checks.
type Router struct {
	services []*Entry
}

// NewServiceRouter creates a router from the registered services.
// Services must already be registered in the registry in desired order.
func NewServiceRouter(registry *Registry) *Router {
	return &Router{
		services: registry.GetAll(),
	}
}

// RouteHandler returns an Echo handler that evaluates all registered
// service matchers in order and routes to the first matching service.
// If no service matches, returns a 404 error.
func (r *Router) RouteHandler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Evaluate matchers in registration order
		for _, entry := range r.services {
			if entry.Matcher(c) {
				// Found matching service, call pre-wrapped handler
				return entry.WrappedHandler(c)
			}
		}

		// No service matched
		return c.JSON(http.StatusNotFound, map[string]string{
			"error": "not found",
		})
	}
}

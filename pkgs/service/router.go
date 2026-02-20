package service

import (
	"sort"

	"github.com/labstack/echo/v5"
)

// Router evaluates matchers by priority and routes requests to the
// first matching service. Implements centralized routing logic that replaces
// scattered pre-middleware and manual routing checks.
type Router struct {
	services []*Entry
}

// NewServiceRouter creates a router from the registered services.
// Services are sorted by priority (highest first) for evaluation.
func NewServiceRouter(registry *Registry) *Router {
	services := registry.GetAll()

	// Sort by priority (descending)
	sort.Slice(services, func(i, j int) bool {
		return services[i].Priority > services[j].Priority
	})

	return &Router{
		services: services,
	}
}

// RouteHandler returns an Echo middleware that evaluates all registered
// service matchers by priority and routes to the first matching service.
// If no service matches, it falls back to the next handler (standard Echo routing).
func (r *Router) RouteHandler() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			// Evaluate matchers in priority order (highest priority first)
			for _, entry := range r.services {
				if entry.Matcher(c) {
					// Found matching service, call pre-wrapped handler
					return entry.WrappedHandler(c)
				}
			}

			// No service matched, fall back to standard Echo routing
			return next(c)
		}
	}
}

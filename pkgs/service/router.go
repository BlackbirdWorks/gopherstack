package service

import (
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const amzTargetHeader = "X-Amz-Target"

// Router evaluates matchers by priority and routes requests to the
// first matching service. Implements centralized routing logic that replaces
// scattered pre-middleware and manual routing checks.
type Router struct {
	services []*Entry
	gates    [][]string
}

// NewServiceRouter creates a router from the registered services.
// Services are sorted by priority (highest first) for evaluation.
func NewServiceRouter(registry *Registry) *Router {
	services := registry.GetAll()

	// Sort by priority (descending). Use SliceStable so that services registered at the
	// same priority retain their original registration order. This prevents new service
	// additions from non-deterministically reordering existing services at the same
	// priority level, which would cause intermittent routing conflicts.
	sort.SliceStable(services, func(i, j int) bool {
		return services[i].Priority > services[j].Priority
	})

	return &Router{
		services: services,
		gates:    make([][]string, len(services)),
	}
}

// WithTargetGates declares, by service name, X-Amz-Target prefixes outside which that
// service's matcher is known to return false, so the scan can skip it without calling it.
func (r *Router) WithTargetGates(gates map[string][]string) *Router {
	for i, entry := range r.services {
		r.gates[i] = gates[entry.Registerable.Name()]
	}

	return r
}

// RouteHandler returns an Echo middleware that evaluates all registered
// service matchers by priority and routes to the first matching service.
// If no service matches, it falls back to the next handler (standard Echo routing).
func (r *Router) RouteHandler() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if entry := r.Lookup(c); entry != nil {
				return entry.WrappedHandler(c)
			}

			// No service matched, fall back to standard Echo routing
			return next(c)
		}
	}
}

// Lookup returns the service entry the router selects for c, or nil if none matches.
func (r *Router) Lookup(c *echo.Context) *Entry {
	target := extractTargetHeader(c)

	for i, entry := range r.services {
		if gate := r.gates[i]; gate != nil && !hasAnyPrefix(target, gate) {
			continue
		}

		if entry.Matcher(c) {
			return entry
		}
	}

	return nil
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}

	return false
}

func extractTargetHeader(c *echo.Context) string {
	req := c.Request()
	if req == nil {
		return ""
	}

	return httputils.HeaderValue(req.Header, amzTargetHeader)
}

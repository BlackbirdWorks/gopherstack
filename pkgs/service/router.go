package service

import (
	"sort"
	"strings"
	"sync"

	"github.com/labstack/echo/v5"
)

const amzTargetHeader = "X-Amz-Target"

// Router evaluates matchers by priority and routes requests to the
// first matching service. Implements centralized routing logic that replaces
// scattered pre-middleware and manual routing checks.
type Router struct {
	targetCache sync.Map
	services    []*Entry
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
	}
}

// RouteHandler returns an Echo middleware that evaluates all registered
// service matchers by priority and routes to the first matching service.
// If no service matches, it falls back to the next handler (standard Echo routing).
func (r *Router) RouteHandler() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if entry := r.matchFastPath(c); entry != nil {
				return entry.WrappedHandler(c)
			}

			// Evaluate matchers in priority order (highest priority first)
			for _, entry := range r.services {
				if entry.Matcher(c) {
					r.recordTargetFastPath(c, entry)

					return entry.WrappedHandler(c)
				}
			}

			// No service matched, fall back to standard Echo routing
			return next(c)
		}
	}
}

func (r *Router) matchFastPath(c *echo.Context) *Entry {
	target := extractTargetHeader(c)
	if target == "" {
		return nil
	}

	prefix := extractTargetPrefix(target)
	if prefix == "" {
		return nil
	}

	val, ok := r.targetCache.Load(prefix)
	if !ok {
		return nil
	}

	entry, ok := val.(*Entry)
	if !ok || !entry.Matcher(c) {
		return nil
	}

	return entry
}

func (r *Router) recordTargetFastPath(c *echo.Context, entry *Entry) {
	target := extractTargetHeader(c)
	if target == "" {
		return
	}

	prefix := extractTargetPrefix(target)
	if prefix == "" {
		return
	}

	r.targetCache.Store(prefix, entry)
}

func extractTargetHeader(c *echo.Context) string {
	req := c.Request()
	if req == nil {
		return ""
	}

	return req.Header.Get(amzTargetHeader)
}

func extractTargetPrefix(target string) string {
	dot := strings.IndexByte(target, '.')
	if dot <= 0 {
		return ""
	}

	return target[:dot+1]
}

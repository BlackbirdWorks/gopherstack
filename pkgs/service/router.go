package service

import (
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	amzTargetHeader  = "X-Amz-Target"
	minTargetGateLen = 1
	minPathGateLen   = 2
)

// Router evaluates matchers by priority and routes requests to the
// first matching service. Implements centralized routing logic that replaces
// scattered pre-middleware and manual routing checks.
type Router struct {
	services    []*Entry
	targetGates [][]string
	pathGates   [][]string
	idx         routeIndex
}

// NewServiceRouter creates a router from the registered services.
// Services are sorted by priority (highest first) for evaluation.
func NewServiceRouter(registry *Registry) *Router {
	services := registry.GetAll()

	// SliceStable keeps registration order within a priority tier so adding a service
	// never reorders existing ones at the same priority.
	sort.SliceStable(services, func(i, j int) bool {
		return services[i].Priority > services[j].Priority
	})

	r := &Router{
		services:    services,
		targetGates: make([][]string, len(services)),
		pathGates:   make([][]string, len(services)),
	}
	r.idx = buildRouteIndex(r.targetGates, r.pathGates)

	return r
}

// WithTargetGates declares, by service name, X-Amz-Target prefixes outside which that
// service's matcher is known to return false, so the scan can skip it without calling it.
// Call before serving: the router is immutable afterwards.
func (r *Router) WithTargetGates(gates map[string][]string) *Router {
	for i, entry := range r.services {
		r.targetGates[i] = validGate(gates[entry.Registerable.Name()], minTargetGateLen)
	}

	r.idx = buildRouteIndex(r.targetGates, r.pathGates)

	return r
}

// WithPathGates declares path prefixes outside which a service's matcher returns false; a
// prefix needs "/" plus one byte or the service stays ungated. Call before serving.
func (r *Router) WithPathGates(gates map[string][]string) *Router {
	for i, entry := range r.services {
		r.pathGates[i] = validGate(gates[entry.Registerable.Name()], minPathGateLen)
	}

	r.idx = buildRouteIndex(r.targetGates, r.pathGates)

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
// Only services whose gate keys fit the request are tried, in priority order.
func (r *Router) Lookup(c *echo.Context) *Entry {
	target := extractTargetHeader(c)
	path := extractPath(c)

	var pathBucket, targetBucket []int

	if len(path) > 1 && path[0] == '/' {
		pathBucket = r.idx.byPath[path[1]]
	}

	if target != "" {
		targetBucket = r.idx.byTarget[target[0]]
	}

	var ia, ip, it int

	for {
		i := nextCandidate(r.idx.always, pathBucket, targetBucket, &ia, &ip, &it)
		if i < 0 {
			return nil
		}

		if g := r.targetGates[i]; g != nil && !hasAnyPrefix(target, g) {
			continue
		}

		if g := r.pathGates[i]; g != nil && !hasAnyPrefix(path, g) {
			continue
		}

		if entry := r.services[i]; entry.Matcher(c) {
			return entry
		}
	}
}

// lookupLinear is the reference ungated first-match scan the index must agree with.
func (r *Router) lookupLinear(c *echo.Context) *Entry {
	for _, entry := range r.services {
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

func extractPath(c *echo.Context) string {
	req := c.Request()
	if req == nil || req.URL == nil {
		return ""
	}

	return req.URL.Path
}

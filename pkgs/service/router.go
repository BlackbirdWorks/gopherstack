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
	idx         routeIndex
	intercept   Interceptor
	services    []*Entry
	targetGates [][]string
	pathGates   [][]string
	cborGates   []string
}

// Interceptor sees every request after the built-in lookup (entry is nil when no service
// matched). A non-nil handler replaces the default dispatch; nil leaves it unchanged.
type Interceptor func(c *echo.Context, entry *Entry) echo.HandlerFunc

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
		cborGates:   make([]string, len(services)),
	}
	r.idx = buildRouteIndex(r.targetGates, r.pathGates, r.cborGates)

	return r
}

// WithTargetGates declares, by service name, X-Amz-Target prefixes outside which that
// service's matcher is known to return false, so the scan can skip it without calling it.
// Call before serving: the router is immutable afterwards.
func (r *Router) WithTargetGates(gates map[string][]string) *Router {
	for i, entry := range r.services {
		r.targetGates[i] = validGate(gates[entry.Registerable.Name()], minTargetGateLen)
	}

	r.idx = buildRouteIndex(r.targetGates, r.pathGates, r.cborGates)

	return r
}

// WithCBORGates names, by service, the Smithy service shape whose rpcv2Cbor calls
// (/service/<name>/operation/<Op>) also pass that service's target gate. Call before serving.
func (r *Router) WithCBORGates(names map[string]string) *Router {
	for i, entry := range r.services {
		r.cborGates[i] = names[entry.Registerable.Name()]
	}

	r.idx = buildRouteIndex(r.targetGates, r.pathGates, r.cborGates)

	return r
}

// WithPathGates declares path prefixes outside which a service's matcher returns false; a
// prefix needs "/" plus one byte or the service stays ungated. Call before serving.
func (r *Router) WithPathGates(gates map[string][]string) *Router {
	for i, entry := range r.services {
		r.pathGates[i] = validGate(gates[entry.Registerable.Name()], minPathGateLen)
	}

	r.idx = buildRouteIndex(r.targetGates, r.pathGates, r.cborGates)

	return r
}

// WithInterceptor installs i (nil clears it). Call before serving: the router is immutable afterwards.
func (r *Router) WithInterceptor(i Interceptor) *Router {
	r.intercept = i

	return r
}

// RouteHandler returns an Echo middleware that evaluates all registered
// service matchers by priority and routes to the first matching service.
// If no service matches, it falls back to the next handler (standard Echo routing).
func (r *Router) RouteHandler() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			entry := r.Lookup(c)

			if r.intercept != nil {
				if h := r.intercept(c, entry); h != nil {
					return h(c)
				}
			}

			if entry != nil {
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
	rawPath := extractPath(c)
	path := gatePath(rawPath)

	var pathBucket, targetBucket []int

	if len(path) > 1 && path[0] == '/' {
		pathBucket = r.idx.byPath[path[1]]
	}

	if target != "" {
		targetBucket = r.idx.byTarget[target[0]]
	}

	cborName := rpcv2CBORServiceName(rawPath)
	if cb := r.idx.byCBOR[cborName]; cborName != "" && len(cb) > 0 {
		targetBucket = mergeAscending(targetBucket, cb)
	}

	var ia, ip, it int

	for {
		i := nextCandidate(r.idx.always, pathBucket, targetBucket, &ia, &ip, &it)
		if i < 0 {
			return nil
		}

		if g := r.targetGates[i]; g != nil && !hasAnyPrefix(target, g) &&
			(cborName == "" || r.cborGates[i] != cborName) {
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

// gatePath collapses leading slashes to one: some matchers trim them before matching,
// and a gate only has to admit what its matcher can claim.
func gatePath(p string) string {
	if len(p) < 2 || p[0] != '/' || p[1] != '/' {
		return p
	}

	k := 2
	for k < len(p) && p[k] == '/' {
		k++
	}

	return p[k-1:]
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

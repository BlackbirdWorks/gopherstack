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
	altGates    []*AltGate
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
		altGates:    make([]*AltGate, len(services)),
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

// WithAltGates declares, by service name, OR-alternative gates (see AltGate). Alt-gated
// services stay in the always-visited list and are skipped cheaply before their matcher.
// Call before serving.
func (r *Router) WithAltGates(gates map[string]AltGate) *Router {
	for i, entry := range r.services {
		r.altGates[i] = nil

		if g, ok := gates[entry.Registerable.Name()]; ok && g.valid() {
			r.altGates[i] = &g
		}
	}

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
	q := lookupCtx{req: c.Request(), target: extractTargetHeader(c), rawPath: extractPath(c)}
	q.path = gatePath(q.rawPath)

	var pathBucket, targetBucket []int

	var pathPrefixes [][]string

	if len(q.path) > 1 && q.path[0] == '/' {
		pathBucket = r.idx.byPath[q.path[1]]
		pathPrefixes = r.idx.byPathPf[q.path[1]]
	}

	if q.target != "" {
		targetBucket = r.idx.byTarget[q.target[0]]
	}

	q.cborName = rpcv2CBORServiceName(q.rawPath)
	if cb := r.idx.byCBOR[q.cborName]; q.cborName != "" && len(cb) > 0 {
		targetBucket = mergeAscending(targetBucket, cb)
	}

	var ia, ip, it int

	for {
		i, src := nextCandidate(r.idx.always, pathBucket, targetBucket, &ia, &ip, &it)
		if i < 0 {
			return nil
		}

		var pfx []string

		if src == fromPath {
			pfx = pathPrefixes[ip-1]
		}

		if !r.gatesPass(i, pfx, &q) {
			continue
		}

		if entry := r.services[i]; entry.Matcher(c) {
			return entry
		}
	}
}

// gatesPass applies every gate of service i; pathPfx, when non-nil, is the part of its path
// gate that can match q.path (see addKeyedPrefixes).
func (r *Router) gatesPass(i int, pathPfx []string, q *lookupCtx) bool {
	if g := r.targetGates[i]; g != nil && !hasAnyPrefix(q.target, g) &&
		(q.cborName == "" || r.cborGates[i] != q.cborName) {
		return false
	}

	if g := r.pathGates[i]; g != nil {
		if pathPfx == nil {
			pathPfx = g
		}

		if !hasAnyPrefix(q.path, pathPfx) {
			return false
		}
	}

	g := r.altGates[i]

	return g == nil || g.admits(q)
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

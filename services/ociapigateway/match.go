package ociapigateway

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

var (
	errPathParam    = errors.New("not a valid path parameter")
	errWildcardLast = errors.New("the wildcard parameter must be the last segment")
	errPathChars    = errors.New("path contains characters that are not allowed")
)

type segKind int

const (
	segWild segKind = iota
	segParam
	segLiteral
)

type segment struct {
	lit  string
	name string
	kind segKind
}

type compiledRoute struct {
	doc     *routeDoc
	methods map[string]bool
	segs    []segment
}

type compiledSpec struct {
	doc    *specDoc
	routes []*compiledRoute
}

func splitSegments(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}

	return strings.Split(p, "/")
}

func compileRoute(r *routeDoc, idx int) (*compiledRoute, *APIError) {
	where := "routes[" + strconv.Itoa(idx) + "]"

	if !strings.HasPrefix(r.Path, "/") || strings.Contains(r.Path, "//") {
		return nil, errParam(where + ".path must start with a forward slash and not contain adjacent slashes")
	}

	cr := &compiledRoute{doc: r, methods: map[string]bool{}}

	parts := splitSegments(r.Path)
	for i, p := range parts {
		seg, err := compileSegment(p, i == len(parts)-1)
		if err != nil {
			return nil, errParam(where + ".path: " + err.Error())
		}

		cr.segs = append(cr.segs, seg)
	}

	if err := validateMethods(r.Methods); err != nil {
		return nil, err
	}

	for _, m := range r.Methods {
		cr.methods[m] = true
	}

	if len(r.Methods) == 0 {
		cr.methods[methodAny] = true
	}

	if err := validateBackend(r.Backend, 0); err != nil {
		return nil, err
	}

	if err := validateRouteRequestPolicies(r.RequestPolicies); err != nil {
		return nil, err
	}

	if r.ResponsePolicies != nil && isPresent(r.ResponsePolicies.ResponseCacheStore) {
		return nil, errUnsupported("route responsePolicies.responseCacheStore")
	}

	return cr, nil
}

func compileSegment(p string, last bool) (segment, error) {
	if strings.ContainsAny(p, "{}") {
		m := paramRe.FindStringSubmatch(p)
		if m == nil {
			return segment{}, fmt.Errorf("%w: %q (use {name} or {name*})", errPathParam, p)
		}

		if m[2] == "*" {
			if !last {
				return segment{}, fmt.Errorf("%w: %q", errWildcardLast, p)
			}

			return segment{kind: segWild, name: m[1]}, nil
		}

		return segment{kind: segParam, name: m[1]}, nil
	}

	if !pathCharsRe.MatchString(p) {
		return segment{}, fmt.Errorf("%w: %q", errPathChars, p)
	}

	return segment{kind: segLiteral, lit: p}, nil
}

// shape renders a route path with parameter names erased, for duplicate detection.
func (c *compiledRoute) shape() string {
	var sb strings.Builder

	for _, s := range c.segs {
		switch s.kind {
		case segLiteral:
			sb.WriteString("/" + s.lit)
		case segParam:
			sb.WriteString("/{}")
		case segWild:
			sb.WriteString("/{*}")
		}
	}

	return sb.String()
}

func checkDuplicateRoutes(routes []*compiledRoute) *APIError {
	seen := map[string]map[string]bool{}

	for _, r := range routes {
		shape := r.shape()
		if seen[shape] == nil {
			seen[shape] = map[string]bool{}
		}

		for m := range r.methods {
			if seen[shape][m] || seen[shape][methodAny] || (m == methodAny && len(seen[shape]) > 0) {
				return errParam("duplicate route for path " + r.doc.Path + " and method " + m)
			}

			seen[shape][m] = true
		}
	}

	return nil
}

// sortRoutes orders routes most specific first: at the first segment where two routes differ, a literal
// beats a parameter, which beats a wildcard; at a shared prefix the shorter (exact) template wins
// over a trailing wildcard.
func sortRoutes(routes []*compiledRoute) {
	slices.SortStableFunc(routes, func(a, b *compiledRoute) int {
		for i := 0; i < len(a.segs) && i < len(b.segs); i++ {
			if c := cmp.Compare(b.segs[i].kind, a.segs[i].kind); c != 0 {
				return c
			}
		}

		return cmp.Compare(len(a.segs), len(b.segs))
	})
}

func (c *compiledRoute) matchPath(parts []string) (map[string]string, bool) {
	params := map[string]string{}

	for i, s := range c.segs {
		switch s.kind {
		case segWild:
			if i <= len(parts) {
				params[s.name] = strings.Join(parts[i:], "/")

				return params, true
			}

			return nil, false
		case segParam:
			if i >= len(parts) {
				return nil, false
			}

			params[s.name] = parts[i]
		case segLiteral:
			if i >= len(parts) || parts[i] != s.lit {
				return nil, false
			}
		}
	}

	return params, len(parts) == len(c.segs)
}

func (c *compiledRoute) allows(method string) bool {
	return c.methods[methodAny] || c.methods[method]
}

type matchResult struct {
	route  *compiledRoute
	params map[string]string
	allow  []string
}

// match finds the most specific route for the path (relative to the deployment prefix) and method.
func (s *compiledSpec) match(path, method string) matchResult {
	parts := splitSegments(path)

	var res matchResult

	for _, r := range s.routes {
		params, ok := r.matchPath(parts)
		if !ok {
			continue
		}

		if r.allows(method) {
			return matchResult{route: r, params: params}
		}

		for m := range r.methods {
			res.allow = append(res.allow, m)
		}
	}

	slices.Sort(res.allow)

	return res
}

// anyRouteFor returns the best route for a path regardless of method (used for CORS preflight).
func (s *compiledSpec) anyRouteFor(path string) *compiledRoute {
	parts := splitSegments(path)

	for _, r := range s.routes {
		if _, ok := r.matchPath(parts); ok {
			return r
		}
	}

	return nil
}

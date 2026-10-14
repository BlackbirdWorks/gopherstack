// Package restroute matches REST-JSON requests against smithy-style URI templates and writes AWS REST-JSON errors.
package restroute

import (
	"maps"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Route binds a method and URI template ("/cells/{CellName}") to an operation name.
type Route struct {
	Method string
	Path   string
	Op     string
}

type compiled struct {
	method   string
	op       string
	segments []string
}

// Router resolves requests to operations; the zero value matches nothing.
type Router struct {
	routes []compiled
}

// New compiles routes. Every {Label} matches exactly one (URL-decoded) path segment.
func New(routes []Route) *Router {
	r := &Router{routes: make([]compiled, 0, len(routes))}

	for _, rt := range routes {
		r.routes = append(r.routes, compiled{
			method:   rt.Method,
			op:       rt.Op,
			segments: splitPath(rt.Path),
		})
	}

	return r
}

func splitPath(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}

	return strings.Split(p, "/")
}

// Match returns the operation and the captured labels for method and the request's escaped path.
func (r *Router) Match(method, escapedPath string) (string, map[string]string) {
	segs := splitPath(escapedPath)

	for _, rt := range r.routes {
		if rt.method != method || len(rt.segments) != len(segs) {
			continue
		}

		if params, ok := bind(rt.segments, segs); ok {
			return rt.op, params
		}
	}

	return "", nil
}

// Handles reports whether any route (of any method) has the shape of escapedPath.
func (r *Router) Handles(escapedPath string) bool {
	segs := splitPath(escapedPath)

	for _, rt := range r.routes {
		if len(rt.segments) != len(segs) {
			continue
		}

		if _, ok := bind(rt.segments, segs); ok {
			return true
		}
	}

	return false
}

func bind(tmpl, segs []string) (map[string]string, bool) {
	var params map[string]string

	for i, t := range tmpl {
		if len(t) > 2 && t[0] == '{' && t[len(t)-1] == '}' {
			v, err := url.PathUnescape(segs[i])
			if err != nil {
				return nil, false
			}

			if params == nil {
				params = map[string]string{}
			}

			params[t[1:len(t)-1]] = v

			continue
		}

		if t != segs[i] {
			return nil, false
		}
	}

	return params, true
}

// EscapedPath returns the request path with percent-encoding preserved (labels may contain "/").
func EscapedPath(r *http.Request) string {
	return r.URL.EscapedPath()
}

// WriteError writes a REST-JSON error. The body carries both "message" and "Message" because
// legacy and schema-serde SDK clients match the modeled key case-sensitively.
func WriteError(c *echo.Context, status int, code, message string, extra map[string]any) error {
	c.Response().Header().Set("X-Amzn-Errortype", code)

	body := map[string]any{
		"message": awserr.StripCode(code, message),
		"Message": awserr.StripCode(code, message),
	}

	maps.Copy(body, extra)

	return c.JSON(status, body)
}

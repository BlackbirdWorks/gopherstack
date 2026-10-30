package ociapigateway

import (
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

// reqView is the request context that policy and backend expressions read.
type reqView struct {
	params map[string]string
	query  url.Values
	header http.Header
}

var (
	varExprRe = regexp.MustCompile(`^request\.(path|query|headers)\[([^\]]+)\]$`)
	interpoRe = regexp.MustCompile(`\$\{([^}]*)\}`)
)

// parseVariable splits request.path[x], request.query[x] or request.headers[x].
func parseVariable(expr string) (string, string, bool) {
	m := varExprRe.FindStringSubmatch(expr)
	if m == nil {
		return "", "", false
	}

	return m[1], m[2], true
}

func (v *reqView) lookup(expr string) string {
	kind, name, ok := parseVariable(expr)
	if !ok {
		return ""
	}

	switch kind {
	case "path":
		return v.params[name]
	case "query":
		return v.query.Get(name)
	default:
		return v.header.Get(name)
	}
}

// interpolate replaces ${request.*[..]} variables; unknown variables become empty.
func (v *reqView) interpolate(s string, esc func(string) string) string {
	return interpoRe.ReplaceAllStringFunc(s, func(m string) string {
		val := v.lookup(m[2 : len(m)-1])
		if esc != nil {
			return esc(val)
		}

		return val
	})
}

// escapePath escapes a value for a URL path while keeping the slashes of a wildcard parameter.
func escapePath(s string) string {
	parts := strings.Split(s, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}

	return strings.Join(parts, "/")
}

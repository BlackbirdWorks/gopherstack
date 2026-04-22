package apigatewayv2

import (
	"net/http"
	"strings"
)

// EvalRouteSelectionExpression evaluates an AWS API Gateway v2 route-selection
// expression against an HTTP request.
//
// AWS API Gateway v2 uses route-selection expressions to determine which route
// handles an incoming request. The expression supports variable substitution
// using ${<variable>} placeholders.
//
// Supported variables:
//   - ${request.method}  — HTTP method (e.g. "GET")
//   - ${request.path}    — Full request path (e.g. "/items/123")
//   - ${request.header.<name>} — Value of the named request header
//   - ${request.querystring.<name>} — Value of the named query-string parameter
//   - ${request.body.<path>} — Replaced with empty string (body parsing not implemented)
//
// If expr is empty the literal "$default" route key is returned, which is the
// catch-all route AWS uses when no other route matches.
func EvalRouteSelectionExpression(expr string, r *http.Request) string {
	if expr == "" {
		return "$default"
	}

	result := expr

	// Substitute ${request.method}.
	result = strings.ReplaceAll(result, "${request.method}", r.Method)

	// Substitute ${request.path}.
	result = strings.ReplaceAll(result, "${request.path}", r.URL.Path)

	// Substitute ${request.header.<name>}.
	result = substitutePattern(result, "${request.header.", func(name string) string {
		return r.Header.Get(name)
	})

	// Substitute ${request.querystring.<name>}.
	result = substitutePattern(result, "${request.querystring.", func(name string) string {
		return r.URL.Query().Get(name)
	})

	// Substitute ${request.body.<path>} with empty string (not implemented).
	result = substitutePattern(result, "${request.body.", func(_ string) string {
		return ""
	})

	return result
}

// substitutePattern replaces all occurrences of ${<prefix><name>} in s with
// the value returned by resolve(name). Each call to resolve receives the
// extracted name portion (everything between the prefix and the closing "}").
func substitutePattern(s, prefix string, resolve func(string) string) string {
	for {
		start := strings.Index(s, prefix)
		if start < 0 {
			break
		}

		end := strings.Index(s[start:], "}")
		if end < 0 {
			break
		}

		end += start
		name := s[start+len(prefix) : end]
		s = s[:start] + resolve(name) + s[end+1:]
	}

	return s
}

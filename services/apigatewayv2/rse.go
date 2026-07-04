package apigatewayv2

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
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
//   - ${request.body.<path>} — Value of the dot-separated JSON path in the
//     request body (e.g. ${request.body.action} or ${request.body.detail.type});
//     resolves to empty string when the body is absent, not JSON, or the field
//     is missing.
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

	// Substitute ${request.body.<path>} by extracting the dot-separated field
	// from the JSON request body. The body is parsed lazily and restored so the
	// caller can still read it afterwards.
	if strings.Contains(result, "${request.body.") {
		body := parseJSONBody(r)
		result = substitutePattern(result, "${request.body.", func(path string) string {
			return lookupJSONPath(body, path)
		})
	}

	return result
}

// parseJSONBody reads r.Body, restores it for subsequent readers, and returns
// the decoded top-level JSON object. It returns nil if the body is absent or
// is not a JSON object.
func parseJSONBody(r *http.Request) map[string]any {
	if r == nil || r.Body == nil {
		return nil
	}

	raw, err := io.ReadAll(r.Body)
	_ = r.Body.Close()

	// Restore the body so callers can read it again.
	r.Body = io.NopCloser(bytes.NewReader(raw))

	if err != nil || len(raw) == 0 {
		return nil
	}

	var obj map[string]any
	if jsonErr := json.Unmarshal(raw, &obj); jsonErr != nil {
		return nil
	}

	return obj
}

// lookupJSONPath walks a dot-separated path through nested JSON objects and
// returns the string form of the resolved scalar, or "" when the path does not
// resolve to a scalar value.
func lookupJSONPath(obj map[string]any, path string) string {
	if obj == nil || path == "" {
		return ""
	}

	parts := strings.Split(path, ".")

	var cur any = obj

	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}

		cur, ok = m[p]
		if !ok {
			return ""
		}
	}

	switch v := cur.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		}

		return "false"
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return ""
	}
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

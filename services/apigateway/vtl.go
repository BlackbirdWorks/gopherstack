package apigateway

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// VTLContext holds the context variables available during VTL template rendering.
type VTLContext struct {
	// Body is the raw request body passed as $input.body.
	Body string
	// RequestID is exposed as $context.requestId.
	RequestID string
}

var (
	// reInputJSON matches $input.json("$.path") or $input.json('$.path').
	reInputJSON = regexp.MustCompile(`\$input\.json\((?:"([^"]*)"|'([^']*)')\)`)
	// reInputPath matches $input.path("$.path") or $input.path('$.path').
	reInputPath = regexp.MustCompile(`\$input\.path\((?:"([^"]*)"|'([^']*)')\)`)
	// reUtilEscape matches $util.escapeJavaScript("...") or $util.escapeJavaScript('...').
	reUtilEscape = regexp.MustCompile(`\$util\.escapeJavaScript\((?:"([^"]*)"|'([^']*)')\)`)
)

// matchArg extracts the first non-empty captured argument from a regex match
// that uses quote-alternation patterns like (?:"([^"]*)"|'([^']*)'). The full
// match is subs[0]; subs[1] is the double-quoted capture, subs[2] is single-quoted.
func matchArg(subs []string) (string, bool) {
	const expectedGroups = 3 // full match + 2 alternation groups
	if len(subs) < expectedGroups {
		return "", false
	}
	// One of the two groups will be non-empty (the one that matched).
	if subs[1] != "" {
		return subs[1], true
	}

	return subs[2], true
}

// RenderTemplate renders a Velocity Template Language (VTL) template string
// using the provided context.  The following constructs are supported:
//
//   - $input.body                        — the raw request body
//   - $input.json("$.path")              — JSON-path extraction, result JSON-encoded
//   - $input.path("$.path")              — JSON-path extraction, result as plain string
//   - $context.requestId                 — the request identifier
//   - $util.escapeJavaScript("literal")  — JavaScript-escape a string literal
func RenderTemplate(tmpl string, ctx VTLContext) string {
	result := tmpl

	// Replace $input.json("path") — extract from body and JSON-encode the result.
	result = reInputJSON.ReplaceAllStringFunc(result, func(m string) string {
		subs := reInputJSON.FindStringSubmatch(m)
		path, ok := matchArg(subs)
		if !ok {
			return m
		}
		val := extractJSONPath(ctx.Body, path)
		enc, err := json.Marshal(val)
		if err != nil {
			return m
		}

		return string(enc)
	})

	// Replace $input.path("path") — extract from body and return as plain string.
	result = reInputPath.ReplaceAllStringFunc(result, func(m string) string {
		subs := reInputPath.FindStringSubmatch(m)
		path, ok := matchArg(subs)
		if !ok {
			return m
		}
		val := extractJSONPath(ctx.Body, path)

		return jsonValueToString(val)
	})

	// Replace $input.body.
	result = strings.ReplaceAll(result, "$input.body", ctx.Body)

	// Replace $context.requestId.
	result = strings.ReplaceAll(result, "$context.requestId", ctx.RequestID)

	// Replace $util.escapeJavaScript("literal").
	result = reUtilEscape.ReplaceAllStringFunc(result, func(m string) string {
		subs := reUtilEscape.FindStringSubmatch(m)
		arg, ok := matchArg(subs)
		if !ok {
			return m
		}

		return escapeJavaScript(arg)
	})

	return result
}

// extractJSONPath resolves a simple JSONPath expression against a JSON body string.
// Supported syntax: $ (root), .key (member), [n] (array index).
func extractJSONPath(body, path string) any {
	if body == "" {
		return nil
	}

	var root any
	if err := json.Unmarshal([]byte(body), &root); err != nil {
		return nil
	}

	if path == "$" || path == "" {
		return root
	}

	// Strip leading "$".
	path = strings.TrimPrefix(path, "$")

	return walkPath(root, path)
}

// walkPath traverses a parsed JSON value along the given path segments.
func walkPath(val any, path string) any {
	if path == "" {
		return val
	}

	switch {
	case strings.HasPrefix(path, "."):
		// Member access: .key or .key.rest or .key[n]...
		path = path[1:]
		dot := strings.IndexAny(path, ".[")
		var key string
		if dot == -1 {
			key = path
			path = ""
		} else {
			key = path[:dot]
			path = path[dot:]
		}

		m, ok := val.(map[string]any)
		if !ok {
			return nil
		}

		return walkPath(m[key], path)

	case strings.HasPrefix(path, "["):
		end := strings.IndexByte(path, ']')
		if end == -1 {
			return nil
		}
		idxStr := path[1:end]
		path = path[end+1:]
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			return nil
		}

		arr, ok := val.([]any)
		if !ok || idx < 0 || idx >= len(arr) {
			return nil
		}

		return walkPath(arr[idx], path)

	default:
		return nil
	}
}

// jsonValueToString converts a JSON value to its string representation.
func jsonValueToString(val any) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		}

		return "false"
	case float64:
		// Prefer integer representation when the value has no fractional part.
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}

		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		enc, err := json.Marshal(v)
		if err != nil {
			return ""
		}

		return string(enc)
	}
}

// escapeJavaScript escapes a string for safe use inside a JavaScript string literal.
// Mirrors the AWS $util.escapeJavaScript() behaviour.
func escapeJavaScript(s string) string {
	var b strings.Builder
	b.Grow(len(s))

	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\'':
			b.WriteString(`\'`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '/':
			b.WriteString(`\/`)
		default:
			b.WriteRune(r)
		}
	}

	return b.String()
}

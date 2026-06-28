package apigateway

import (
	"encoding/base64"
	"encoding/json"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// VTLContext holds the context variables available during VTL template rendering.
type VTLContext struct {
	// StageVariables is exposed as $stageVariables.X.
	StageVariables map[string]string
	// Body is the raw request body passed as $input.body.
	Body string
	// RequestID is exposed as $context.requestId.
	RequestID string
	// HTTPMethod is exposed as $context.httpMethod.
	HTTPMethod string
	// ResourcePath is exposed as $context.resourcePath (the route template, e.g. /users/{id}).
	ResourcePath string
	// Path is exposed as $context.path (the actual request path).
	Path string
	// Stage is exposed as $context.stage.
	Stage string
	// APIID is exposed as $context.apiId.
	APIID string
	// SourceIP is exposed as $context.identity.sourceIp.
	SourceIP string
	// UserAgent is exposed as $context.identity.userAgent.
	UserAgent string
}

var (
	// reInputJSON matches $input.json("$.path") or $input.json('$.path').
	reInputJSON = regexp.MustCompile(`\$input\.json\((?:"([^"]*)"|'([^']*)')\)`)
	// reInputPath matches $input.path("$.path") or $input.path('$.path').
	reInputPath = regexp.MustCompile(`\$input\.path\((?:"([^"]*)"|'([^']*)')\)`)
	// reUtilEscape matches $util.escapeJavaScript("...") or $util.escapeJavaScript('...').
	reUtilEscape = regexp.MustCompile(`\$util\.escapeJavaScript\((?:"([^"]*)"|'([^']*)')\)`)
	// reUtilURLEncode matches $util.urlEncode("...").
	reUtilURLEncode = regexp.MustCompile(`\$util\.urlEncode\((?:"([^"]*)"|'([^']*)')\)`)
	// reUtilURLDecode matches $util.urlDecode("...").
	reUtilURLDecode = regexp.MustCompile(`\$util\.urlDecode\((?:"([^"]*)"|'([^']*)')\)`)
	// reUtilBase64Encode matches $util.base64Encode("...").
	reUtilBase64Encode = regexp.MustCompile(`\$util\.base64Encode\((?:"([^"]*)"|'([^']*)')\)`)
	// reUtilBase64Decode matches $util.base64Decode("...").
	reUtilBase64Decode = regexp.MustCompile(`\$util\.base64Decode\((?:"([^"]*)"|'([^']*)')\)`)
	// reStageVar matches $stageVariables.X.
	reStageVar = regexp.MustCompile(`\$stageVariables\.([A-Za-z0-9_]+)`)
)

// minStageVarSubmatches is the minimum number of submatches expected from reStageVar
// (full match + 1 capture group).
const minStageVarSubmatches = 2

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
// using the provided context.
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

	// $context.* variables.
	result = strings.ReplaceAll(result, "$context.requestId", ctx.RequestID)
	result = strings.ReplaceAll(result, "$context.httpMethod", ctx.HTTPMethod)
	result = strings.ReplaceAll(result, "$context.resourcePath", ctx.ResourcePath)
	result = strings.ReplaceAll(result, "$context.path", ctx.Path)
	result = strings.ReplaceAll(result, "$context.stage", ctx.Stage)
	result = strings.ReplaceAll(result, "$context.apiId", ctx.APIID)
	result = strings.ReplaceAll(result, "$context.identity.sourceIp", ctx.SourceIP)
	result = strings.ReplaceAll(result, "$context.identity.userAgent", ctx.UserAgent)

	// $stageVariables.X.
	result = reStageVar.ReplaceAllStringFunc(result, func(m string) string {
		subs := reStageVar.FindStringSubmatch(m)
		if len(subs) < minStageVarSubmatches {
			return m
		}
		if ctx.StageVariables == nil {
			return ""
		}

		return ctx.StageVariables[subs[1]]
	})

	// $util functions on string literals.
	result = applyUtilFunctions(result)

	return result
}

// applyUtilFunctions applies $util.* literal substitutions to the template result.
func applyUtilFunctions(result string) string {
	result = reUtilEscape.ReplaceAllStringFunc(result, func(m string) string {
		subs := reUtilEscape.FindStringSubmatch(m)
		arg, ok := matchArg(subs)
		if !ok {
			return m
		}

		return escapeJavaScript(arg)
	})

	result = reUtilURLEncode.ReplaceAllStringFunc(result, func(m string) string {
		subs := reUtilURLEncode.FindStringSubmatch(m)
		arg, ok := matchArg(subs)
		if !ok {
			return m
		}

		return url.QueryEscape(arg)
	})

	result = reUtilURLDecode.ReplaceAllStringFunc(result, func(m string) string {
		subs := reUtilURLDecode.FindStringSubmatch(m)
		arg, ok := matchArg(subs)
		if !ok {
			return m
		}
		decoded, err := url.QueryUnescape(arg)
		if err != nil {
			return m
		}

		return decoded
	})

	result = reUtilBase64Encode.ReplaceAllStringFunc(result, func(m string) string {
		subs := reUtilBase64Encode.FindStringSubmatch(m)
		arg, ok := matchArg(subs)
		if !ok {
			return m
		}

		return base64.StdEncoding.EncodeToString([]byte(arg))
	})

	result = reUtilBase64Decode.ReplaceAllStringFunc(result, func(m string) string {
		subs := reUtilBase64Decode.FindStringSubmatch(m)
		arg, ok := matchArg(subs)
		if !ok {
			return m
		}
		decoded, err := base64.StdEncoding.DecodeString(arg)
		if err != nil {
			return m
		}

		return string(decoded)
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

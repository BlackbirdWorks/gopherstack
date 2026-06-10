package appsync

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// AppSync APPSYNC_JS evaluation.
//
// AWS's EvaluateCode runs the exported `request` or `response` handler from the
// supplied APPSYNC_JS module against the provided context object and returns the
// JSON-stringified return value as `evaluationResult`. A full implementation
// requires a JavaScript engine; gopherstack does not vendor one (no goja/otto/v8
// in go.mod, and adding a heavy JS runtime is out of scope for the parity work).
//
// Instead this implements a faithful evaluator for the documented APPSYNC_JS
// patterns that resolvers and functions actually use in practice:
//
//   - Selecting the handler to run (`function` arg, else `request`, else `response`).
//   - `return <object literal>;` where the object's values are JSON literals or
//     context member expressions (ctx.arguments.x, ctx.args.x, ctx.result,
//     ctx.prev.result, ctx.identity, ctx.source, ctx.stash).
//   - `return <context member expression>;` (e.g. `return ctx.result;`).
//   - `return <json literal>;` (object, array, string, number, bool, null).
//   - `return util.<helper>(...)` for the common pure helpers: util.toJson,
//     util.parseJson, util.error, util.appendError, util.unauthorized.
//
// Anything outside this set (control flow, arbitrary JS expressions, loops,
// variable bindings, string concatenation, etc.) returns ErrUnsupportedJSCode
// rather than a fabricated empty result, so callers can tell the difference
// between "evaluated" and "not supported".

const (
	jsHandlerRequest  = "request"
	jsHandlerResponse = "response"
)

var (
	// reExportedFunc matches `export function <name>(<args>) { <body> }` and the
	// `function <name>(...)` form. Body capture is greedy to the matching final brace
	// of the module-level function (handled by brace-balancing in extractFuncBody).
	reExportedFunc = regexp.MustCompile(`(?s)(?:export\s+)?function\s+(request|response)\s*\(([^)]*)\)\s*\{`)
	// reReturnStmt matches a `return <expr>;` statement (expr may be empty for `return;`).
	reReturnStmt = regexp.MustCompile(`(?s)\breturn\b\s*(.*?);`)
	// reCtxParamName captures the first parameter name of the handler (the context binding).
	reCtxParamName = regexp.MustCompile(`^\s*(\w+)`)
)

// jsContext mirrors the AppSync request/response context object that the handler
// receives. Unknown JSON keys are preserved in Raw for member resolution.
type jsContext struct {
	Raw       map[string]any
	Arguments map[string]any
	Args      map[string]any
	Result    any
	Prev      map[string]any
	Identity  any
	Source    any
	Stash     map[string]any
}

// evaluateAppSyncJS evaluates an APPSYNC_JS module against the supplied context
// JSON and returns the JSON-stringified handler return value.
func evaluateAppSyncJS(code, contextJSON, function string) (string, error) {
	jc, err := parseJSContext(contextJSON)
	if err != nil {
		return "", err
	}

	body, paramName, err := selectHandlerBody(code, function)
	if err != nil {
		return "", err
	}

	retExpr, ok := lastReturnExpr(body)
	if !ok || strings.TrimSpace(retExpr) == "" {
		// A handler with no return (or a bare `return;`) yields JS undefined, which
		// AppSync serializes as null.
		return "null", nil
	}

	val, err := evalJSExpr(retExpr, paramName, jc)
	if err != nil {
		return "", err
	}

	out, mErr := json.Marshal(val)
	if mErr != nil {
		return "", fmt.Errorf("%w: result is not JSON-serializable", ErrValidation)
	}

	return string(out), nil
}

// parseJSContext decodes the context JSON into a jsContext. An empty context is valid.
func parseJSContext(contextJSON string) (*jsContext, error) {
	jc := &jsContext{Raw: map[string]any{}}

	if strings.TrimSpace(contextJSON) == "" {
		return jc, nil
	}

	if err := json.Unmarshal([]byte(contextJSON), &jc.Raw); err != nil {
		return nil, fmt.Errorf("%w: invalid context JSON", ErrValidation)
	}

	jc.Arguments = mapField(jc.Raw, "arguments")
	jc.Args = mapField(jc.Raw, "args")
	if jc.Arguments == nil {
		jc.Arguments = jc.Args
	}
	if jc.Args == nil {
		jc.Args = jc.Arguments
	}
	jc.Result = jc.Raw["result"]
	jc.Prev = mapField(jc.Raw, "prev")
	jc.Identity = jc.Raw["identity"]
	jc.Source = jc.Raw["source"]
	jc.Stash = mapField(jc.Raw, "stash")

	return jc, nil
}

func mapField(m map[string]any, key string) map[string]any {
	if v, ok := m[key].(map[string]any); ok {
		return v
	}

	return nil
}

// selectHandlerBody returns the body and context-parameter name of the handler to
// run. AWS uses the `function` field when set (request/response), else defaults to
// `request`, falling back to `response` when only that is defined.
func selectHandlerBody(code, function string) (string, string, error) {
	type handler struct {
		body  string
		param string
	}
	handlers := map[string]handler{}

	idx := reExportedFunc.FindAllStringSubmatchIndex(code, -1)
	for _, m := range idx {
		name := code[m[2]:m[3]]
		params := code[m[4]:m[5]]
		openBrace := m[1] - 1
		fnBody, ok := extractBracedBody(code, openBrace)
		if !ok {
			continue
		}

		param := ""
		if pm := reCtxParamName.FindStringSubmatch(params); pm != nil {
			param = pm[1]
		}

		handlers[name] = handler{body: fnBody, param: param}
	}

	var order []string
	switch function {
	case jsHandlerRequest, jsHandlerResponse:
		order = []string{function}
	default:
		order = []string{jsHandlerRequest, jsHandlerResponse}
	}

	for _, name := range order {
		if h, ok := handlers[name]; ok {
			return h.body, h.param, nil
		}
	}

	return "", "", fmt.Errorf("%w: no request/response handler found in code", ErrUnsupportedJSCode)
}

// extractBracedBody returns the contents between the brace at openIdx and its
// matching close brace, respecting string/char literals and nested braces.
func extractBracedBody(s string, openIdx int) (string, bool) {
	if openIdx < 0 || openIdx >= len(s) || s[openIdx] != '{' {
		return "", false
	}

	depth := 0
	inStr := byte(0)
	escaped := false

	for i := openIdx; i < len(s); i++ {
		ch := s[i]

		if inStr != 0 {
			switch {
			case escaped:
				escaped = false
			case ch == '\\':
				escaped = true
			case ch == inStr:
				inStr = 0
			}

			continue
		}

		switch ch {
		case '"', '\'', '`':
			inStr = ch
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return s[openIdx+1 : i], true
			}
		}
	}

	return "", false
}

// lastReturnExpr returns the expression of the last top-level `return` statement
// in the body. AppSync handlers are straight-line; we take the final return.
func lastReturnExpr(body string) (string, bool) {
	matches := reReturnStmt.FindAllStringSubmatch(body, -1)
	if len(matches) == 0 {
		return "", false
	}

	expr := strings.TrimSpace(matches[len(matches)-1][1])

	return expr, true
}

// evalJSExpr evaluates a single APPSYNC_JS return expression against the context.
// The expression is assumed non-empty; an empty `return;` is handled by the caller
// (it evaluates to JS undefined, serialized as null).
func evalJSExpr(expr, ctxParam string, jc *jsContext) (any, error) {
	expr = strings.TrimSpace(expr)

	// JSON literal (object, array, string, number, bool, null).
	if v, ok := tryParseJSONLiteral(expr); ok {
		return v, nil
	}

	// util.<helper>(...) calls.
	if strings.HasPrefix(expr, "util.") {
		return evalUtilCall(expr, ctxParam, jc)
	}

	// Context member access (ctx.result, ctx.arguments.x, ...).
	if ctxParam != "" && (expr == ctxParam || strings.HasPrefix(expr, ctxParam+".")) {
		return resolveContextPath(expr, ctxParam, jc), nil
	}

	// Object literal whose values are themselves resolvable expressions.
	if strings.HasPrefix(expr, "{") && strings.HasSuffix(expr, "}") {
		return evalObjectLiteral(expr, ctxParam, jc)
	}

	return nil, fmt.Errorf("%w: cannot evaluate expression %q", ErrUnsupportedJSCode, expr)
}

// tryParseJSONLiteral attempts to parse the expression as a JSON value. JS object
// keys are often unquoted, so this only succeeds for strict-JSON literals.
func tryParseJSONLiteral(expr string) (any, bool) {
	switch expr {
	case "null", "undefined":
		return nil, true
	case "true":
		return true, true
	case "false":
		return false, true
	}

	if n, err := strconv.ParseFloat(expr, 64); err == nil {
		return n, true
	}

	var v any
	if err := json.Unmarshal([]byte(expr), &v); err == nil {
		return v, true
	}

	return nil, false
}

// evalObjectLiteral evaluates a `{ key: <expr>, ... }` literal where values may be
// JSON literals or context member expressions. Keys may be quoted or bare.
func evalObjectLiteral(expr, ctxParam string, jc *jsContext) (any, error) {
	inner := strings.TrimSpace(expr[1 : len(expr)-1])
	result := map[string]any{}
	if inner == "" {
		return result, nil
	}

	pairs, err := splitTopLevel(inner, ',')
	if err != nil {
		return nil, err
	}

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		kv, kvErr := splitTopLevel(pair, ':')
		if kvErr != nil || len(kv) != 2 {
			return nil, fmt.Errorf("%w: malformed object entry %q", ErrUnsupportedJSCode, pair)
		}

		key := strings.Trim(strings.TrimSpace(kv[0]), `"'`)
		val, vErr := evalJSExpr(strings.TrimSpace(kv[1]), ctxParam, jc)
		if vErr != nil {
			return nil, vErr
		}

		result[key] = val
	}

	return result, nil
}

// resolveContextPath resolves a dotted path rooted at the context parameter,
// e.g. ctx.arguments.id, ctx.result, ctx.prev.result, ctx.stash.key. A path that
// does not resolve yields nil (JS undefined); this is never an error.
func resolveContextPath(expr, ctxParam string, jc *jsContext) any {
	path := strings.TrimPrefix(expr, ctxParam)
	path = strings.TrimPrefix(path, ".")
	if path == "" {
		return jc.Raw
	}

	segments := strings.Split(path, ".")
	// Normalize the well-known top-level aliases.
	var cur any
	switch segments[0] {
	case "arguments", "args":
		cur = jc.Arguments
	case "result":
		cur = jc.Result
	case "prev":
		cur = jc.Prev
	case "identity":
		cur = jc.Identity
	case "source":
		cur = jc.Source
	case "stash":
		cur = jc.Stash
	default:
		if jc.Raw == nil {
			return nil
		}
		cur = jc.Raw[segments[0]]
	}

	for _, seg := range segments[1:] {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[seg]
	}

	return cur
}

// evalUtilCall evaluates the supported pure `util.*` helpers.
func evalUtilCall(expr, ctxParam string, jc *jsContext) (any, error) {
	open := strings.IndexByte(expr, '(')
	if open < 0 || !strings.HasSuffix(expr, ")") {
		return nil, fmt.Errorf("%w: malformed util call %q", ErrUnsupportedJSCode, expr)
	}

	fn := strings.TrimSpace(expr[:open])
	argStr := strings.TrimSpace(expr[open+1 : len(expr)-1])

	switch fn {
	case "util.toJson":
		v, err := evalJSExpr(argStr, ctxParam, jc)
		if err != nil {
			return nil, err
		}
		b, mErr := json.Marshal(v)
		if mErr != nil {
			return nil, fmt.Errorf("%w: util.toJson argument not serializable", ErrValidation)
		}

		return string(b), nil

	case "util.parseJson":
		v, err := evalJSExpr(argStr, ctxParam, jc)
		if err != nil {
			return nil, err
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%w: util.parseJson requires a string", ErrValidation)
		}
		var parsed any
		if uErr := json.Unmarshal([]byte(s), &parsed); uErr != nil {
			return nil, fmt.Errorf("%w: util.parseJson invalid JSON", ErrValidation)
		}

		return parsed, nil

	case "util.error", "util.appendError":
		// These raise an error in AppSync; surface it as a validation error.
		return nil, fmt.Errorf("%w: %s", ErrValidation, strings.Trim(argStr, `"'`))

	case "util.unauthorized":
		return nil, fmt.Errorf("%w: unauthorized", ErrValidation)
	}

	return nil, fmt.Errorf("%w: unsupported helper %q", ErrUnsupportedJSCode, fn)
}

// splitTopLevel splits s on sep, ignoring separators inside brackets, braces,
// parens, and string literals.
func splitTopLevel(s string, sep byte) ([]string, error) {
	var (
		parts   []string
		buf     strings.Builder
		depth   int
		inStr   byte
		escaped bool
	)

	for i := range len(s) {
		ch := s[i]

		if inStr != 0 {
			buf.WriteByte(ch)
			switch {
			case escaped:
				escaped = false
			case ch == '\\':
				escaped = true
			case ch == inStr:
				inStr = 0
			}

			continue
		}

		switch ch {
		case '"', '\'', '`':
			inStr = ch
			buf.WriteByte(ch)
		case '{', '[', '(':
			depth++
			buf.WriteByte(ch)
		case '}', ']', ')':
			depth--
			buf.WriteByte(ch)
		case sep:
			if depth == 0 {
				parts = append(parts, buf.String())
				buf.Reset()
			} else {
				buf.WriteByte(ch)
			}
		default:
			buf.WriteByte(ch)
		}
	}

	if depth != 0 || inStr != 0 {
		return nil, fmt.Errorf("%w: unbalanced expression", ErrUnsupportedJSCode)
	}

	parts = append(parts, buf.String())

	return parts, nil
}

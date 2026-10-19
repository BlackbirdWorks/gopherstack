package appsync

import (
	"encoding/json"
	"fmt"
	"strings"
)

// evalContext decodes an EvaluateMappingTemplate/EvaluateCode context JSON
// into a full AppSync context map (args/arguments aliased, stash present).
func evalContext(contextJSON string) (map[string]any, error) {
	ctx := map[string]any{}

	if strings.TrimSpace(contextJSON) != "" {
		if err := json.Unmarshal([]byte(contextJSON), &ctx); err != nil {
			return nil, fmt.Errorf("%w: invalid context JSON", ErrValidation)
		}
	}

	args, hasArgs := ctx[jsCtxKeyArguments]
	alias, hasAlias := ctx["args"]

	switch {
	case hasArgs && !hasAlias:
		ctx["args"] = args
	case hasAlias && !hasArgs:
		ctx[jsCtxKeyArguments] = alias
	case !hasArgs:
		empty := map[string]any{}
		ctx[jsCtxKeyArguments], ctx["args"] = empty, empty
	}

	if _, ok := ctx["stash"].(map[string]any); !ok {
		ctx["stash"] = map[string]any{}
	}

	for _, k := range []string{jsCtxKeyPrev, "info", "request"} {
		if _, ok := ctx[k].(map[string]any); !ok {
			ctx[k] = map[string]any{}
		}
	}

	return ctx, nil
}

// EvaluateMappingTemplate renders a VTL template against the context JSON.
// A #return value or $util.error surfaces as in a live resolver.
func (b *InMemoryBackend) EvaluateMappingTemplate(template, contextJSON string) (string, error) {
	ctx, err := evalContext(contextJSON)
	if err != nil {
		return "", err
	}

	cc := &callCtx{}

	out, err := runVTL(template, ctx, cc.utilEnv())
	if err != nil {
		return "", err
	}

	if out.Returned {
		return returnedString(out.Value), nil
	}

	return out.Output, nil
}

// EvaluateCode runs an APPSYNC_JS module's request or response handler against
// the context JSON and returns the JSON-stringified return value.
func (b *InMemoryBackend) EvaluateCode(code, contextJSON, function, runtime string) (string, error) {
	if code == "" {
		return "", fmt.Errorf("%w: code is required", ErrValidation)
	}

	if runtime != "" && runtime != "APPSYNC_JS" {
		return "", fmt.Errorf("%w: unsupported runtime %q", ErrValidation, runtime)
	}

	if function == "" {
		function = jsHandlerRequest
	}

	if function != jsHandlerRequest && function != jsHandlerResponse {
		return "", fmt.Errorf("%w: function must be 'request' or 'response'", ErrValidation)
	}

	ctx, err := evalContext(contextJSON)
	if err != nil {
		return "", err
	}

	cc := &callCtx{}

	out, err := runJS(code, function, ctx, cc.utilEnv())
	if err != nil {
		if handlerMissing(err) {
			return "", fmt.Errorf("%w: no %s handler found in code", ErrValidation, function)
		}

		return "", err
	}

	raw, err := json.Marshal(out.Value)
	if err != nil {
		return "", fmt.Errorf("%w: result is not JSON-serializable", ErrValidation)
	}

	return string(raw), nil
}

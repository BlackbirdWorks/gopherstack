package appsync

import (
	"encoding/json"
	"fmt"
)

// EvaluateMappingTemplate evaluates a VTL mapping template with the provided context JSON.
// The context is expected to be a JSON object with optional "arguments" and "result" keys.
func (b *InMemoryBackend) EvaluateMappingTemplate(template, contextJSON string) (string, error) {
	var ctx struct {
		Arguments map[string]any `json:"arguments"`
		Result    any            `json:"result"`
	}

	if contextJSON != "" {
		if err := json.Unmarshal([]byte(contextJSON), &ctx); err != nil {
			return "", fmt.Errorf("%w: invalid context JSON", ErrValidation)
		}
	}

	out, err := renderVTL(template, ctx.Arguments, ctx.Result)
	if err != nil {
		return "", err
	}

	return out, nil
}

// EvaluateCode evaluates an APPSYNC_JS module against the supplied context and
// returns the JSON-stringified return value of the selected handler.
//
// gopherstack does not embed a JavaScript engine, so this evaluates the documented
// APPSYNC_JS patterns directly (see appsync_js.go). Constructs beyond that set
// return ErrUnsupportedJSCode rather than a fabricated result, so callers can
// distinguish "evaluated" from "not supported by the emulator".
func (b *InMemoryBackend) EvaluateCode(code, contextJSON, function, runtime string) (string, error) {
	if code == "" {
		return "", fmt.Errorf("%w: code is required", ErrValidation)
	}

	if runtime != "" && runtime != "APPSYNC_JS" {
		return "", fmt.Errorf("%w: unsupported runtime %q", ErrValidation, runtime)
	}

	if function != "" && function != jsHandlerRequest && function != jsHandlerResponse {
		return "", fmt.Errorf("%w: function must be 'request' or 'response'", ErrValidation)
	}

	return evaluateAppSyncJS(code, contextJSON, function)
}

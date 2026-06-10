package appsync_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// twoHandlerCode defines both request and response handlers, used to exercise
// handler selection.
const twoHandlerCode = `export function request(ctx) { return ctx.arguments; } ` +
	`export function response(ctx) { return ctx.result; }`

func TestInMemoryBackend_EvaluateCode_Eval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		code     string
		context  string
		function string
		runtime  string
		want     string // expected evaluationResult (JSON-stringified)
		wantErr  bool
	}{
		{
			name: "empty_object_request",
			code: `export function request(ctx) { return {}; }`,
			want: `{}`,
		},
		{
			name: "json_object_literal",
			code: `export function request(ctx) { return {"operation":"GetItem"}; }`,
			want: `{"operation":"GetItem"}`,
		},
		{
			name:    "return_context_argument",
			code:    `export function request(ctx) { return ctx.arguments.id; }`,
			context: `{"arguments":{"id":"abc-123"}}`,
			want:    `"abc-123"`,
		},
		{
			name:    "return_context_args_alias",
			code:    `export function request(ctx) { return ctx.args.count; }`,
			context: `{"args":{"count":42}}`,
			want:    `42`,
		},
		{
			name:     "response_returns_result",
			code:     `export function response(ctx) { return ctx.result; }`,
			context:  `{"result":{"name":"widget"}}`,
			function: "response",
			want:     `{"name":"widget"}`,
		},
		{
			name:    "object_with_context_values",
			code:    `export function request(ctx) { return {"id": ctx.arguments.id, "static": "v"}; }`,
			context: `{"arguments":{"id":"x1"}}`,
			want:    `{"id":"x1","static":"v"}`,
		},
		{
			name:    "util_toJson",
			code:    `export function request(ctx) { return util.toJson(ctx.arguments); }`,
			context: `{"arguments":{"a":1}}`,
			want:    `"{\"a\":1}"`,
		},
		{
			name: "util_parseJson",
			code: `export function request(ctx) { return util.parseJson("{\"k\":true}"); }`,
			want: `{"k":true}`,
		},
		{
			name:    "return_prev_result",
			code:    `export function request(ctx) { return ctx.prev.result; }`,
			context: `{"prev":{"result":{"step":1}}}`,
			want:    `{"step":1}`,
		},
		{
			name: "no_return_yields_null",
			code: `export function request(ctx) { const x = 1; }`,
			want: `null`,
		},
		{
			name:     "function_selects_response",
			code:     twoHandlerCode,
			context:  `{"arguments":{"a":1},"result":"final"}`,
			function: "response",
			want:     `"final"`,
		},
		{
			name:    "default_selects_request",
			code:    twoHandlerCode,
			context: `{"arguments":{"a":1},"result":"final"}`,
			want:    `{"a":1}`,
		},
		{
			name:    "util_error_is_error",
			code:    `export function request(ctx) { return util.error("boom"); }`,
			wantErr: true,
		},
		{
			name:    "empty_code_errors",
			code:    "",
			wantErr: true,
		},
		{
			name:    "invalid_context_json_errors",
			code:    `export function request(ctx) { return {}; }`,
			context: `{not json`,
			wantErr: true,
		},
		{
			name:    "unsupported_runtime_errors",
			code:    `export function request(ctx) { return {}; }`,
			runtime: "VTL",
			wantErr: true,
		},
		{
			name:     "invalid_function_name_errors",
			code:     `export function request(ctx) { return {}; }`,
			function: "bogus",
			wantErr:  true,
		},
		{
			name:    "no_handler_errors",
			code:    `const x = 1;`,
			wantErr: true,
		},
		{
			name:    "unsupported_expression_errors",
			code:    `export function request(ctx) { return someUndefinedVar + 1; }`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			out, err := b.EvaluateCode(tt.code, tt.context, tt.function, tt.runtime)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			// Compare as canonical JSON to be robust to key ordering.
			assert.JSONEq(t, tt.want, out)
		})
	}
}

// TestInMemoryBackend_EvaluateCode_ResultIsValidJSON ensures the evaluationResult
// is always a valid JSON document on success.
func TestInMemoryBackend_EvaluateCode_ResultIsValidJSON(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.EvaluateCode(
		`export function request(ctx) { return {"version":"2018-05-29","operation":"Scan"}; }`,
		"", "", "APPSYNC_JS",
	)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &decoded))
	assert.Equal(t, "Scan", decoded["operation"])
}

package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestRenderVTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tmpl   string
		args   map[string]any
		result any
		want   string
	}{
		{
			name:   "empty_template_with_nil_result",
			tmpl:   "",
			args:   nil,
			result: nil,
			want:   "{}",
		},
		{
			name:   "empty_template_with_result",
			tmpl:   "",
			args:   nil,
			result: map[string]any{"id": "123"},
			want:   `{"id":"123"}`,
		},
		{
			name:   "substitutes_context_arguments",
			tmpl:   `{"id": "$context.arguments.id"}`,
			args:   map[string]any{"id": "abc"},
			result: nil,
			want:   `{"id": "abc"}`,
		},
		{
			name:   "substitutes_ctx_args",
			tmpl:   `{"key": "$ctx.args.key"}`,
			args:   map[string]any{"key": "val"},
			result: nil,
			want:   `{"key": "val"}`,
		},
		{
			name:   "substitutes_context_result",
			tmpl:   "$util.toJson($context.result)",
			args:   nil,
			result: map[string]any{"name": "test"},
			want:   `{"name":"test"}`,
		},
		{
			name:   "substitutes_context_result_field",
			tmpl:   "$context.result.name",
			args:   nil,
			result: map[string]any{"name": "hello"},
			want:   "hello",
		},
		{
			name:   "handles_return_directive",
			tmpl:   "#return($context.result)",
			args:   nil,
			result: map[string]any{"id": "1"},
			want:   `{"id":"1"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := appsync.RenderVTL(tt.tmpl, tt.args, tt.result)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRenderVTL_Extended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tmpl   string
		args   map[string]any
		result any
		want   string
	}{
		{
			name:   "bare_context_arguments",
			tmpl:   "$context.arguments",
			args:   map[string]any{"key": "val"},
			result: nil,
			want:   `{"key":"val"}`,
		},
		{
			name:   "bare_ctx_args",
			tmpl:   "$ctx.args",
			args:   map[string]any{"x": 1.0},
			result: nil,
			want:   `{"x":1}`,
		},
		{
			name:   "util_to_json_bare_result",
			tmpl:   "$util.toJson($context.result)",
			args:   nil,
			result: "hello",
			want:   `"hello"`,
		},
		{
			name:   "dynamodb_to_json_string",
			tmpl:   `$util.dynamodb.toDynamoDBJson($ctx.args.id)`,
			args:   map[string]any{"id": "abc"},
			result: nil,
			want:   `{"S":"abc"}`,
		},
		{
			name:   "result_field_not_found_returns_null",
			tmpl:   "$context.result.missing",
			args:   nil,
			result: map[string]any{"other": "val"},
			want:   "null",
		},
		{
			name:   "nil_result_returns_null",
			tmpl:   "$context.result",
			args:   nil,
			result: nil,
			want:   "null",
		},
		{
			name:   "missing_arg_returns_null",
			tmpl:   "$ctx.args.missing",
			args:   map[string]any{"other": "val"},
			result: nil,
			want:   "null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := appsync.RenderVTL(tt.tmpl, tt.args, tt.result)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestToDynamoDBJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input any
		want  string
	}{
		{name: "string", input: "hello", want: `{"S":"hello"}`},
		{name: "float64", input: float64(42), want: `{"N":"42"}`},
		{name: "bool_true", input: true, want: `{"BOOL":true}`},
		{name: "bool_false", input: false, want: `{"BOOL":false}`},
		{name: "nil", input: nil, want: `{"NULL":true}`},
		{name: "map_passes_through", input: map[string]any{"x": 1}, want: `{"x":1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := appsync.ToDynamoDBJSON(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRenderVTL_ResolveExpr_AllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tmpl   string
		args   map[string]any
		result any
		want   string
	}{
		{
			name:   "resolve_context_arguments_key",
			tmpl:   "$util.toJson($context.arguments.id)",
			args:   map[string]any{"id": "test"},
			result: nil,
			want:   `"test"`,
		},
		{
			name:   "resolve_context_arguments_bare",
			tmpl:   "$util.toJson($context.arguments)",
			args:   map[string]any{"x": "y"},
			result: nil,
			want:   `{"x":"y"}`,
		},
		{
			name:   "resolve_ctx_args_key",
			tmpl:   "$util.toJson($ctx.args.name)",
			args:   map[string]any{"name": "alice"},
			result: nil,
			want:   `"alice"`,
		},
		{
			name:   "resolve_ctx_args_bare",
			tmpl:   "$util.toJson($ctx.args)",
			args:   map[string]any{"a": "b"},
			result: nil,
			want:   `{"a":"b"}`,
		},
		{
			name:   "resolve_context_result_bare",
			tmpl:   "$util.toJson($context.result)",
			args:   nil,
			result: map[string]any{"id": "1"},
			want:   `{"id":"1"}`,
		},
		{
			name:   "unknown_expr_passthrough",
			tmpl:   "$util.toJson(something_unknown)",
			args:   nil,
			result: nil,
			want:   `"something_unknown"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := appsync.RenderVTL(tt.tmpl, tt.args, tt.result)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

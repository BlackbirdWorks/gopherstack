package apigateway_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestRenderTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tmpl         string
		ctx          apigateway.VTLContext
		wantEqual    string
		wantJSONEq   string
		wantContains []string
		wantEmpty    bool
	}{
		// $input.body
		{
			name:       "input_body",
			tmpl:       `{"body":$input.body}`,
			ctx:        apigateway.VTLContext{Body: `{"hello":"world"}`},
			wantJSONEq: `{"body":{"hello":"world"}}`,
		},
		{
			name:      "input_body_empty",
			tmpl:      `$input.body`,
			ctx:       apigateway.VTLContext{},
			wantEmpty: true,
		},
		// $context
		{
			name:       "context_request_id",
			tmpl:       `{"id":"$context.requestId"}`,
			ctx:        apigateway.VTLContext{RequestID: "req-123"},
			wantJSONEq: `{"id":"req-123"}`,
		},
		// $input.json
		{
			name:         "input_json_root",
			tmpl:         `$input.json('$')`,
			ctx:          apigateway.VTLContext{Body: `{"name":"Alice","age":30}`},
			wantContains: []string{`"name"`, `"Alice"`},
		},
		{
			name:       "input_json_string_field",
			tmpl:       `{"name":$input.json('$.name')}`,
			ctx:        apigateway.VTLContext{Body: `{"name":"Alice","age":30}`},
			wantJSONEq: `{"name":"Alice"}`,
		},
		{
			name:      "input_json_numeric_field",
			tmpl:      `$input.json('$.count')`,
			ctx:       apigateway.VTLContext{Body: `{"count":42}`},
			wantEqual: `42`,
		},
		{
			name:      "input_json_array_index",
			tmpl:      `$input.json('$.items[1]')`,
			ctx:       apigateway.VTLContext{Body: `{"items":["a","b","c"]}`},
			wantEqual: `"b"`,
		},
		{
			name:      "input_json_double_quote_delimiter",
			tmpl:      `$input.json("$.key")`,
			ctx:       apigateway.VTLContext{Body: `{"key":"val"}`},
			wantEqual: `"val"`,
		},
		// $input.path
		{
			name:      "input_path_string_field",
			tmpl:      `Hello $input.path('$.username')`,
			ctx:       apigateway.VTLContext{Body: `{"username":"bob"}`},
			wantEqual: `Hello bob`,
		},
		{
			name:      "input_path_bool_field",
			tmpl:      `$input.path('$.active')`,
			ctx:       apigateway.VTLContext{Body: `{"active":true}`},
			wantEqual: `true`,
		},
		{
			name:      "input_path_numeric_field",
			tmpl:      `$input.path('$.count')`,
			ctx:       apigateway.VTLContext{Body: `{"count":7}`},
			wantEqual: `7`,
		},
		{
			name:      "input_path_nested_field",
			tmpl:      `$input.path('$.user.name')`,
			ctx:       apigateway.VTLContext{Body: `{"user":{"name":"carol"}}`},
			wantEqual: `carol`,
		},
		{
			name:      "input_path_missing_field",
			tmpl:      `$input.path('$.nonexistent')`,
			ctx:       apigateway.VTLContext{Body: `{"a":1}`},
			wantEmpty: true,
		},
		{
			name:      "input_path_invalid_body",
			tmpl:      `$input.path('$.key')`,
			ctx:       apigateway.VTLContext{Body: `not-json`},
			wantEmpty: true,
		},
		// $util.escapeJavaScript
		{
			name:      "util_escape_javascript_double_quotes",
			tmpl:      `$util.escapeJavaScript('hello "world"')`,
			ctx:       apigateway.VTLContext{},
			wantEqual: `hello \"world\"`,
		},
		{
			name:      "util_escape_javascript_slash",
			tmpl:      `$util.escapeJavaScript('a/b')`,
			ctx:       apigateway.VTLContext{},
			wantEqual: `a\/b`,
		},
		{
			name:      "util_escape_javascript_newline",
			tmpl:      "$util.escapeJavaScript('line1\nline2')",
			ctx:       apigateway.VTLContext{},
			wantEqual: `line1\nline2`,
		},
		// $context.* additional variables
		{
			name:      "context_http_method",
			tmpl:      `$context.httpMethod`,
			ctx:       apigateway.VTLContext{HTTPMethod: "POST"},
			wantEqual: "POST",
		},
		{
			name:      "context_resource_path",
			tmpl:      `$context.resourcePath`,
			ctx:       apigateway.VTLContext{ResourcePath: "/users/{id}"},
			wantEqual: "/users/{id}",
		},
		{
			name:      "context_path",
			tmpl:      `$context.path`,
			ctx:       apigateway.VTLContext{Path: "/users/42"},
			wantEqual: "/users/42",
		},
		{
			name:      "context_stage",
			tmpl:      `$context.stage`,
			ctx:       apigateway.VTLContext{Stage: "prod"},
			wantEqual: "prod",
		},
		{
			name:      "context_api_id",
			tmpl:      `$context.apiId`,
			ctx:       apigateway.VTLContext{APIID: "abc123"},
			wantEqual: "abc123",
		},
		{
			name:      "context_source_ip",
			tmpl:      `$context.identity.sourceIp`,
			ctx:       apigateway.VTLContext{SourceIP: "1.2.3.4"},
			wantEqual: "1.2.3.4",
		},
		{
			name:      "context_user_agent",
			tmpl:      `$context.identity.userAgent`,
			ctx:       apigateway.VTLContext{UserAgent: "curl/7.88"},
			wantEqual: "curl/7.88",
		},
		// $stageVariables
		{
			name:      "stage_variable_present",
			tmpl:      `$stageVariables.myTable`,
			ctx:       apigateway.VTLContext{StageVariables: map[string]string{"myTable": "users_prod"}},
			wantEqual: "users_prod",
		},
		{
			name:      "stage_variable_missing",
			tmpl:      `$stageVariables.noSuchVar`,
			ctx:       apigateway.VTLContext{StageVariables: map[string]string{}},
			wantEmpty: true,
		},
		{
			name:      "stage_variable_nil_map",
			tmpl:      `$stageVariables.x`,
			ctx:       apigateway.VTLContext{},
			wantEmpty: true,
		},
		// $util.urlEncode / urlDecode
		{
			name:      "util_url_encode",
			tmpl:      `$util.urlEncode('hello world')`,
			ctx:       apigateway.VTLContext{},
			wantEqual: "hello+world",
		},
		{
			name:      "util_url_decode",
			tmpl:      `$util.urlDecode('hello+world')`,
			ctx:       apigateway.VTLContext{},
			wantEqual: "hello world",
		},
		// $util.base64Encode / base64Decode
		{
			name:      "util_base64_encode",
			tmpl:      `$util.base64Encode('hello')`,
			ctx:       apigateway.VTLContext{},
			wantEqual: "aGVsbG8=",
		},
		{
			name:      "util_base64_decode",
			tmpl:      `$util.base64Decode('aGVsbG8=')`,
			ctx:       apigateway.VTLContext{},
			wantEqual: "hello",
		},
		// combined / edge cases
		{
			name:       "combined_json_path_context",
			tmpl:       `{"action":$input.json('$.action'),"user":"$input.path('$.user')","reqId":"$context.requestId"}`,
			ctx:        apigateway.VTLContext{Body: `{"action":"login","user":"dave"}`, RequestID: "abc-999"},
			wantJSONEq: `{"action":"login","user":"dave","reqId":"abc-999"}`,
		},
		{
			name: "combined_full_context",
			tmpl: `{"method":"$context.httpMethod","stage":"$context.stage",` +
				`"apiId":"$context.apiId","table":"$stageVariables.table"}`,
			ctx: apigateway.VTLContext{
				HTTPMethod:     "GET",
				Stage:          "dev",
				APIID:          "xyzabc",
				StageVariables: map[string]string{"table": "orders_dev"},
			},
			wantJSONEq: `{"method":"GET","stage":"dev","apiId":"xyzabc","table":"orders_dev"}`,
		},
		{
			name:      "no_placeholders",
			tmpl:      `{"static":"value"}`,
			ctx:       apigateway.VTLContext{Body: `{"x":1}`},
			wantEqual: `{"static":"value"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := apigateway.RenderTemplate(tt.tmpl, tt.ctx)

			if tt.wantEmpty {
				assert.Empty(t, out)
			}

			for _, s := range tt.wantContains {
				assert.Contains(t, out, s)
			}

			if tt.wantEqual != "" {
				assert.Equal(t, tt.wantEqual, out)
			}

			if tt.wantJSONEq != "" {
				assert.JSONEq(t, tt.wantJSONEq, out)
			}
		})
	}
}

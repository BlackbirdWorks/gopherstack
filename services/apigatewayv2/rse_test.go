package apigatewayv2_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestEvalRouteSelectionExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		expr   string
		method string
		path   string
		header map[string]string
		query  map[string]string
		want   string
	}{
		{
			name:   "empty_expression_returns_default",
			expr:   "",
			method: http.MethodGet,
			path:   "/items",
			want:   "$default",
		},
		{
			name:   "method_and_path",
			expr:   "${request.method} ${request.path}",
			method: http.MethodGet,
			path:   "/items",
			want:   "GET /items",
		},
		{
			name:   "method_only",
			expr:   "${request.method}",
			method: http.MethodPost,
			path:   "/items",
			want:   "POST",
		},
		{
			name:   "path_only",
			expr:   "${request.path}",
			method: http.MethodGet,
			path:   "/users/123",
			want:   "/users/123",
		},
		{
			name:   "header_substitution",
			expr:   "${request.header.x-action}",
			method: http.MethodPost,
			path:   "/",
			header: map[string]string{"x-action": "doSomething"},
			want:   "doSomething",
		},
		{
			name:   "missing_header_returns_empty",
			expr:   "${request.header.x-missing}",
			method: http.MethodGet,
			path:   "/",
			want:   "",
		},
		{
			name:   "querystring_substitution",
			expr:   "${request.querystring.action}",
			method: http.MethodGet,
			path:   "/",
			query:  map[string]string{"action": "list"},
			want:   "list",
		},
		{
			name:   "missing_querystring_returns_empty",
			expr:   "${request.querystring.missing}",
			method: http.MethodGet,
			path:   "/",
			want:   "",
		},
		{
			name:   "literal_dollar_default",
			expr:   "$default",
			method: http.MethodGet,
			path:   "/",
			want:   "$default",
		},
		{
			name:   "websocket_action_expression",
			expr:   "$request.body.action",
			method: http.MethodPost,
			path:   "/",
			want:   "$request.body.action",
		},
		{
			name:   "body_braced_substitution_returns_empty",
			expr:   "${request.body.action}",
			method: http.MethodPost,
			path:   "/",
			want:   "",
		},
		{
			name:   "multiple_substitutions",
			expr:   "${request.method} ${request.path} ${request.querystring.v}",
			method: http.MethodDelete,
			path:   "/item",
			query:  map[string]string{"v": "2"},
			want:   "DELETE /item 2",
		},
		{
			name:   "delete_method",
			expr:   "${request.method} ${request.path}",
			method: http.MethodDelete,
			path:   "/item/42",
			want:   "DELETE /item/42",
		},
		{
			name:   "put_method",
			expr:   "${request.method} ${request.path}",
			method: http.MethodPut,
			path:   "/item/42",
			want:   "PUT /item/42",
		},
		{
			name:   "patch_method",
			expr:   "${request.method} ${request.path}",
			method: http.MethodPatch,
			path:   "/item/42",
			want:   "PATCH /item/42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)

			if len(tt.query) > 0 {
				q := req.URL.Query()
				for k, v := range tt.query {
					q.Set(k, v)
				}

				req.URL.RawQuery = q.Encode()
			}

			for k, v := range tt.header {
				req.Header.Set(k, v)
			}

			got := apigatewayv2.EvalRouteSelectionExpression(tt.expr, req)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvalRouteSelectionExpression_BodyExtraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		body string
		want string
	}{
		{
			name: "top_level_string_field",
			expr: "${request.body.action}",
			body: `{"action":"subscribe"}`,
			want: "subscribe",
		},
		{
			name: "nested_field",
			expr: "${request.body.detail.type}",
			body: `{"detail":{"type":"ping"}}`,
			want: "ping",
		},
		{
			name: "number_field_stringified",
			expr: "${request.body.count}",
			body: `{"count":42}`,
			want: "42",
		},
		{
			name: "bool_field_stringified",
			expr: "${request.body.active}",
			body: `{"active":true}`,
			want: "true",
		},
		{
			name: "missing_field_empty",
			expr: "${request.body.action}",
			body: `{"other":"x"}`,
			want: "",
		},
		{
			name: "non_json_body_empty",
			expr: "${request.body.action}",
			body: `not json`,
			want: "",
		},
		{
			name: "combined_with_method",
			expr: "${request.method}#${request.body.action}",
			body: `{"action":"sendmessage"}`,
			want: "POST#sendmessage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))

			got := apigatewayv2.EvalRouteSelectionExpression(tt.expr, req)
			assert.Equal(t, tt.want, got)

			// Body must remain readable after evaluation.
			rest, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.body, string(rest))
		})
	}
}

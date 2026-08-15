package lambda_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestHandler_TagsRoute(t *testing.T) {
	t.Parallel()

	arn := "arn:aws:lambda:us-east-1:000000000000:function:my-fn"
	tagsPath := "/2017-03-31/tags/" + arn

	tests := []struct {
		wantTagValues   map[string]string
		setup           func(*testing.T, *lambda.Handler)
		verifyTagValues map[string]string
		verifyPath      string
		body            string
		path            string
		name            string
		method          string
		wantTagAbsent   []string
		verifyTagAbsent []string
		wantCode        int
		verifyCode      int
		wantTagsNotNil  bool
	}{
		{
			name:           "get_empty",
			method:         http.MethodGet,
			path:           tagsPath,
			wantCode:       http.StatusOK,
			wantTagsNotNil: true,
		},
		{
			name: "post_and_get",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				rec := callHandler(t, h, http.MethodPost, tagsPath, `{"Tags":{"env":"prod","team":"infra"}}`, nil)
				assert.Equal(t, http.StatusNoContent, rec.Code)
			},
			method:        http.MethodGet,
			path:          tagsPath,
			wantCode:      http.StatusOK,
			wantTagValues: map[string]string{"env": "prod", "team": "infra"},
		},
		{
			name: "delete_tag",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				callHandler(t, h, http.MethodPost, tagsPath, `{"Tags":{"env":"prod","team":"infra"}}`, nil)
			},
			method:          http.MethodDelete,
			path:            tagsPath + "?tagKeys=team",
			wantCode:        http.StatusNoContent,
			verifyPath:      tagsPath,
			verifyCode:      http.StatusOK,
			verifyTagValues: map[string]string{"env": "prod"},
			verifyTagAbsent: []string{"team"},
		},
		{
			name:     "method_not_allowed",
			method:   http.MethodPut,
			path:     tagsPath,
			wantCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, tt.method, tt.path, tt.body, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantTagsNotNil || len(tt.wantTagValues) > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				if tt.wantTagsNotNil {
					assert.NotNil(t, resp["Tags"])
				}
				if len(tt.wantTagValues) > 0 {
					tags, ok := resp["Tags"].(map[string]any)
					require.True(t, ok)
					for k, v := range tt.wantTagValues {
						assert.Equal(t, v, tags[k])
					}
				}
			}

			if tt.verifyPath != "" {
				verifyRec := callHandler(t, h, http.MethodGet, tt.verifyPath, "", nil)
				assert.Equal(t, tt.verifyCode, verifyRec.Code)

				if len(tt.verifyTagValues) > 0 || len(tt.verifyTagAbsent) > 0 {
					var resp map[string]any
					require.NoError(t, json.Unmarshal(verifyRec.Body.Bytes(), &resp))
					tags, ok := resp["Tags"].(map[string]any)
					require.True(t, ok)
					for k, v := range tt.verifyTagValues {
						assert.Equal(t, v, tags[k])
					}
					for _, k := range tt.verifyTagAbsent {
						_, present := tags[k]
						assert.False(t, present, "tag %q should be absent", k)
					}
				}
			}
		})
	}
}

func TestHandler_IAMAction(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "create_function",
			method: http.MethodPost,
			path:   "/2015-03-31/functions",
			want:   "lambda:CreateFunction",
		},
		{
			name:   "list_functions",
			method: http.MethodGet,
			path:   "/2015-03-31/functions",
			want:   "lambda:ListFunctions",
		},
		{
			name:   "get_function",
			method: http.MethodGet,
			path:   "/2015-03-31/functions/my-func",
			want:   "lambda:GetFunction",
		},
		{
			name:   "delete_function",
			method: http.MethodDelete,
			path:   "/2015-03-31/functions/my-func",
			want:   "lambda:DeleteFunction",
		},
		{
			name:   "update_code",
			method: http.MethodPut,
			path:   "/2015-03-31/functions/my-func/code",
			want:   "lambda:UpdateFunctionCode",
		},
		{
			name:   "invoke",
			method: http.MethodPost,
			path:   "/2015-03-31/functions/my-func/invocations",
			want:   "lambda:InvokeFunction",
		},
		{
			name:   "list_tags",
			method: http.MethodGet,
			path:   "/2017-03-31/tags/arn:aws:lambda:us-east-1:0:function:f",
			want:   "lambda:ListTags",
		},
		{
			name:   "tag_resource",
			method: http.MethodPost,
			path:   "/2017-03-31/tags/arn:aws:lambda:us-east-1:0:function:f",
			want:   "lambda:TagResource",
		},
		{name: "non_lambda_path", method: http.MethodGet, path: "/s3/bucket", want: ""},
		{
			name:   "esm_create",
			method: http.MethodPost,
			path:   "/2015-03-31/event-source-mappings",
			want:   "lambda:CreateEventSourceMapping",
		},
		{
			name:   "esm_list",
			method: http.MethodGet,
			path:   "/2015-03-31/event-source-mappings",
			want:   "lambda:ListEventSourceMappings",
		},
		{
			name:   "esm_get",
			method: http.MethodGet,
			path:   "/2015-03-31/event-source-mappings/uuid-1234",
			want:   "lambda:GetEventSourceMapping",
		},
		{
			name:   "esm_delete",
			method: http.MethodDelete,
			path:   "/2015-03-31/event-source-mappings/uuid-1234",
			want:   "lambda:DeleteEventSourceMapping",
		},
		{
			name:   "esm_update",
			method: http.MethodPut,
			path:   "/2015-03-31/event-source-mappings/uuid-1234",
			want:   "lambda:UpdateEventSourceMapping",
		},
		{
			// ESM with unrecognized method → esmIAMAction returns "".
			name:   "esm_unknown_method",
			method: http.MethodPatch,
			path:   "/2015-03-31/event-source-mappings/uuid-1234",
			want:   "",
		},
		{
			// Lambda layers path → extractLayerOperation path in IAMAction (correct prefix).
			name:   "layers_list",
			method: http.MethodGet,
			path:   "/2018-10-31/layers",
			want:   "lambda:ListLayers",
		},
		{
			// Lambda 2020-06-30 path prefix → lambda2020PathPrefix branch in IAMAction.
			name:   "lambda_2020_get_function",
			method: http.MethodGet,
			path:   "/2020-06-30/functions/my-func",
			want:   "lambda:GetFunction",
		},
		{
			// Lambda path with no matching route → returns "".
			name:   "lambda_path_unknown_sub_path",
			method: http.MethodPatch,
			path:   "/2015-03-31/functions/fn/unknown-route",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			assert.Equal(t, tt.want, h.IAMAction(req))
		})
	}
}

// TestHandler_ChaosProvider verifies that the Lambda handler implements the ChaosProvider interface.
func TestHandler_ChaosProvider(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	assert.Equal(t, "lambda", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

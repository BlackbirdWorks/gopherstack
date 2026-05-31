package dlm_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dlm"
)

func newTestHandler(t *testing.T) *dlm.Handler {
	t.Helper()
	backend := dlm.NewInMemoryBackend("000000000000", "us-east-1")

	return dlm.NewHandler(backend)
}

func doRequest(t *testing.T, h *dlm.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error

		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func createPolicy(t *testing.T, h *dlm.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/policies", map[string]any{
		"Description":      "test policy",
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/dlm-role",
		"State":            "ENABLED",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	policyID, ok := resp["PolicyId"].(string)
	require.True(t, ok, "PolicyId missing from response")

	return policyID
}

func TestDLM_LifecyclePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *dlm.Handler) string
		check    func(t *testing.T, body []byte)
		path     func(setup string) string
		body     any
		name     string
		method   string
		wantCode int
	}{
		{
			name:   "CreateLifecyclePolicy returns policyId",
			method: http.MethodPost,
			path:   func(_ string) string { return "/policies" },
			body: map[string]any{
				"Description":      "my backup policy",
				"ExecutionRoleArn": "arn:aws:iam::000000000000:role/dlm-role",
				"State":            "ENABLED",
			},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				assert.Contains(t, resp["PolicyId"], "policy-")
			},
		},
		{
			name:   "GetLifecyclePolicies returns created policy",
			method: http.MethodGet,
			path:   func(_ string) string { return "/policies" },
			setup: func(h *dlm.Handler) string {
				return createPolicy(t, h)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				policies, ok := resp["Policies"].([]any)
				require.True(t, ok)
				assert.Len(t, policies, 1)
			},
		},
		{
			name:   "GetLifecyclePolicy returns full policy details",
			method: http.MethodGet,
			setup: func(h *dlm.Handler) string {
				return createPolicy(t, h)
			},
			path: func(policyID string) string {
				return fmt.Sprintf("/policies/%s", policyID)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				policy, ok := resp["Policy"].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, policy["PolicyId"], "policy-")
				assert.Equal(t, "test policy", policy["Description"])
				assert.NotEmpty(t, policy["PolicyArn"])
			},
		},
		{
			name:     "GetLifecyclePolicy unknown id returns 404",
			method:   http.MethodGet,
			path:     func(_ string) string { return "/policies/policy-doesnotexist" },
			wantCode: http.StatusNotFound,
		},
		{
			name:   "UpdateLifecyclePolicy changes state",
			method: http.MethodPatch,
			setup: func(h *dlm.Handler) string {
				return createPolicy(t, h)
			},
			path: func(policyID string) string {
				return fmt.Sprintf("/policies/%s", policyID)
			},
			body:     map[string]any{"State": "DISABLED"},
			wantCode: http.StatusOK,
		},
		{
			name:     "UpdateLifecyclePolicy unknown id returns 404",
			method:   http.MethodPatch,
			path:     func(_ string) string { return "/policies/policy-doesnotexist" },
			body:     map[string]any{"State": "DISABLED"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteLifecyclePolicy returns 200",
			method: http.MethodDelete,
			setup: func(h *dlm.Handler) string {
				return createPolicy(t, h)
			},
			path: func(policyID string) string {
				return fmt.Sprintf("/policies/%s", policyID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteLifecyclePolicy unknown id returns 404",
			method:   http.MethodDelete,
			path:     func(_ string) string { return "/policies/policy-doesnotexist" },
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var setupResult string
			if tc.setup != nil {
				setupResult = tc.setup(h)
			}

			path := tc.path(setupResult)
			rec := doRequest(t, h, tc.method, path, tc.body)

			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestDLM_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *dlm.Handler) string
		check    func(t *testing.T, body []byte)
		path     func(setup string) string
		body     any
		name     string
		query    string
		method   string
		wantCode int
	}{
		{
			name:   "TagResource applies tags",
			method: http.MethodPost,
			setup: func(h *dlm.Handler) string {
				policyID := createPolicy(t, h)
				rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				policy := resp["Policy"].(map[string]any)
				arn := policy["PolicyArn"].(string)

				return arn
			},
			path: func(arn string) string {
				return fmt.Sprintf("/tags/%s", arn)
			},
			body:     map[string]any{"Tags": map[string]string{"env": "prod"}},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForResource returns tags",
			method: http.MethodGet,
			setup: func(h *dlm.Handler) string {
				policyID := createPolicy(t, h)
				rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				policy := resp["Policy"].(map[string]any)
				arn := policy["PolicyArn"].(string)
				doRequest(t, h, http.MethodPost, fmt.Sprintf("/tags/%s", arn), map[string]any{
					"Tags": map[string]string{"env": "prod"},
				})

				return arn
			},
			path: func(arn string) string {
				return fmt.Sprintf("/tags/%s", arn)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				tags, ok := resp["Tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "prod", tags["env"])
			},
		},
		{
			name:   "UntagResource removes tags",
			method: http.MethodDelete,
			setup: func(h *dlm.Handler) string {
				policyID := createPolicy(t, h)
				rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				policy := resp["Policy"].(map[string]any)
				arn := policy["PolicyArn"].(string)
				doRequest(t, h, http.MethodPost, fmt.Sprintf("/tags/%s", arn), map[string]any{
					"Tags": map[string]string{"env": "prod"},
				})

				return arn
			},
			path: func(arn string) string {
				return fmt.Sprintf("/tags/%s?tagKeys=env", arn)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "ListTagsForResource unknown ARN returns 404",
			method:   http.MethodGet,
			path:     func(_ string) string { return "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-doesnotexist" },
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var setupResult string
			if tc.setup != nil {
				setupResult = tc.setup(h)
			}

			path := tc.path(setupResult)
			rec := doRequest(t, h, tc.method, path, tc.body)

			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

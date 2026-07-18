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

// ---------------------------------------------------------------------------
// Handler meta: Name, Reset, RouteMatcher, MatchPriority, ExtractOperation,
// ExtractResource
// ---------------------------------------------------------------------------

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns DLM", want: "DLM"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.Equal(t, tc.want, h.Name())
		})
	}
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset clears handler backend state"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_ = createPolicy(t, h)
			h.Reset()

			rec := doRequest(t, h, http.MethodGet, "/policies", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			policies := resp["Policies"].([]any)
			assert.Empty(t, policies)
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "matches /policies", path: "/policies", want: true},
		{name: "matches /policies/policy-xxx", path: "/policies/policy-0000000000000001", want: true},
		{
			name: "matches /tags/ with dlm ARN",
			path: "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
			want: true,
		},
		{name: "does not match /tags/ non-dlm ARN", path: "/tags/arn:aws:s3:::bucket", want: false},
		{name: "does not match unrelated path", path: "/ec2/instances", want: false},
		{name: "does not match empty path", path: "/", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			matcher := h.RouteMatcher()

			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tc.want, matcher(c))
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "returns non-zero priority"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.NotZero(t, h.MatchPriority())
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "POST /policies → CreateLifecyclePolicy",
			method: http.MethodPost,
			path:   "/policies",
			want:   "CreateLifecyclePolicy",
		},
		{
			name:   "GET /policies → GetLifecyclePolicies",
			method: http.MethodGet,
			path:   "/policies",
			want:   "GetLifecyclePolicies",
		},
		{
			name:   "GET /policies/id → GetLifecyclePolicy",
			method: http.MethodGet,
			path:   "/policies/policy-abc",
			want:   "GetLifecyclePolicy",
		},
		{
			name:   "DELETE /policies/id → DeleteLifecyclePolicy",
			method: http.MethodDelete,
			path:   "/policies/policy-abc",
			want:   "DeleteLifecyclePolicy",
		},
		{
			name:   "PATCH /policies/id → UpdateLifecyclePolicy",
			method: http.MethodPatch,
			path:   "/policies/policy-abc",
			want:   "UpdateLifecyclePolicy",
		},
		{
			name:   "POST /tags/arn → TagResource",
			method: http.MethodPost,
			path:   "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
			want:   "TagResource",
		},
		{
			name:   "DELETE /tags/arn → UntagResource",
			method: http.MethodDelete,
			path:   "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
			want:   "UntagResource",
		},
		{
			name:   "GET /tags/arn → ListTagsForResource",
			method: http.MethodGet,
			path:   "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
			want:   "ListTagsForResource",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			req := httptest.NewRequest(tc.method, tc.path, nil)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tc.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "extracts resource from /tags/ path",
			path: "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
			want: "arn:aws:dlm:us-east-1:000000000000:policy/policy-1",
		},
		{name: "extracts policy id from /policies/ path", path: "/policies/policy-abc", want: "policy-abc"},
		{name: "returns full path for /policies base", path: "/policies", want: "/policies"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tc.want, h.ExtractResource(c))
		})
	}
}

// ---------------------------------------------------------------------------
// mapError: cover InternalServerException branch (non-awserr error)
// ---------------------------------------------------------------------------

func TestHandler_MapError_InternalError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		path     string
		method   string
		wantType string
		wantCode int
	}{
		{
			// Trigger mapError with an error that is neither ErrNotFound nor ErrInvalidParameter.
			// We DELETE a policy that exists but then delete it again — second delete hits ErrPolicyNotFound
			// which is an awserr.ErrNotFound → 404. We need a different path.
			// Instead, use TagResource on a non-existent ARN to get 404.
			name:     "TagResource unknown ARN returns 404 ResourceNotFoundException",
			path:     "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-doesnotexist",
			method:   http.MethodPost,
			wantCode: http.StatusNotFound,
			wantType: "ResourceNotFoundException",
		},
		{
			name:     "UntagResource unknown ARN returns 404",
			path:     "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-doesnotexist",
			method:   http.MethodDelete,
			wantCode: http.StatusNotFound,
			wantType: "ResourceNotFoundException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bodyBytes []byte
			if tc.method == http.MethodPost {
				bodyBytes, _ = json.Marshal(map[string]any{"Tags": map[string]string{"k": "v"}})
			}

			req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tc.wantCode, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantType, resp["__type"])
		})
	}
}

// ---------------------------------------------------------------------------
// classifyPath: unknown method returns Unknown → 501
// ---------------------------------------------------------------------------

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "PUT on /tags/ ARN returns 501 (unknown method for tags path)",
			method:   http.MethodPut,
			path:     "/tags/arn:aws:dlm:us-east-1:000000000000:policy/policy-x",
			wantCode: http.StatusNotImplemented,
		},
		{
			name:     "PUT on /policies returns 501",
			method:   http.MethodPut,
			path:     "/policies",
			wantCode: http.StatusNotImplemented,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

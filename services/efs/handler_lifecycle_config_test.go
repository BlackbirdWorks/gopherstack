package efs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLifecycleConfiguration tests DescribeLifecycleConfiguration and PutLifecycleConfiguration.
func TestLifecycleConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	tests := []struct {
		setup           func(t *testing.T) string
		body            map[string]any
		name            string
		method          string
		pathSuffix      string
		wantCode        int
		wantPoliciesLen int
	}{
		{
			name: "describe_empty",
			setup: func(t *testing.T) string {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "lc-empty-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)

				return parseResp(t, rec)["FileSystemId"].(string)
			},
			method:          http.MethodGet,
			wantCode:        http.StatusOK,
			wantPoliciesLen: 0,
		},
		{
			name: "put_and_describe",
			setup: func(t *testing.T) string {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "lc-put-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)

				return parseResp(t, rec)["FileSystemId"].(string)
			},
			method: http.MethodPut,
			body: map[string]any{
				"LifecyclePolicies": []map[string]string{
					{"TransitionToIA": "AFTER_30_DAYS"},
				},
			},
			wantCode:        http.StatusOK,
			wantPoliciesLen: 1,
		},
		{
			name: "describe_not_found",
			setup: func(t *testing.T) string {
				t.Helper()

				return "fs-nonexistent"
			},
			method:   http.MethodGet,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fsID := tt.setup(t)
			path := "/2015-02-01/file-systems/" + fsID + "/lifecycle-configuration"
			rec := doREST(t, h, tt.method, path, tt.body)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := parseResp(t, rec)
				policiesRaw, ok := resp["LifecyclePolicies"].([]any)
				require.True(t, ok)
				assert.Len(t, policiesRaw, tt.wantPoliciesLen)
			}
		})
	}
}

// TestLifecyclePolicy_HTTPValidation verifies the handler returns 400 for invalid enum values.
func TestLifecyclePolicy_HTTPValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policies   []map[string]any
		wantStatus int
	}{
		{
			name:       "valid_policy_ok",
			policies:   []map[string]any{{"TransitionToIA": "AFTER_14_DAYS"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_ia_value_returns_400",
			policies:   []map[string]any{{"TransitionToIA": "INVALID_VALUE"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "tok-lp-http-"+tt.name)

			rec := doREST(t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID+"/lifecycle-configuration",
				map[string]any{"LifecyclePolicies": tt.policies})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestLifecyclePolicy_InvalidTransitionRejected verifies that PutLifecycleConfiguration
// returns 400 for an invalid TransitionToIA value. Real AWS rejects unknown enum values.
func TestLifecyclePolicy_InvalidTransitionRejected(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "lifecycle-invalid",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	rec := doREST(t, h, http.MethodPut,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/lifecycle-configuration",
		map[string]any{
			"LifecyclePolicies": []map[string]any{
				{"TransitionToIA": "AFTER_999_DAYS"},
			},
		})

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"PutLifecycleConfiguration with invalid TransitionToIA must return 400; body: %s", rec.Body.String())
}

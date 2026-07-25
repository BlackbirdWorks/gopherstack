package efs_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestFileSystemPolicy exercises DescribeFileSystemPolicy and DeleteFileSystemPolicy.
func TestFileSystemPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *efs.Handler)
		name string
	}{
		{
			name: "describe_policy_not_set_returns_policy_not_found",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "policy-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				fsID := parseResp(t, rec)["FileSystemId"].(string)

				rec2 := doREST(t, h, http.MethodGet,
					"/2015-02-01/file-systems/"+fsID+"/policy", nil)
				assert.Equal(t, http.StatusNotFound, rec2.Code)
				assert.Equal(t, "PolicyNotFound", parseResp(t, rec2)["ErrorCode"])
			},
		},
		{
			name: "delete_policy_is_idempotent",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "policy-del-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				fsID := parseResp(t, rec)["FileSystemId"].(string)

				// Delete when no policy exists - should succeed (idempotent).
				rec2 := doREST(t, h, http.MethodDelete,
					"/2015-02-01/file-systems/"+fsID+"/policy", nil)
				assert.Equal(t, http.StatusNoContent, rec2.Code)
			},
		},
		{
			name: "describe_policy_on_missing_fs_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodGet,
					"/2015-02-01/file-systems/fs-notexist/policy", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_policy_on_missing_fs_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodDelete,
					"/2015-02-01/file-systems/fs-notexist/policy", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestEFSHandler()
			tt.ops(t, h)
		})
	}
}

// TestDescribeFileSystemPolicy_PolicyNotFound_HTTP verifies that DescribeFileSystemPolicy
// returns PolicyNotFound (HTTP 404) when the file system exists but has no policy configured,
// matching the real AWS EFS service model (PolicyNotFound has httpStatusCode 404 -- see
// botocore's efs/service-2.json). Previously returned FileSystemNotFound (404) via the
// wrong error, then briefly 400 via the right error but wrong status code.
func TestDescribeFileSystemPolicy_PolicyNotFound_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "no_policy_returns_policy_not_found",
			wantStatus: http.StatusNotFound,
			wantErr:    "PolicyNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "policy-test-"+tt.name)

			rec := doREST(
				t, h, http.MethodGet,
				"/2015-02-01/file-systems/"+fsID+"/policy",
				nil,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResp(t, rec)
			assert.Equal(t, tt.wantErr, resp["ErrorCode"])
		})
	}
}

// TestPutFileSystemPolicy_InvalidPolicyRejected verifies that malformed or oversized
// policy documents are rejected with InvalidPolicyException (HTTP 400), matching
// botocore's efs/service-2.json PutFileSystemPolicy error catalog (BadRequest,
// InternalServerError, FileSystemNotFound, InvalidPolicyException,
// IncorrectFileSystemLifeCycleState -- notably no ValidationException).
func TestPutFileSystemPolicy_InvalidPolicyRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{name: "malformed_json", policy: `{not valid json}`},
		{name: "oversized_policy", policy: `{"Statement":[{"Sid":"` + strings.Repeat("a", 21*1024) + `"}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "policy-invalid-"+tt.name)

			rec := doREST(
				t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID+"/policy",
				map[string]any{"Policy": tt.policy},
			)
			require.Equal(t, http.StatusBadRequest, rec.Code, "PutFileSystemPolicy body: %s", rec.Body.String())

			resp := parseResp(t, rec)
			assert.Equal(t, "InvalidPolicyException", resp["ErrorCode"])
		})
	}
}

// TestDescribeFileSystemPolicy_AfterPut verifies that once a policy is set,
// DescribeFileSystemPolicy returns it without error (regression guard).
func TestDescribeFileSystemPolicy_AfterPut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{
			name:   "valid_policy_returned",
			policy: `{"Version":"2012-10-17","Statement":[]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "policy-put-"+tt.name)

			// Set policy.
			rec := doREST(
				t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID+"/policy",
				map[string]any{"Policy": tt.policy},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe should now succeed.
			rec = doREST(
				t, h, http.MethodGet,
				"/2015-02-01/file-systems/"+fsID+"/policy",
				nil,
			)
			assert.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			assert.Equal(t, tt.policy, resp["Policy"])
		})
	}
}

// TestPutFileSystemPolicy verifies PutFileSystemPolicy sets and retrieves a policy.
func TestPutFileSystemPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{name: "set_policy", policy: `{"Version":"2012-10-17","Statement":[]}`},
		{
			name:   "overwrite_policy",
			policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "tok-fsp-"+tt.name)

			rec := doREST(t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID+"/policy",
				map[string]any{"Policy": tt.policy})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			assert.Equal(t, tt.policy, resp["Policy"])

			// Verify via describe.
			rec2 := doREST(t, h, http.MethodGet,
				"/2015-02-01/file-systems/"+fsID+"/policy", nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			resp2 := parseResp(t, rec2)
			assert.Equal(t, tt.policy, resp2["Policy"])
		})
	}
}

// TestFileSystemPolicy_RoundTrip verifies that PutFileSystemPolicy stores the policy
// and DescribeFileSystemPolicy returns the exact same JSON. Real AWS returns the policy body
// verbatim in the Policy field.
func TestFileSystemPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()

	fsRec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": "policy-roundtrip",
	})
	require.Equal(t, http.StatusCreated, fsRec.Code)

	var fsOut struct {
		FileSystemID string `json:"FileSystemId"`
	}
	require.NoError(t, json.Unmarshal(fsRec.Body.Bytes(), &fsOut))

	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"elasticfilesystem:ClientMount"}]}`

	putRec := doREST(t, h, http.MethodPut,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/policy",
		map[string]any{"Policy": policy})
	require.Equal(t, http.StatusOK, putRec.Code, "PutFileSystemPolicy failed: %s", putRec.Body.String())

	descRec := doREST(t, h, http.MethodGet,
		"/2015-02-01/file-systems/"+fsOut.FileSystemID+"/policy", nil)
	require.Equal(t, http.StatusOK, descRec.Code, "DescribeFileSystemPolicy failed: %s", descRec.Body.String())

	var descOut struct {
		Policy string `json:"Policy"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	assert.JSONEq(t, policy, descOut.Policy,
		"DescribeFileSystemPolicy must return the stored policy verbatim")
}

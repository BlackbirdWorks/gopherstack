package efs_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestBatch2_DescribeFileSystems_CreationTokenFilter verifies that ?CreationToken= query param
// filters DescribeFileSystems to the matching file system, matching AWS EFS behaviour.
func TestBatch2_DescribeFileSystems_CreationTokenFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		token         string
		wantCount     int
		wantStatus    int
		setupTokens   []string
		wantNotFound  bool
	}{
		{
			name:        "existing_token_returns_matching_fs",
			setupTokens: []string{"tok-alpha", "tok-beta"},
			token:       "tok-alpha",
			wantStatus:  http.StatusOK,
			wantCount:   1,
		},
		{
			name:        "nonexistent_token_returns_empty_list",
			setupTokens: []string{"tok-gamma"},
			token:       "tok-nonexistent",
			wantStatus:  http.StatusOK,
			wantCount:   0,
		},
		{
			name:        "no_token_returns_all",
			setupTokens: []string{"tok-one", "tok-two", "tok-three"},
			token:       "",
			wantStatus:  http.StatusOK,
			wantCount:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			for _, tok := range tt.setupTokens {
				rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": tok,
				})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			path := "/2015-02-01/file-systems"
			if tt.token != "" {
				path += "?CreationToken=" + tt.token
			}

			rec := doRESTRefinement(t, h, http.MethodGet, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseRefinementResp(t, rec)
			fsList, _ := resp["FileSystems"].([]any)
			assert.Len(t, fsList, tt.wantCount)

			if tt.wantCount == 1 && tt.token != "" {
				fs := fsList[0].(map[string]any)
				assert.Equal(t, tt.token, fs["CreationToken"])
			}
		})
	}
}

// TestBatch2_DescribeFileSystems_CreationTokenFilter_Backend verifies the backend
// CreationToken filter directly.
func TestBatch2_DescribeFileSystems_CreationTokenFilter_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		token       string
		setupTokens []string
		wantCount   int
	}{
		{
			name:        "matches_existing_token",
			setupTokens: []string{"tok-x", "tok-y"},
			token:       "tok-x",
			wantCount:   1,
		},
		{
			name:        "no_match_returns_empty",
			setupTokens: []string{"tok-a"},
			token:       "tok-z",
			wantCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			for _, tok := range tt.setupTokens {
				_, err := b.CreateFileSystem(efs.CreateFileSystemRequest{CreationToken: tok})
				require.NoError(t, err)
			}

			list, _, err := b.DescribeFileSystems("", tt.token, "", 0)
			require.NoError(t, err)
			assert.Len(t, list, tt.wantCount)

			if tt.wantCount == 1 {
				assert.Equal(t, tt.token, list[0].CreationToken)
			}
		})
	}
}

// TestBatch2_DescribeFileSystemPolicy_PolicyNotFound verifies that DescribeFileSystemPolicy
// returns PolicyNotFound (HTTP 400) when the file system exists but has no policy configured,
// matching AWS EFS behaviour. Previously returned FileSystemNotFound (404).
func TestBatch2_DescribeFileSystemPolicy_PolicyNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantErr    string
	}{
		{
			name:       "no_policy_returns_policy_not_found",
			wantStatus: http.StatusBadRequest,
			wantErr:    "PolicyNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "policy-test-"+tt.name)

			rec := doRESTRefinement(t, h, http.MethodGet,
				"/2015-02-01/file-systems/"+fsID+"/policy",
				nil,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseRefinementResp(t, rec)
			assert.Equal(t, tt.wantErr, resp["ErrorCode"])
		})
	}
}

// TestBatch2_DescribeFileSystemPolicy_PolicyNotFound_Backend verifies the backend
// returns ErrPolicyNotFound (not ErrNotFound) for a file system with no policy.
func TestBatch2_DescribeFileSystemPolicy_PolicyNotFound_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "fs_with_no_policy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, err := b.CreateFileSystem(efs.CreateFileSystemRequest{CreationToken: "policy-backend-" + tt.name})
			require.NoError(t, err)

			_, err = b.DescribeFileSystemPolicy(fs.FileSystemID)
			require.ErrorIs(t, err, efs.ErrPolicyNotFound)
			require.NotErrorIs(t, err, efs.ErrNotFound)
		})
	}
}

// TestBatch2_DescribeFileSystemPolicy_AfterPut verifies that once a policy is set,
// DescribeFileSystemPolicy returns it without error (regression guard).
func TestBatch2_DescribeFileSystemPolicy_AfterPut(t *testing.T) {
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

			h := newRefinementHandler()
			fsID := createFS(t, h, "policy-put-"+tt.name)

			// Set policy.
			rec := doRESTRefinement(t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID+"/policy",
				map[string]any{"Policy": tt.policy},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe should now succeed.
			rec = doRESTRefinement(t, h, http.MethodGet,
				"/2015-02-01/file-systems/"+fsID+"/policy",
				nil,
			)
			assert.Equal(t, http.StatusOK, rec.Code)

			resp := parseRefinementResp(t, rec)
			assert.Equal(t, tt.policy, resp["Policy"])
		})
	}
}

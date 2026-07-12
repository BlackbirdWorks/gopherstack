package efs_test

import (
	"context"
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
		name         string
		token        string
		setupTokens  []string
		wantCount    int
		wantStatus   int
		wantNotFound bool
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
				rec := doRESTRefinement(
					t,
					h,
					http.MethodPost,
					"/2015-02-01/file-systems",
					map[string]any{
						"CreationToken": tok,
					},
				)
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
				_, err := b.CreateFileSystem(
					context.Background(),
					efs.CreateFileSystemRequest{CreationToken: tok},
				)
				require.NoError(t, err)
			}

			list, _, err := b.DescribeFileSystems(context.Background(), "", tt.token, "", 0)
			require.NoError(t, err)
			assert.Len(t, list, tt.wantCount)

			if tt.wantCount == 1 {
				assert.Equal(t, tt.token, list[0].CreationToken)
			}
		})
	}
}

// TestBatch2_DescribeFileSystemPolicy_PolicyNotFound verifies that DescribeFileSystemPolicy
// returns PolicyNotFound (HTTP 404) when the file system exists but has no policy configured,
// matching the real AWS EFS service model (PolicyNotFound has httpStatusCode 404 -- see
// botocore's efs/service-2.json). Previously returned FileSystemNotFound (404) via the
// wrong error, then briefly 400 via the right error but wrong status code.
func TestBatch2_DescribeFileSystemPolicy_PolicyNotFound(t *testing.T) {
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

			h := newRefinementHandler()
			fsID := createFS(t, h, "policy-test-"+tt.name)

			rec := doRESTRefinement(
				t, h, http.MethodGet,
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
			fs, err := b.CreateFileSystem(
				context.Background(),
				efs.CreateFileSystemRequest{CreationToken: "policy-backend-" + tt.name},
			)
			require.NoError(t, err)

			_, err = b.DescribeFileSystemPolicy(context.Background(), fs.FileSystemID)
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
			rec := doRESTRefinement(
				t, h, http.MethodPut,
				"/2015-02-01/file-systems/"+fsID+"/policy",
				map[string]any{"Policy": tt.policy},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe should now succeed.
			rec = doRESTRefinement(
				t, h, http.MethodGet,
				"/2015-02-01/file-systems/"+fsID+"/policy",
				nil,
			)
			assert.Equal(t, http.StatusOK, rec.Code)

			resp := parseRefinementResp(t, rec)
			assert.Equal(t, tt.policy, resp["Policy"])
		})
	}
}

// TestBatch2_MountTargetArn verifies that CreateMountTarget and DescribeMountTargets
// responses include MountTargetArn, matching the AWS EFS MountTargetDescription shape.
func TestBatch2_MountTargetArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		via  string // "create" or "describe"
	}{
		{name: "create_response_includes_arn", via: "create"},
		{name: "describe_response_includes_arn", via: "describe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()
			fsID := createFS(t, h, "mt-arn-"+tt.name)

			rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
				"FileSystemId": fsID,
				"SubnetId":     "subnet-aabbcc",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var mtARN string
			if tt.via == "create" {
				resp := parseRefinementResp(t, rec)
				mtARN, _ = resp["MountTargetArn"].(string)
			} else {
				rec2 := doRESTRefinement(t, h, http.MethodGet, "/2015-02-01/mount-targets", nil)
				require.Equal(t, http.StatusOK, rec2.Code)
				mts := parseRefinementResp(t, rec2)["MountTargets"].([]any)
				require.Len(t, mts, 1)
				mtARN, _ = mts[0].(map[string]any)["MountTargetArn"].(string)
			}

			assert.NotEmpty(t, mtARN)
			assert.Contains(t, mtARN, "mount-target/fsmt-")
		})
	}
}

// TestBatch2_DescribeMountTargets_AccessPointIdFilter verifies that passing ?AccessPointId=
// to DescribeMountTargets returns mount targets for the file system the access point belongs
// to, matching real AWS EFS behavior.
func TestBatch2_DescribeMountTargets_AccessPointIdFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErr    string
		wantCount  int
		wantStatus int
		hasMT      bool
	}{
		{
			name:       "access_point_with_mount_target_returns_it",
			hasMT:      true,
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "access_point_without_mount_target_returns_empty",
			hasMT:      false,
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "nonexistent_access_point_returns_404",
			wantStatus: http.StatusNotFound,
			wantErr:    "AccessPointNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRefinementHandler()

			if tt.wantStatus == http.StatusNotFound {
				rec := doRESTRefinement(
					t, h, http.MethodGet,
					"/2015-02-01/mount-targets?AccessPointId=fsap-notexist",
					nil,
				)
				assert.Equal(t, tt.wantStatus, rec.Code)
				resp := parseRefinementResp(t, rec)
				assert.Equal(t, tt.wantErr, resp["ErrorCode"])

				return
			}

			fsID := createFS(t, h, "mt-ap-filter-"+tt.name)

			// Create access point on the file system.
			rec := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/access-points", map[string]any{
				"FileSystemId": fsID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			apID := parseRefinementResp(t, rec)["AccessPointId"].(string)

			if tt.hasMT {
				rec2 := doRESTRefinement(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "subnet-1122",
				})
				require.Equal(t, http.StatusOK, rec2.Code)
			}

			rec3 := doRESTRefinement(
				t, h, http.MethodGet,
				"/2015-02-01/mount-targets?AccessPointId="+apID,
				nil,
			)
			assert.Equal(t, tt.wantStatus, rec3.Code)

			mts := parseRefinementResp(t, rec3)["MountTargets"].([]any)
			assert.Len(t, mts, tt.wantCount)

			if tt.wantCount > 0 {
				mt := mts[0].(map[string]any)
				assert.Equal(t, fsID, mt["FileSystemId"])
				assert.NotEmpty(t, mt["MountTargetArn"])
			}
		})
	}
}

// TestBatch2_DescribeMountTargets_BackendAccessPointFilter verifies the backend
// DescribeAccessPoints can be used to resolve an access point to its file system,
// enabling the AccessPointId filter in the handler layer.
func TestBatch2_DescribeMountTargets_BackendAccessPointFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		apExists bool
	}{
		{name: "existing_access_point_resolves_to_fs", apExists: true},
		{name: "missing_access_point_returns_not_found", apExists: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			fs, createErr := b.CreateFileSystem(context.Background(), fsReq("ap-resolve-"+tt.name))
			require.NoError(t, createErr)

			if tt.apExists {
				ap, apErr := b.CreateAccessPoint(context.Background(), apReq(fs.FileSystemID))
				require.NoError(t, apErr)

				// Access point should resolve to the same file system.
				aps, _, descErr := b.DescribeAccessPoints(context.Background(), "", ap.AccessPointID, "", 1)
				require.NoError(t, descErr)
				require.Len(t, aps, 1)
				assert.Equal(t, fs.FileSystemID, aps[0].FileSystemID)
			} else {
				_, _, descErr := b.DescribeAccessPoints(context.Background(), "", "fsap-missing", "", 1)
				require.ErrorIs(t, descErr, efs.ErrAccessPointNotFound)
			}
		})
	}
}

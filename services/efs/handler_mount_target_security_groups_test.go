package efs_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestDescribeMountTargetSecurityGroups exercises DescribeMountTargetSecurityGroups.
func TestDescribeMountTargetSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *efs.Handler)
		name string
	}{
		{
			name: "returns_empty_groups_for_new_mount_target",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "sg-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				fsID := parseResp(t, rec)["FileSystemId"].(string)

				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "subnet-sg-test",
				})
				require.Equal(t, http.StatusOK, rec2.Code)
				mtID := parseResp(t, rec2)["MountTargetId"].(string)

				rec3 := doREST(t, h, http.MethodGet,
					"/2015-02-01/mount-targets/"+mtID+"/security-groups", nil)
				require.Equal(t, http.StatusOK, rec3.Code)

				resp := parseResp(t, rec3)
				groups := resp["SecurityGroups"].([]any)
				assert.Empty(t, groups)
			},
		},
		{
			name: "missing_mount_target_returns_404",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodGet,
					"/2015-02-01/mount-targets/fsmt-notexist/security-groups", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			// Same httpStatusCode=400 rule as CreateMountTarget: SecurityGroupLimitExceeded
			// is a 400, not a 409.
			name: "modify_too_many_security_groups_returns_400",
			ops: func(t *testing.T, h *efs.Handler) {
				t.Helper()

				rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
					"CreationToken": "sg-modify-limit-token",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				fsID := parseResp(t, rec)["FileSystemId"].(string)

				rec2 := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "subnet-sg-modify-test",
				})
				require.Equal(t, http.StatusOK, rec2.Code)
				mtID := parseResp(t, rec2)["MountTargetId"].(string)

				rec3 := doREST(t, h, http.MethodPut,
					"/2015-02-01/mount-targets/"+mtID+"/security-groups", map[string]any{
						"SecurityGroups": []string{"sg-1", "sg-2", "sg-3", "sg-4", "sg-5", "sg-6"},
					})
				assert.Equal(t, http.StatusBadRequest, rec3.Code)
				assert.Equal(t, "SecurityGroupLimitExceeded", parseResp(t, rec3)["ErrorCode"])
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

// TestModifyMountTargetSecurityGroups_MaxQuota verifies the max-5 quota on modify.
func TestModifyMountTargetSecurityGroups_MaxQuota(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		securityGroups []string
		wantHTTPStatus int
	}{
		{
			name:           "replace_with_three",
			securityGroups: []string{"sg-x", "sg-y", "sg-z"},
			wantHTTPStatus: http.StatusNoContent,
		},
		{
			// SecurityGroupLimitExceeded has httpStatusCode 400 in the AWS EFS service
			// model (botocore efs/service-2.json), not 409.
			name:           "six_rejected",
			securityGroups: []string{"sg-1", "sg-2", "sg-3", "sg-4", "sg-5", "sg-6"},
			wantHTTPStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEFSHandler()
			fsID := createFS(t, h, "tok-mtsgs-"+tt.name)

			mtRec := doREST(
				t,
				h,
				http.MethodPost,
				"/2015-02-01/mount-targets",
				map[string]any{
					"FileSystemId": fsID,
					"SubnetId":     "subnet-abc",
				},
			)
			require.Equal(t, http.StatusOK, mtRec.Code)
			mtID := parseResp(t, mtRec)["MountTargetId"].(string)

			rec := doREST(t, h, http.MethodPut,
				"/2015-02-01/mount-targets/"+mtID+"/security-groups",
				map[string]any{"SecurityGroups": tt.securityGroups})
			assert.Equal(t, tt.wantHTTPStatus, rec.Code)
		})
	}
}

package backup_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestBackupTagging exercises TagResource and ListTags.
func TestBackupTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *backup.Handler)
		name string
	}{
		{
			name: "tag_and_list",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				// Create vault and get its ARN.
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/tag-vault", nil)
				require.Equal(t, http.StatusOK, rec.Code)
				vaultResp := parseResp(t, rec)
				vaultARN := vaultResp["BackupVaultArn"].(string)

				// TagResource.
				tagRec := doREST(t, h, http.MethodPost, "/tags/"+vaultARN, map[string]any{
					"Tags": map[string]string{"Project": "demo"},
				})
				assert.Equal(t, http.StatusOK, tagRec.Code)

				// ListTags.
				listRec := doREST(t, h, http.MethodGet, "/tags/"+vaultARN, nil)
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseResp(t, listRec)
				tags, ok := listResp["Tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "demo", tags["Project"])
			},
		},
		{
			name: "tag_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(
					t,
					h,
					http.MethodPost,
					"/tags/arn:aws:backup:us-east-1:123:backup-vault:no",
					map[string]any{
						"Tags": map[string]string{"k": "v"},
					},
				)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestUntagResource exercises UntagResource via the REST handler.
func TestUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *backup.Handler)
		name string
	}{
		{
			name: "untag_vault",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPut, "/backup-vaults/my-vault", map[string]any{
					"BackupVaultTags": map[string]string{"env": "test", "team": "platform"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				vaultARN := parseResp(t, rec)["BackupVaultArn"].(string)

				// Remove one tag.
				delRec := doREST(t, h, http.MethodPost, "/untag/"+vaultARN, map[string]any{
					"TagKeyList": []string{"env"},
				})
				require.Equal(t, http.StatusOK, delRec.Code)

				// Verify tag was removed.
				listRec := doREST(t, h, http.MethodGet, "/tags/"+vaultARN, nil)
				require.Equal(t, http.StatusOK, listRec.Code)
				tags := parseResp(t, listRec)["Tags"].(map[string]any)
				assert.NotContains(t, tags, "env")
				assert.Contains(t, tags, "team")
			},
		},
		{
			name: "untag_framework",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
					"FrameworkName": "fw",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				fwARN := parseResp(t, rec)["FrameworkArn"].(string)

				// Tag and then untag.
				doREST(t, h, http.MethodPost, "/tags/"+fwARN, map[string]any{
					"Tags": map[string]string{"k": "v"},
				})
				delRec := doREST(t, h, http.MethodPost, "/untag/"+fwARN, map[string]any{
					"TagKeyList": []string{"k"},
				})
				assert.Equal(t, http.StatusOK, delRec.Code)
			},
		},
		{
			name: "untag_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				const missingARN = "/untag/arn:aws:backup:us-east-1:000000000000:backup-vault:missing"
				rec := doREST(t, h, http.MethodPost, missingARN, map[string]any{
					"TagKeyList": []string{"k"},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestTagsInVaultResponse checks that DescribeBackupVault includes Tags.
func TestTagsInVaultResponse(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	doREST(t, h, http.MethodPut, "/backup-vaults/tagged-vault", map[string]any{
		"BackupVaultTags": map[string]string{"env": "prod"},
	})

	rec := doREST(t, h, http.MethodGet, "/backup-vaults/tagged-vault", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	tags, ok := resp["Tags"].(map[string]any)
	require.True(t, ok, "Tags field missing from DescribeBackupVault response")
	assert.Equal(t, "prod", tags["env"])
}

// TestTagsInPlanResponse checks that GetBackupPlan includes Tags.
func TestTagsInPlanResponse(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
		"BackupPlan":     map[string]any{"BackupPlanName": "tagged-plan"},
		"BackupPlanTags": map[string]string{"env": "staging"},
	})

	// List plans to get the plan ID.
	listRec := doREST(t, h, http.MethodGet, "/backup/plans", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	listResp := parseResp(t, listRec)
	plans := listResp["BackupPlansList"].([]any)
	require.Len(t, plans, 1)
	planID := plans[0].(map[string]any)["BackupPlanId"].(string)

	rec := doREST(t, h, http.MethodGet, "/backup/plans/"+planID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	tags, ok := resp["Tags"].(map[string]any)
	require.True(t, ok, "Tags field missing from GetBackupPlan response")
	assert.Equal(t, "staging", tags["env"])
}

// TestTagResourceForFrameworkAndReportPlan checks tagging on frameworks and report plans.
func TestTagResourceForFrameworkAndReportPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(h *backup.Handler) string
		name   string
	}{
		{
			name: "framework",
			create: func(h *backup.Handler) string {
				rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
					"FrameworkName": "fw-tag-test",
				})

				return parseResp(t, rec)["FrameworkArn"].(string)
			},
		},
		{
			name: "report_plan",
			create: func(h *backup.Handler) string {
				rec := doREST(t, h, http.MethodPost, "/audit/report-plans", map[string]any{
					"ReportPlanName": "rp-tag-test",
				})

				return parseResp(t, rec)["ReportPlanArn"].(string)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			arn := tt.create(h)

			// Tag resource.
			tagRec := doREST(t, h, http.MethodPost, "/tags/"+arn, map[string]any{
				"Tags": map[string]string{"foo": "bar"},
			})
			assert.Equal(t, http.StatusOK, tagRec.Code)

			// List tags.
			listRec := doREST(t, h, http.MethodGet, "/tags/"+arn, nil)
			require.Equal(t, http.StatusOK, listRec.Code)
			tags := parseResp(t, listRec)["Tags"].(map[string]any)
			assert.Equal(t, "bar", tags["foo"])
		})
	}
}

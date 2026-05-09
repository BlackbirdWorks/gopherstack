package backup_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// Helper: create a backup plan and return its ID.
func createBackupPlan(t *testing.T, h *backup.Handler, name string) string {
	t.Helper()

	rec := doREST(t, h, http.MethodPut, "/backup/plans", map[string]any{
		"BackupPlan": map[string]any{
			"BackupPlanName": name,
			"Rules": []map[string]any{{
				"RuleName":        "daily",
				"TargetBackupVaultName": name + "-vault",
				"ScheduleExpression":    "cron(0 12 * * ? *)",
			}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	id, ok := resp["BackupPlanId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}

func TestBackupPlan_CreateGetListDelete(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	doREST(t, h, http.MethodPut, "/backup-vaults/plan-vault", nil)

	// Create
	planID := createBackupPlan(t, h, "myplan")
	assert.NotEmpty(t, planID)

	// Get
	rec := doREST(t, h, http.MethodGet, "/backup/plans/"+planID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	assert.Equal(t, planID, resp["BackupPlanId"])

	// List
	rec2 := doREST(t, h, http.MethodGet, "/backup/plans", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)
	listResp := parseResp(t, rec2)
	plans, ok := listResp["BackupPlansList"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, plans)

	// Delete
	rec3 := doREST(t, h, http.MethodDelete, "/backup/plans/"+planID, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Get after delete
	rec4 := doREST(t, h, http.MethodGet, "/backup/plans/"+planID, nil)
	assert.Equal(t, http.StatusNotFound, rec4.Code)
}

func TestBackupSelection_CreateGetListDelete(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()
	doREST(t, h, http.MethodPut, "/backup-vaults/sel-vault", nil)
	planID := createBackupPlan(t, h, "selplan")

	// Create selection
	rec := doREST(t, h, http.MethodPut, fmt.Sprintf("/backup/plans/%s/selections", planID), map[string]any{
		"BackupSelection": map[string]any{
			"SelectionName": "myselection",
			"IamRoleArn":    "arn:aws:iam::123456789012:role/role",
			"Resources":     []string{"arn:aws:ec2:us-east-1:123456789012:instance/*"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	selID, ok := resp["SelectionId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, selID)

	// Get selection
	rec2 := doREST(t, h, http.MethodGet, fmt.Sprintf("/backup/plans/%s/selections/%s", planID, selID), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List selections
	rec3 := doREST(t, h, http.MethodGet, fmt.Sprintf("/backup/plans/%s/selections", planID), nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
	listResp := parseResp(t, rec3)
	sels, ok := listResp["BackupSelectionsList"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, sels)

	// Delete selection
	rec4 := doREST(t, h, http.MethodDelete, fmt.Sprintf("/backup/plans/%s/selections/%s", planID, selID), nil)
	assert.Equal(t, http.StatusOK, rec4.Code)
}

func TestRestoreTestingPlan_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	// Create (PUT /restore-testing/plans)
	rec := doREST(t, h, http.MethodPut, "/restore-testing/plans", map[string]any{
		"RestoreTestingPlan": map[string]any{
			"RestoreTestingPlanName": "myrestoreplan",
			"ScheduleExpression":     "cron(0 1 * * ? *)",
			"StartWindowHours":       1,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	planName, ok := resp["RestoreTestingPlanName"].(string)
	require.True(t, ok)
	require.NotEmpty(t, planName)

	// Get
	rec2 := doREST(t, h, http.MethodGet, "/restore-testing/plans/"+planName, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List
	rec3 := doREST(t, h, http.MethodGet, "/restore-testing/plans", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
	listResp := parseResp(t, rec3)
	plans, ok := listResp["RestoreTestingPlans"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, plans)

	// Update
	rec4 := doREST(t, h, http.MethodPut, "/restore-testing/plans/"+planName, map[string]any{
		"RestoreTestingPlan": map[string]any{
			"StartWindowHours": 2,
		},
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete
	rec5 := doREST(t, h, http.MethodDelete, "/restore-testing/plans/"+planName, nil)
	assert.Equal(t, http.StatusOK, rec5.Code)
}

func TestAuditFramework_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	// Create
	rec := doREST(t, h, http.MethodPost, "/audit/frameworks", map[string]any{
		"FrameworkName": "myframework",
		"FrameworkControls": []map[string]any{{
			"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK",
		}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	frameworkArn, ok := resp["FrameworkArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, frameworkArn)

	// Describe
	rec2 := doREST(t, h, http.MethodGet, "/audit/frameworks/myframework", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List
	rec3 := doREST(t, h, http.MethodGet, "/audit/frameworks", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
	listResp := parseResp(t, rec3)
	fwks, ok := listResp["Frameworks"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, fwks)

	// Update
	rec4 := doREST(t, h, http.MethodPut, "/audit/frameworks/myframework", map[string]any{
		"FrameworkControls": []map[string]any{{
			"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK",
		}},
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete
	rec5 := doREST(t, h, http.MethodDelete, "/audit/frameworks/myframework", nil)
	assert.Equal(t, http.StatusOK, rec5.Code)
}

func TestAuditReportPlan_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestBackupHandler()

	// Create
	rec := doREST(t, h, http.MethodPost, "/audit/report-plans", map[string]any{
		"ReportPlanName": "myreportplan",
		"ReportDeliveryChannel": map[string]any{
			"S3BucketName": "my-reports-bucket",
		},
		"ReportSetting": map[string]any{
			"ReportTemplate": "BACKUP_JOB_REPORT",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	planName, ok := resp["ReportPlanName"].(string)
	require.True(t, ok)
	require.NotEmpty(t, planName)

	// Describe
	rec2 := doREST(t, h, http.MethodGet, "/audit/report-plans/"+planName, nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List
	rec3 := doREST(t, h, http.MethodGet, "/audit/report-plans", nil)
	assert.Equal(t, http.StatusOK, rec3.Code)
	listResp := parseResp(t, rec3)
	plans, ok := listResp["ReportPlans"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, plans)

	// Update
	rec4 := doREST(t, h, http.MethodPut, "/audit/report-plans/"+planName, map[string]any{
		"ReportDeliveryChannel": map[string]any{
			"S3BucketName": "updated-bucket",
		},
		"ReportSetting": map[string]any{
			"ReportTemplate": "BACKUP_JOB_REPORT",
		},
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete
	rec5 := doREST(t, h, http.MethodDelete, "/audit/report-plans/"+planName, nil)
	assert.Equal(t, http.StatusOK, rec5.Code)
}

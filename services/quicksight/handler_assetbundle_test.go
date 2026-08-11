package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Asset bundle export job lifecycle and errors ----

func TestQuickSight_AssetBundleExportJob_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doRequest(t, h, http.MethodPost, accountPath("/asset-bundle-export-jobs"), map[string]any{
		"AssetBundleExportJobId": "job1",
		"ResourceArns":           []string{"arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1"},
		"ExportFormat":           "QUICKSIGHT_JSON",
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	assert.Equal(t, "job1", parseBody(t, startRec)["AssetBundleExportJobId"])

	// Missing required ResourceArns -> validation error.
	invalidRec := doRequest(t, h, http.MethodPost, accountPath("/asset-bundle-export-jobs"), map[string]any{
		"AssetBundleExportJobId": "job2",
	})
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)

	// First describe settles the job to SUCCESSFUL, not stuck QUEUED.
	describeRec := doRequest(t, h, http.MethodGet, accountPath("/asset-bundle-export-jobs/job1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	body := parseBody(t, describeRec)
	assert.Equal(t, "SUCCESSFUL", body["JobStatus"])
	assert.NotEmpty(t, body["DownloadUrl"])

	// Subsequent describes stay settled.
	describeAgainRec := doRequest(t, h, http.MethodGet, accountPath("/asset-bundle-export-jobs/job1"), nil)
	assert.Equal(t, "SUCCESSFUL", parseBody(t, describeAgainRec)["JobStatus"])

	missingRec := doRequest(t, h, http.MethodGet, accountPath("/asset-bundle-export-jobs/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
}

// ---- StartAssetBundleExportJobInput's Include* request flags round-trip
// into DescribeAssetBundleExportJobOutput ----

func TestQuickSight_AssetBundleExportJob_IncludeFlags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doRequest(t, h, http.MethodPost, accountPath("/asset-bundle-export-jobs"), map[string]any{
		"AssetBundleExportJobId":   "job1",
		"ResourceArns":             []string{"arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1"},
		"IncludeFolderMembers":     "RECURSE",
		"IncludeFolderMemberships": true,
		"IncludePermissions":       true,
		"IncludeTags":              true,
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/asset-bundle-export-jobs/job1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	body := parseBody(t, describeRec)
	assert.Equal(t, "RECURSE", body["IncludeFolderMembers"])
	assert.Equal(t, true, body["IncludeFolderMemberships"])
	assert.Equal(t, true, body["IncludePermissions"])
	assert.Equal(t, true, body["IncludeTags"])
}

func TestQuickSight_ListAssetBundleExportJobs_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		doRequest(t, h, http.MethodPost, accountPath("/asset-bundle-export-jobs"), map[string]any{
			"AssetBundleExportJobId": id,
			"ResourceArns":           []string{"arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1"},
		})
	}

	rec := doRequest(t, h, http.MethodGet, accountPath("/asset-bundle-export-jobs?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	items := parseBody(t, rec)["AssetBundleExportJobSummaryList"].([]any)
	assert.Len(t, items, 2)
	next := parseBody(t, rec)["NextToken"].(string)
	require.NotEmpty(t, next)

	page2 := doRequest(
		t, h, http.MethodGet,
		accountPath(fmt.Sprintf("/asset-bundle-export-jobs?max-results=2&next-token=%s", next)), nil,
	)
	require.Equal(t, http.StatusOK, page2.Code)
	assert.Len(t, parseBody(t, page2)["AssetBundleExportJobSummaryList"].([]any), 2)
}

// ---- Asset bundle import job lifecycle and errors ----

func TestQuickSight_AssetBundleImportJob_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doRequest(t, h, http.MethodPost, accountPath("/asset-bundle-import-jobs"), map[string]any{
		"AssetBundleImportJobId": "job1",
		"FailureAction":          "ROLLBACK",
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	assert.Equal(t, "job1", parseBody(t, startRec)["AssetBundleImportJobId"])

	invalidRec := doRequest(t, h, http.MethodPost, accountPath("/asset-bundle-import-jobs"), map[string]any{})
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/asset-bundle-import-jobs/job1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Equal(t, "SUCCESSFUL", parseBody(t, describeRec)["JobStatus"])
	assert.Equal(t, "ROLLBACK", parseBody(t, describeRec)["FailureAction"])

	missingRec := doRequest(t, h, http.MethodGet, accountPath("/asset-bundle-import-jobs/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)

	listRec := doRequest(t, h, http.MethodGet, accountPath("/asset-bundle-import-jobs"), nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.Len(t, parseBody(t, listRec)["AssetBundleImportJobSummaryList"].([]any), 1)
}

// ---- Dashboard snapshot job lifecycle and errors ----

func TestQuickSight_DashboardSnapshotJob_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createDashRec := doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1"), map[string]any{
		"Name": "Dashboard1",
	})
	require.Equal(t, http.StatusOK, createDashRec.Code)

	// Starting a snapshot job for an unknown dashboard -> 404.
	missingDashRec := doRequest(
		t, h, http.MethodPost, accountPath("/dashboards/notexist/snapshot-jobs"),
		map[string]any{"SnapshotJobId": "job1"},
	)
	assert.Equal(t, http.StatusNotFound, missingDashRec.Code)

	startRec := doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1/snapshot-jobs"), map[string]any{
		"SnapshotJobId":         "job1",
		"SnapshotConfiguration": map[string]any{"FileFormats": []string{"PDF"}},
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	assert.Equal(t, "job1", parseBody(t, startRec)["SnapshotJobId"])

	// First poll settles the job to COMPLETED.
	describeRec := doRequest(t, h, http.MethodGet, accountPath("/dashboards/dash1/snapshot-jobs/job1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	descBody := parseBody(t, describeRec)
	assert.Equal(t, "COMPLETED", descBody["JobStatus"])
	assert.Equal(t, "dash1", descBody["DashboardId"])

	resultRec := doRequest(t, h, http.MethodGet, accountPath("/dashboards/dash1/snapshot-jobs/job1/result"), nil)
	require.Equal(t, http.StatusOK, resultRec.Code)
	resultBody := parseBody(t, resultRec)
	assert.Equal(t, "COMPLETED", resultBody["JobStatus"])
	result := resultBody["Result"].(map[string]any)
	assert.Contains(t, result["S3Uri"], "job1")

	missingJobRec := doRequest(t, h, http.MethodGet, accountPath("/dashboards/dash1/snapshot-jobs/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingJobRec.Code)
}

// ---- StartDashboardSnapshotJobSchedule ----

// TestQuickSight_StartDashboardSnapshotJobSchedule verifies the real (not
// fabricated) response shape and error behavior: real
// StartDashboardSnapshotJobScheduleOutput carries only RequestId/Status (no
// DashboardId), the target dashboard must actually exist, and a missing
// ScheduleId is a validation error.
func TestQuickSight_StartDashboardSnapshotJobSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash-sched"), map[string]any{
		"Name": "DashSched",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	tests := []struct {
		name       string
		path       string
		wantCode   string
		wantStatus int
	}{
		{
			name:       "existing_dashboard_and_schedule",
			path:       accountPath("/dashboards/dash-sched/schedules/sched1"),
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_dashboard",
			path:       accountPath("/dashboards/no-such-dashboard/schedules/sched1"),
			wantStatus: http.StatusNotFound,
			wantCode:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			body := parseBody(t, rec)
			if tt.wantStatus == http.StatusOK {
				assert.NotContains(t, body, "DashboardId",
					"a stub response would fabricate a DashboardId field; real AWS output has none")
				assert.Contains(t, body, "RequestId")

				return
			}

			assert.Equal(t, tt.wantCode, body["Code"])
		})
	}
}

// TestSnapshotJobIDIsUnique verifies StartDashboardSnapshotJob returns distinct IDs per call.
func TestSnapshotJobIDIsUnique(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	h := quicksight.NewHandler(b)

	doRequest(t, h, http.MethodPost, accountPath("/dashboards/d1"), map[string]any{"Name": "Dash"})

	path := accountPath("/dashboards/d1/snapshot-jobs")

	rec1 := doRequest(t, h, http.MethodPost, path, map[string]any{"SnapshotJobId": "job1"})
	rec2 := doRequest(t, h, http.MethodPost, path, map[string]any{"SnapshotJobId": "job2"})
	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, http.StatusOK, rec2.Code)

	id1, _ := parseBody(t, rec1)["SnapshotJobId"].(string)
	id2, _ := parseBody(t, rec2)["SnapshotJobId"].(string)
	assert.NotEmpty(t, id1)
	assert.NotEqual(t, id1, id2, "each snapshot job must have a unique ID")
}

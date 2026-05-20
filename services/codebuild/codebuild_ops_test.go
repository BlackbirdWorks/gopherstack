package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

// helpers

func createTestProject(t *testing.T, h *codebuild.Handler, name string) {
	t.Helper()

	rec := doRequest(t, h, "CreateProject", map[string]any{
		"name":      name,
		"source":    map[string]any{"type": "NO_SOURCE"},
		"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:5.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func projectARN(t *testing.T, h *codebuild.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, "BatchGetProjects", map[string]any{"names": []string{name}})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Projects []struct {
			Arn string `json:"arn"`
		} `json:"projects"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotEmpty(t, out.Projects)

	return out.Projects[0].Arn
}

// TestCodeBuild_SourceCredentials covers Import, List, Delete.
func TestCodeBuild_SourceCredentials(t *testing.T) {
	t.Parallel()

	t.Run("import_returns_arn", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ImportSourceCredentials", map[string]any{
			"authType":   "PERSONAL_ACCESS_TOKEN",
			"serverType": "GITHUB",
			"token":      "my-token",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Arn string `json:"arn"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Contains(t, out.Arn, "GITHUB")
	})

	t.Run("import_missing_token", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ImportSourceCredentials", map[string]any{
			"authType":   "PERSONAL_ACCESS_TOKEN",
			"serverType": "GITHUB",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("list_after_import", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "ImportSourceCredentials", map[string]any{
			"authType":   "PERSONAL_ACCESS_TOKEN",
			"serverType": "BITBUCKET",
			"token":      "tok",
		})

		rec := doRequest(t, h, "ListSourceCredentials", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			SourceCredentialsInfos []map[string]any `json:"sourceCredentialsInfos"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		require.Len(t, out.SourceCredentialsInfos, 1)
		assert.Equal(t, "BITBUCKET", out.SourceCredentialsInfos[0]["serverType"])
		assert.Equal(t, "PERSONAL_ACCESS_TOKEN", out.SourceCredentialsInfos[0]["authType"])
	})

	t.Run("delete_existing", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		importRec := doRequest(t, h, "ImportSourceCredentials", map[string]any{
			"authType":   "PERSONAL_ACCESS_TOKEN",
			"serverType": "GITHUB",
			"token":      "tok",
		})
		require.Equal(t, http.StatusOK, importRec.Code)

		var importOut struct {
			Arn string `json:"arn"`
		}
		require.NoError(t, json.NewDecoder(importRec.Body).Decode(&importOut))

		delRec := doRequest(t, h, "DeleteSourceCredentials", map[string]any{"arn": importOut.Arn})
		assert.Equal(t, http.StatusOK, delRec.Code)

		// Verify gone.
		listRec := doRequest(t, h, "ListSourceCredentials", nil)
		var listOut struct {
			SourceCredentialsInfos []map[string]any `json:"sourceCredentialsInfos"`
		}
		require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
		assert.Empty(t, listOut.SourceCredentialsInfos)
	})

	t.Run("delete_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "DeleteSourceCredentials", map[string]any{
			"arn": "arn:aws:codebuild:us-east-1:000000000000:token/GHOST",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("delete_missing_arn", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "DeleteSourceCredentials", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestCodeBuild_ResourcePolicy covers Put, Get, Delete.
func TestCodeBuild_ResourcePolicy(t *testing.T) {
	t.Parallel()

	const resArn = "arn:aws:codebuild:us-east-1:000000000000:report-group/my-rg"
	const policy = `{"Version":"2012-10-17"}`

	t.Run("put_and_get", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		putRec := doRequest(t, h, "PutResourcePolicy", map[string]any{
			"resourceArn": resArn,
			"policy":      policy,
		})
		require.Equal(t, http.StatusOK, putRec.Code)

		var putOut struct {
			ResourceArn string `json:"resourceArn"`
		}
		require.NoError(t, json.NewDecoder(putRec.Body).Decode(&putOut))
		assert.Equal(t, resArn, putOut.ResourceArn)

		getRec := doRequest(t, h, "GetResourcePolicy", map[string]any{"resourceArn": resArn})
		require.Equal(t, http.StatusOK, getRec.Code)

		var getOut struct {
			Policy string `json:"policy"`
		}
		require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
		assert.JSONEq(t, policy, getOut.Policy)
	})

	t.Run("get_missing_returns_empty_json", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "GetResourcePolicy", map[string]any{
			"resourceArn": "arn:aws:codebuild:us-east-1:000000000000:report-group/ghost",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Policy string `json:"policy"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Equal(t, "{}", out.Policy)
	})

	t.Run("delete_idempotent", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "PutResourcePolicy", map[string]any{
			"resourceArn": resArn,
			"policy":      policy,
		})

		del1 := doRequest(t, h, "DeleteResourcePolicy", map[string]any{"resourceArn": resArn})
		assert.Equal(t, http.StatusOK, del1.Code)

		del2 := doRequest(t, h, "DeleteResourcePolicy", map[string]any{"resourceArn": resArn})
		assert.Equal(t, http.StatusOK, del2.Code)

		// Policy should now return default.
		getRec := doRequest(t, h, "GetResourcePolicy", map[string]any{"resourceArn": resArn})
		var out struct {
			Policy string `json:"policy"`
		}
		require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
		assert.Equal(t, "{}", out.Policy)
	})

	t.Run("put_missing_fields", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "PutResourcePolicy", map[string]any{"resourceArn": resArn})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("get_missing_resource_arn", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "GetResourcePolicy", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestCodeBuild_Reports covers DeleteReport, ListReports, ListReportsForReportGroup.
func TestCodeBuild_Reports(t *testing.T) {
	t.Parallel()

	t.Run("list_reports_empty", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ListReports", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Reports []string `json:"reports"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Empty(t, out.Reports)
	})

	t.Run("list_reports_after_seed", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		h.Backend.AddReportInternal(&codebuild.Report{
			Arn:    "arn:aws:codebuild:us-east-1:000000000000:report/rg1:r1",
			Status: "SUCCEEDED",
		})
		h.Backend.AddReportInternal(&codebuild.Report{
			Arn:    "arn:aws:codebuild:us-east-1:000000000000:report/rg1:r2",
			Status: "FAILED",
		})

		rec := doRequest(t, h, "ListReports", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Reports []string `json:"reports"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Len(t, out.Reports, 2)
	})

	t.Run("delete_report", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		reportArn := "arn:aws:codebuild:us-east-1:000000000000:report/rg1:del1"
		h.Backend.AddReportInternal(&codebuild.Report{
			Arn:    reportArn,
			Status: "SUCCEEDED",
		})

		delRec := doRequest(t, h, "DeleteReport", map[string]any{"arn": reportArn})
		assert.Equal(t, http.StatusOK, delRec.Code)

		// Verify gone.
		listRec := doRequest(t, h, "ListReports", nil)
		var out struct {
			Reports []string `json:"reports"`
		}
		require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
		assert.Empty(t, out.Reports)
	})

	t.Run("delete_report_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "DeleteReport", map[string]any{
			"arn": "arn:aws:codebuild:us-east-1:000000000000:report/rg1:ghost",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("list_reports_for_report_group", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rgArn := "arn:aws:codebuild:us-east-1:000000000000:report-group/my-rg"

		h.Backend.AddReportInternal(&codebuild.Report{
			Arn:            "arn:aws:codebuild:us-east-1:000000000000:report/my-rg:r1",
			ReportGroupArn: rgArn,
			Status:         "SUCCEEDED",
		})
		h.Backend.AddReportInternal(&codebuild.Report{
			Arn:            "arn:aws:codebuild:us-east-1:000000000000:report/my-rg:r2",
			ReportGroupArn: rgArn,
			Status:         "FAILED",
		})
		// Different group.
		h.Backend.AddReportInternal(&codebuild.Report{
			Arn:            "arn:aws:codebuild:us-east-1:000000000000:report/other-rg:r1",
			ReportGroupArn: "arn:aws:codebuild:us-east-1:000000000000:report-group/other-rg",
			Status:         "SUCCEEDED",
		})

		rec := doRequest(t, h, "ListReportsForReportGroup", map[string]any{
			"reportGroupArn": rgArn,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Reports []string `json:"reports"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Len(t, out.Reports, 2)
		for _, r := range out.Reports {
			assert.Contains(t, r, "my-rg")
		}
	})
}

// TestCodeBuild_ReportGroups covers DeleteReportGroup, UpdateReportGroup.
func TestCodeBuild_ReportGroups(t *testing.T) {
	t.Parallel()

	t.Run("delete_report_group", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createRec := doRequest(t, h, "CreateReportGroup", map[string]any{
			"name": "rg-to-delete",
			"type": "TEST",
		})
		require.Equal(t, http.StatusOK, createRec.Code)

		var createOut struct {
			ReportGroup struct {
				Arn string `json:"arn"`
			} `json:"reportGroup"`
		}
		require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
		rgArn := createOut.ReportGroup.Arn

		delRec := doRequest(t, h, "DeleteReportGroup", map[string]any{"arn": rgArn})
		assert.Equal(t, http.StatusOK, delRec.Code)

		// Verify gone.
		batchRec := doRequest(t, h, "BatchGetReportGroups", map[string]any{
			"reportGroupArns": []string{rgArn},
		})
		var batchOut struct {
			ReportGroupsNotFound []string `json:"reportGroupsNotFound"`
		}
		require.NoError(t, json.NewDecoder(batchRec.Body).Decode(&batchOut))
		assert.Len(t, batchOut.ReportGroupsNotFound, 1)
	})

	t.Run("delete_report_group_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "DeleteReportGroup", map[string]any{
			"arn": "arn:aws:codebuild:us-east-1:000000000000:report-group/ghost",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("update_report_group", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createRec := doRequest(t, h, "CreateReportGroup", map[string]any{
			"name": "rg-to-update",
			"type": "TEST",
			"exportConfig": map[string]any{
				"exportConfigType": "NO_EXPORT",
			},
		})
		require.Equal(t, http.StatusOK, createRec.Code)

		var createOut struct {
			ReportGroup struct {
				Arn string `json:"arn"`
			} `json:"reportGroup"`
		}
		require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
		rgArn := createOut.ReportGroup.Arn

		updateRec := doRequest(t, h, "UpdateReportGroup", map[string]any{
			"arn": rgArn,
			"exportConfig": map[string]any{
				"exportConfigType": "S3",
			},
		})
		require.Equal(t, http.StatusOK, updateRec.Code)

		var updateOut struct {
			ReportGroup struct {
				ExportConfig struct {
					ExportConfigType string `json:"exportConfigType"`
				} `json:"exportConfig"`
			} `json:"reportGroup"`
		}
		require.NoError(t, json.NewDecoder(updateRec.Body).Decode(&updateOut))
		assert.Equal(t, "S3", updateOut.ReportGroup.ExportConfig.ExportConfigType)
	})

	t.Run("update_report_group_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "UpdateReportGroup", map[string]any{
			"arn": "arn:aws:codebuild:us-east-1:000000000000:report-group/ghost",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestCodeBuild_Webhooks covers DeleteWebhook, UpdateWebhook.
func TestCodeBuild_Webhooks(t *testing.T) {
	t.Parallel()

	t.Run("delete_webhook", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "wh-del-proj")
		doRequest(t, h, "CreateWebhook", map[string]any{
			"projectName":  "wh-del-proj",
			"branchFilter": "main",
		})

		delRec := doRequest(t, h, "DeleteWebhook", map[string]any{"projectName": "wh-del-proj"})
		assert.Equal(t, http.StatusOK, delRec.Code)
	})

	t.Run("delete_webhook_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "DeleteWebhook", map[string]any{"projectName": "ghost-proj"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("update_webhook", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "wh-upd-proj")
		doRequest(t, h, "CreateWebhook", map[string]any{
			"projectName":  "wh-upd-proj",
			"branchFilter": "main",
			"buildType":    "BUILD",
		})

		updRec := doRequest(t, h, "UpdateWebhook", map[string]any{
			"projectName":  "wh-upd-proj",
			"branchFilter": "develop",
			"buildType":    "BUILD_BATCH",
		})
		require.Equal(t, http.StatusOK, updRec.Code)

		var out struct {
			Webhook struct {
				BranchFilter string `json:"branchFilter"`
				BuildType    string `json:"buildType"`
			} `json:"webhook"`
		}
		require.NoError(t, json.NewDecoder(updRec.Body).Decode(&out))
		assert.Equal(t, "develop", out.Webhook.BranchFilter)
		assert.Equal(t, "BUILD_BATCH", out.Webhook.BuildType)
	})

	t.Run("update_webhook_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "UpdateWebhook", map[string]any{
			"projectName":  "ghost-proj",
			"branchFilter": "main",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestCodeBuild_Fleet covers UpdateFleet.
func TestCodeBuild_Fleet(t *testing.T) {
	t.Parallel()

	t.Run("update_fleet", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createRec := doRequest(t, h, "CreateFleet", map[string]any{
			"name":         "upd-fleet",
			"baseCapacity": 2,
		})
		require.Equal(t, http.StatusOK, createRec.Code)

		var createOut struct {
			Fleet struct {
				Arn          string `json:"arn"`
				BaseCapacity int    `json:"baseCapacity"`
			} `json:"fleet"`
		}
		require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
		fleetArn := createOut.Fleet.Arn

		updRec := doRequest(t, h, "UpdateFleet", map[string]any{
			"arn":          fleetArn,
			"baseCapacity": 5,
		})
		require.Equal(t, http.StatusOK, updRec.Code)

		var updOut struct {
			Fleet struct {
				BaseCapacity int `json:"baseCapacity"`
			} `json:"fleet"`
		}
		require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updOut))
		assert.Equal(t, 5, updOut.Fleet.BaseCapacity)
	})

	t.Run("update_fleet_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "UpdateFleet", map[string]any{
			"arn":          "arn:aws:codebuild:us-east-1:000000000000:fleet/ghost",
			"baseCapacity": 1,
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestCodeBuild_BuildBatch covers RetryBuildBatch, StopBuildBatch, ListBuildBatchesForProject.
func TestCodeBuild_BuildBatch(t *testing.T) {
	t.Parallel()

	t.Run("stop_build_batch", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "batch-proj")

		startRec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-proj"})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			BuildBatch struct {
				ID string `json:"id"`
			} `json:"buildBatch"`
		}
		require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
		batchID := startOut.BuildBatch.ID

		stopRec := doRequest(t, h, "StopBuildBatch", map[string]any{"id": batchID})
		require.Equal(t, http.StatusOK, stopRec.Code)

		var stopOut struct {
			BuildBatch struct {
				BuildBatchStatus string `json:"buildBatchStatus"`
			} `json:"buildBatch"`
		}
		require.NoError(t, json.NewDecoder(stopRec.Body).Decode(&stopOut))
		assert.Equal(t, "STOPPED", stopOut.BuildBatch.BuildBatchStatus)
	})

	t.Run("stop_build_batch_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "StopBuildBatch", map[string]any{"id": "ghost-proj:ghost-batch"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("retry_build_batch", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "batch-retry-proj")

		startRec := doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-retry-proj"})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			BuildBatch struct {
				ID string `json:"id"`
			} `json:"buildBatch"`
		}
		require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
		batchID := startOut.BuildBatch.ID

		retryRec := doRequest(t, h, "RetryBuildBatch", map[string]any{"id": batchID})
		require.Equal(t, http.StatusOK, retryRec.Code)

		var retryOut struct {
			BuildBatch struct {
				ID               string `json:"id"`
				BuildBatchStatus string `json:"buildBatchStatus"`
				ProjectName      string `json:"projectName"`
			} `json:"buildBatch"`
		}
		require.NoError(t, json.NewDecoder(retryRec.Body).Decode(&retryOut))
		assert.NotEqual(t, batchID, retryOut.BuildBatch.ID)
		assert.Equal(t, "IN_PROGRESS", retryOut.BuildBatch.BuildBatchStatus)
		assert.Equal(t, "batch-retry-proj", retryOut.BuildBatch.ProjectName)
	})

	t.Run("retry_build_batch_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "RetryBuildBatch", map[string]any{"id": "ghost-proj:ghost"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("list_build_batches_for_project", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "batch-list-proj")

		// Start 2 batches.
		doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-list-proj"})
		doRequest(t, h, "StartBuildBatch", map[string]any{"projectName": "batch-list-proj"})

		listRec := doRequest(t, h, "ListBuildBatchesForProject", map[string]any{
			"projectName": "batch-list-proj",
		})
		require.Equal(t, http.StatusOK, listRec.Code)

		var out struct {
			IDs []string `json:"ids"`
		}
		require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
		assert.Len(t, out.IDs, 2)
	})

	t.Run("list_build_batches_for_project_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ListBuildBatchesForProject", map[string]any{
			"projectName": "ghost-proj",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestCodeBuild_Sandbox covers StopSandbox, ListSandboxesForProject, ListCommandExecutionsForSandbox.
func TestCodeBuild_Sandbox(t *testing.T) {
	t.Parallel()

	t.Run("stop_sandbox", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "sandbox-proj")

		startRec := doRequest(t, h, "StartSandbox", map[string]any{"projectName": "sandbox-proj"})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			Sandbox struct {
				ID string `json:"id"`
			} `json:"sandbox"`
		}
		require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
		sandboxID := startOut.Sandbox.ID

		stopRec := doRequest(t, h, "StopSandbox", map[string]any{"id": sandboxID})
		require.Equal(t, http.StatusOK, stopRec.Code)

		var stopOut struct {
			Sandbox struct {
				Status string `json:"status"`
			} `json:"sandbox"`
		}
		require.NoError(t, json.NewDecoder(stopRec.Body).Decode(&stopOut))
		assert.Equal(t, "STOPPED", stopOut.Sandbox.Status)
	})

	t.Run("stop_sandbox_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "StopSandbox", map[string]any{"id": "ghost-sandbox"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("list_sandboxes_for_project", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "sandbox-list-proj")

		doRequest(t, h, "StartSandbox", map[string]any{"projectName": "sandbox-list-proj"})
		doRequest(t, h, "StartSandbox", map[string]any{"projectName": "sandbox-list-proj"})

		listRec := doRequest(t, h, "ListSandboxesForProject", map[string]any{
			"projectName": "sandbox-list-proj",
		})
		require.Equal(t, http.StatusOK, listRec.Code)

		var out struct {
			IDs []string `json:"ids"`
		}
		require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
		assert.Len(t, out.IDs, 2)
	})

	t.Run("list_sandboxes_for_project_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ListSandboxesForProject", map[string]any{
			"projectName": "ghost-proj",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("list_command_executions_for_sandbox", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "ce-proj")

		startRec := doRequest(t, h, "StartSandbox", map[string]any{"projectName": "ce-proj"})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			Sandbox struct {
				ID string `json:"id"`
			} `json:"sandbox"`
		}
		require.NoError(t, json.NewDecoder(startRec.Body).Decode(&startOut))
		sandboxID := startOut.Sandbox.ID

		doRequest(t, h, "StartCommandExecution", map[string]any{
			"sandboxId": sandboxID,
			"command":   "echo hello",
			"type":      "COMMAND",
		})
		doRequest(t, h, "StartCommandExecution", map[string]any{
			"sandboxId": sandboxID,
			"command":   "echo world",
			"type":      "COMMAND",
		})

		listRec := doRequest(t, h, "ListCommandExecutionsForSandbox", map[string]any{
			"sandboxId": sandboxID,
		})
		require.Equal(t, http.StatusOK, listRec.Code)

		var out struct {
			CommandExecutions []string `json:"commandExecutions"`
		}
		require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
		assert.Len(t, out.CommandExecutions, 2)
	})

	t.Run("list_command_executions_for_sandbox_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ListCommandExecutionsForSandbox", map[string]any{
			"sandboxId": "ghost-sandbox",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestCodeBuild_Project covers UpdateProjectVisibility, InvalidateProjectCache.
func TestCodeBuild_Project(t *testing.T) {
	t.Parallel()

	t.Run("update_project_visibility", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "vis-proj")
		arn := projectARN(t, h, "vis-proj")

		rec := doRequest(t, h, "UpdateProjectVisibility", map[string]any{
			"projectArn":        arn,
			"projectVisibility": "PUBLIC_READ",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			ProjectArn        string `json:"projectArn"`
			ProjectVisibility string `json:"projectVisibility"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Equal(t, arn, out.ProjectArn)
		assert.Equal(t, "PUBLIC_READ", out.ProjectVisibility)
	})

	t.Run("update_project_visibility_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "UpdateProjectVisibility", map[string]any{
			"projectArn":        "arn:aws:codebuild:us-east-1:000000000000:project/ghost",
			"projectVisibility": "PRIVATE",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("update_project_visibility_missing_arn", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "UpdateProjectVisibility", map[string]any{
			"projectVisibility": "PRIVATE",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalidate_project_cache", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestProject(t, h, "cache-proj")

		rec := doRequest(t, h, "InvalidateProjectCache", map[string]any{"projectName": "cache-proj"})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("invalidate_project_cache_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "InvalidateProjectCache", map[string]any{"projectName": "ghost-proj"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

// TestCodeBuild_Misc covers DescribeCodeCoverages, DescribeTestCases, GetReportGroupTrend,
// ListSharedProjects, ListSharedReportGroups, ListCuratedEnvironmentImages.
func TestCodeBuild_Misc(t *testing.T) {
	t.Parallel()

	t.Run("describe_code_coverages_returns_empty", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "DescribeCodeCoverages", map[string]any{
			"reportArn": "arn:aws:codebuild:us-east-1:000000000000:report/rg:r1",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			CodeCoverages []any `json:"codeCoverages"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Empty(t, out.CodeCoverages)
	})

	t.Run("describe_test_cases_returns_empty", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "DescribeTestCases", map[string]any{
			"reportArn": "arn:aws:codebuild:us-east-1:000000000000:report/rg:r1",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			TestCases []any `json:"testCases"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Empty(t, out.TestCases)
	})

	t.Run("get_report_group_trend_returns_empty_stats", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "GetReportGroupTrend", map[string]any{
			"reportGroupArn": "arn:aws:codebuild:us-east-1:000000000000:report-group/rg",
			"trendField":     "PASS_RATE",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Stats map[string]any `json:"stats"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.NotNil(t, out.Stats)
	})

	t.Run("list_shared_projects_returns_empty", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ListSharedProjects", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Projects []string `json:"projects"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Empty(t, out.Projects)
	})

	t.Run("list_shared_report_groups_returns_empty", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ListSharedReportGroups", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			ReportGroups []string `json:"reportGroups"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Empty(t, out.ReportGroups)
	})

	t.Run("list_curated_environment_images_returns_platforms", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ListCuratedEnvironmentImages", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Platforms []map[string]any `json:"platforms"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		require.NotEmpty(t, out.Platforms)
		assert.Equal(t, "UBUNTU", out.Platforms[0]["platform"])
	})
}

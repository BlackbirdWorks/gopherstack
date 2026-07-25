package backup_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestReportJob(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	job := b.StartReportJob("my-report-plan")
	assert.NotEmpty(t, job.ReportJobID)

	found, err := b.DescribeReportJob(job.ReportJobID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", found.Status)

	jobs := b.ListReportJobs("")
	assert.NotEmpty(t, jobs)
}

func TestReportPlanSubFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantStatus   int
		wantDelivery bool
		wantSetting  bool
	}{
		{
			name: "report_plan_with_delivery_and_setting",
			body: `{
				"ReportPlanName": "rp-full",
				"ReportPlanDescription": "full report",
				"ReportDeliveryChannel": {
					"S3BucketName": "my-reports-bucket",
					"Formats": ["CSV","JSON"]
				},
				"ReportSetting": {
					"ReportTemplate": "BACKUP_JOB_REPORT",
					"FrameworkArns": ["arn:aws:backup:us-east-1:000000000000:framework:fw-1"]
				}
			}`,
			wantStatus:   http.StatusOK,
			wantDelivery: true,
			wantSetting:  true,
		},
		{
			name: "report_plan_minimal",
			body: `{
				"ReportPlanName": "rp-minimal"
			}`,
			wantStatus:   http.StatusOK,
			wantDelivery: false,
			wantSetting:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newHandler(t)

			resp := doRequest(t, h, http.MethodPost, "/audit/report-plans", tt.body)
			require.Equal(t, tt.wantStatus, resp.Code)

			var createData map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &createData))
			rpName := createData["ReportPlanName"].(string)

			// Describe and verify sub-fields.
			getResp := doRequest(t, h, http.MethodGet, "/audit/report-plans/"+rpName, "")
			require.Equal(t, http.StatusOK, getResp.Code)

			var getData map[string]any
			require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &getData))
			rpDoc := getData["ReportPlan"].(map[string]any)

			if tt.wantDelivery {
				ch, ok := rpDoc["ReportDeliveryChannel"].(map[string]any)
				require.True(t, ok, "ReportDeliveryChannel should be present")
				assert.Equal(t, "my-reports-bucket", ch["S3BucketName"])
				formats, ok2 := ch["Formats"].([]any)
				require.True(t, ok2)
				assert.Len(t, formats, 2)
			} else {
				_, hasDelivery := rpDoc["ReportDeliveryChannel"]
				assert.False(t, hasDelivery, "ReportDeliveryChannel should be absent")
			}

			if tt.wantSetting {
				rs, ok := rpDoc["ReportSetting"].(map[string]any)
				require.True(t, ok, "ReportSetting should be present")
				assert.Equal(t, "BACKUP_JOB_REPORT", rs["ReportTemplate"])
				arns, ok2 := rs["FrameworkArns"].([]any)
				require.True(t, ok2)
				assert.Len(t, arns, 1)
			} else {
				_, hasSetting := rpDoc["ReportSetting"]
				assert.False(t, hasSetting, "ReportSetting should be absent")
			}
		})
	}
}

// TestReportPlanFullShape covers the previously-missing ReportDeliveryChannel
// (S3KeyPrefix) and ReportSetting (Accounts/OrganizationUnits/Regions/
// NumberOfFrameworks) fields, plus UpdateReportPlan's ability to replace
// them (previously only ReportPlanDescription was updatable).
func TestReportPlanFullShape(t *testing.T) {
	t.Parallel()
	h, _ := newHandler(t)

	createResp := doRequest(t, h, http.MethodPost, "/audit/report-plans", `{
		"ReportPlanName": "rp-full-shape",
		"ReportDeliveryChannel": {
			"S3BucketName": "my-reports-bucket",
			"S3KeyPrefix": "reports/prefix",
			"Formats": ["CSV"]
		},
		"ReportSetting": {
			"ReportTemplate": "RESOURCE_COMPLIANCE_REPORT",
			"Accounts": ["123456789012"],
			"OrganizationUnits": ["ou-1234-abcd5678"],
			"Regions": ["us-east-1", "us-west-2"],
			"NumberOfFrameworks": 3
		}
	}`)
	require.Equal(t, http.StatusOK, createResp.Code)

	getResp := doRequest(t, h, http.MethodGet, "/audit/report-plans/rp-full-shape", "")
	require.Equal(t, http.StatusOK, getResp.Code)
	var getData map[string]any
	require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &getData))
	rpDoc := getData["ReportPlan"].(map[string]any)

	ch := rpDoc["ReportDeliveryChannel"].(map[string]any)
	assert.Equal(t, "reports/prefix", ch["S3KeyPrefix"])

	rs := rpDoc["ReportSetting"].(map[string]any)
	assert.Equal(t, "RESOURCE_COMPLIANCE_REPORT", rs["ReportTemplate"])
	accounts, ok := rs["Accounts"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"123456789012"}, accounts)
	regions, ok := rs["Regions"].([]any)
	require.True(t, ok)
	assert.Len(t, regions, 2)
	assert.InEpsilon(t, float64(3), rs["NumberOfFrameworks"], 0)

	// UpdateReportPlan can replace the delivery channel and setting, not
	// just the description.
	updateResp := doRequest(t, h, http.MethodPut, "/audit/report-plans/rp-full-shape", `{
		"ReportPlanDescription": "updated",
		"ReportDeliveryChannel": {"S3BucketName": "new-bucket"},
		"ReportSetting": {"ReportTemplate": "COPY_JOB_REPORT"}
	}`)
	require.Equal(t, http.StatusOK, updateResp.Code)

	getResp2 := doRequest(t, h, http.MethodGet, "/audit/report-plans/rp-full-shape", "")
	var getData2 map[string]any
	require.NoError(t, json.Unmarshal(getResp2.Body.Bytes(), &getData2))
	rpDoc2 := getData2["ReportPlan"].(map[string]any)
	assert.Equal(t, "updated", rpDoc2["ReportPlanDescription"])
	ch2 := rpDoc2["ReportDeliveryChannel"].(map[string]any)
	assert.Equal(t, "new-bucket", ch2["S3BucketName"])
	rs2 := rpDoc2["ReportSetting"].(map[string]any)
	assert.Equal(t, "COPY_JOB_REPORT", rs2["ReportTemplate"])
}

// ---- Job progress fields (item 10) ----

// TestCreateReportPlan exercises the report plan creation operation.
func TestCreateReportPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/audit/report-plans", map[string]any{
					"ReportPlanName":        "my-report-plan",
					"ReportPlanDescription": "Daily backup report",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseResp(t, rec)
				assert.Equal(t, "my-report-plan", resp["ReportPlanName"])
				assert.NotEmpty(t, resp["ReportPlanArn"])
				assert.NotNil(t, resp["CreationTime"])
			},
		},
		{
			name: "duplicate_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/audit/report-plans", map[string]any{
					"ReportPlanName": "dup-report",
				})
				rec := doREST(t, h, http.MethodPost, "/audit/report-plans", map[string]any{
					"ReportPlanName": "dup-report",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_name",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/audit/report-plans", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
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

package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

func TestHandler_CreateReportGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"name":         "my-report-group",
				"type":         "TEST",
				"exportConfig": map[string]any{"exportConfigType": "S3"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_name",
			body:       map[string]any{"type": "TEST"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_type",
			body:       map[string]any{"name": "no-type-rg"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate_fails",
			body: map[string]any{
				"name": "dup-rg",
				"type": "CODE_COVERAGE",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_fails" {
				rec := doRequest(t, h, "CreateReportGroup", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateReportGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_BatchGetReportGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupGroups  []string
		wantFound    int
		wantNotFound int
	}{
		{
			name:         "returns_report_group_by_arn",
			setupGroups:  []string{"rg-one"},
			wantFound:    1,
			wantNotFound: 0,
		},
		{
			name:         "not_found",
			setupGroups:  []string{},
			wantFound:    0,
			wantNotFound: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			arns := make([]string, 0)
			for _, gn := range tt.setupGroups {
				rec := doRequest(t, h, "CreateReportGroup", map[string]any{
					"name": gn,
					"type": "TEST",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				rg, _ := out["reportGroup"].(map[string]any)
				arns = append(arns, rg["arn"].(string))
			}

			if len(tt.setupGroups) == 0 {
				arns = []string{"arn:aws:codebuild:us-east-1:000000000000:report-group/ghost"}
			}

			rec := doRequest(t, h, "BatchGetReportGroups", map[string]any{"reportGroupArns": arns})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			groups, _ := out["reportGroups"].([]any)
			assert.Len(t, groups, tt.wantFound)

			notFound, _ := out["reportGroupsNotFound"].([]any)
			assert.Len(t, notFound, tt.wantNotFound)
		})
	}
}

func TestHandler_BatchGetReports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		queryArns    func(arns []string) []string
		name         string
		seedReports  int
		wantFound    int
		wantNotFound int
	}{
		{
			name:         "returns_seeded_report",
			seedReports:  1,
			queryArns:    func(arns []string) []string { return arns },
			wantFound:    1,
			wantNotFound: 0,
		},
		{
			name:        "missing_arn_in_not_found",
			seedReports: 0,
			queryArns: func(_ []string) []string {
				return []string{"arn:aws:codebuild:us-east-1:000000000000:report/ghost:abc"}
			},
			wantFound:    0,
			wantNotFound: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seededArns := make([]string, 0, tt.seedReports)

			for range tt.seedReports {
				reportArn := "arn:aws:codebuild:us-east-1:000000000000:report/my-group:abc123"
				h.Backend.AddReportInternal(&codebuild.Report{
					Arn:    reportArn,
					Status: "SUCCEEDED",
					Type:   "TEST",
				})
				seededArns = append(seededArns, reportArn)
			}

			queryArns := tt.queryArns(seededArns)
			rec := doRequest(t, h, "BatchGetReports", map[string]any{"reportArns": queryArns})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			reports, _ := out["reports"].([]any)
			assert.Len(t, reports, tt.wantFound)

			notFound, _ := out["reportsNotFound"].([]any)
			assert.Len(t, notFound, tt.wantNotFound)
		})
	}
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

// TestCodeBuild_ReportExtras covers DescribeCodeCoverages, DescribeTestCases,
// GetReportGroupTrend, ListSharedReportGroups.
func TestCodeBuild_ReportExtras(t *testing.T) {
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
}

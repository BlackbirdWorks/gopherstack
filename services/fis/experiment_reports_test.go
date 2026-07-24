package fis_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fis"
)

// ----------------------------------------
// computeExperimentReport (unit-level, via export)
// ----------------------------------------

func TestComputeExperimentReport(t *testing.T) {
	t.Parallel()

	s3Config := &fis.ExperimentReportConfiguration{
		Outputs: &fis.ExperimentReportConfigurationOutputs{
			S3Configuration: &fis.ExperimentReportConfigurationOutputsS3Configuration{
				BucketName: "my-fis-reports",
				Prefix:     "reports/",
			},
		},
	}

	tests := []struct {
		cfg            *fis.ExperimentReportConfiguration
		name           string
		terminalStatus string
		wantStatus     string
		wantErrorCode  string
		wantS3Reports  bool
	}{
		{
			name:           "completed_experiment_with_s3_output_generates_report",
			cfg:            s3Config,
			terminalStatus: "completed",
			wantStatus:     "completed",
			wantS3Reports:  true,
		},
		{
			name:           "stopped_experiment_with_s3_output_still_generates_report",
			cfg:            s3Config,
			terminalStatus: "stopped",
			wantStatus:     "completed",
			wantS3Reports:  true,
		},
		{
			name:           "missing_s3_output_fails_report_generation",
			cfg:            &fis.ExperimentReportConfiguration{},
			terminalStatus: "completed",
			wantStatus:     "failed",
			wantErrorCode:  "MissingReportOutputConfiguration",
		},
		{
			name:           "cancelled_experiment_cancels_report_even_with_valid_output",
			cfg:            s3Config,
			terminalStatus: "cancelled",
			wantStatus:     "cancelled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			report := fis.ComputeExperimentReportForTest(tt.cfg, "EXPtest1234567890", tt.terminalStatus)
			require.NotNil(t, report)
			require.NotNil(t, report.State)
			assert.Equal(t, tt.wantStatus, report.State.Status)

			if tt.wantErrorCode != "" {
				require.NotNil(t, report.State.Error)
				assert.Equal(t, tt.wantErrorCode, report.State.Error.Code)
			}

			if tt.wantS3Reports {
				require.Len(t, report.S3Reports, 1)
				assert.Equal(t, "experiment-report", report.S3Reports[0].ReportType)
				assert.True(t, strings.HasPrefix(report.S3Reports[0].Arn, "arn:aws:s3:::my-fis-reports/reports/"))
			} else {
				assert.Empty(t, report.S3Reports)
			}
		})
	}
}

// ----------------------------------------
// Template experimentReportConfiguration CRUD
// ----------------------------------------

func reportConfigBody() map[string]any {
	return map[string]any{
		"dataSources": map[string]any{
			"cloudWatchDashboards": []map[string]any{
				{"dashboardIdentifier": "arn:aws:cloudwatch::000000000000:dashboard/MyDashboard"},
			},
		},
		"outputs": map[string]any{
			"s3Configuration": map[string]any{
				"bucketName": "my-fis-reports",
				"prefix":     "reports/",
			},
		},
		"preExperimentDuration":  "PT5M",
		"postExperimentDuration": "PT10M",
	}
}

func TestCreateExperimentTemplate_WithReportConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := minimalTemplateBody()
	body["experimentReportConfiguration"] = reportConfigBody()

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		ExperimentTemplate struct {
			ID                            string `json:"id"`
			ExperimentReportConfiguration struct {
				Outputs struct {
					S3Configuration struct {
						BucketName string `json:"bucketName"`
						Prefix     string `json:"prefix"`
					} `json:"s3Configuration"`
				} `json:"outputs"`
				PreExperimentDuration  string `json:"preExperimentDuration"`
				PostExperimentDuration string `json:"postExperimentDuration"`
				DataSources            struct {
					CloudWatchDashboards []struct {
						DashboardIdentifier string `json:"dashboardIdentifier"`
					} `json:"cloudWatchDashboards"`
				} `json:"dataSources"`
			} `json:"experimentReportConfiguration"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &resp)

	cfg := resp.ExperimentTemplate.ExperimentReportConfiguration
	require.Len(t, cfg.DataSources.CloudWatchDashboards, 1)
	assert.Equal(t,
		"arn:aws:cloudwatch::000000000000:dashboard/MyDashboard",
		cfg.DataSources.CloudWatchDashboards[0].DashboardIdentifier,
	)
	assert.Equal(t, "my-fis-reports", cfg.Outputs.S3Configuration.BucketName)
	assert.Equal(t, "reports/", cfg.Outputs.S3Configuration.Prefix)
	assert.Equal(t, "PT5M", cfg.PreExperimentDuration)
	assert.Equal(t, "PT10M", cfg.PostExperimentDuration)

	// GET round-trips the same configuration.
	rec2 := doRequest(t, h, http.MethodGet, "/experimentTemplates/"+resp.ExperimentTemplate.ID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var getResp struct {
		ExperimentTemplate struct {
			ExperimentReportConfiguration struct {
				PreExperimentDuration string `json:"preExperimentDuration"`
			} `json:"experimentReportConfiguration"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec2, &getResp)
	assert.Equal(t, "PT5M", getResp.ExperimentTemplate.ExperimentReportConfiguration.PreExperimentDuration)
}

func TestCreateExperimentTemplate_ReportConfiguration_InvalidDuration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := minimalTemplateBody()
	body["experimentReportConfiguration"] = map[string]any{
		"preExperimentDuration": "not-a-duration",
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}

	mustJSON(t, rec, &errResp)
	assert.Equal(t, "ValidationException", errResp.Type)
}

func TestUpdateExperimentTemplate_ReplacesReportConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := minimalTemplateBody()
	body["experimentReportConfiguration"] = reportConfigBody()

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &createResp)

	updateBody := map[string]any{
		"experimentReportConfiguration": map[string]any{
			"outputs": map[string]any{
				"s3Configuration": map[string]any{
					"bucketName": "a-different-bucket",
				},
			},
		},
	}

	rec2 := doRequest(t, h, http.MethodPatch, "/experimentTemplates/"+createResp.ExperimentTemplate.ID, updateBody)
	require.Equal(t, http.StatusOK, rec2.Code)

	var updateResp struct {
		ExperimentTemplate struct {
			ExperimentReportConfiguration struct {
				DataSources *struct {
					CloudWatchDashboards []struct {
						DashboardIdentifier string `json:"dashboardIdentifier"`
					} `json:"cloudWatchDashboards"`
				} `json:"dataSources"`
				Outputs struct {
					S3Configuration struct {
						BucketName string `json:"bucketName"`
					} `json:"s3Configuration"`
				} `json:"outputs"`
			} `json:"experimentReportConfiguration"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec2, &updateResp)

	gotCfg := updateResp.ExperimentTemplate.ExperimentReportConfiguration
	assert.Equal(t, "a-different-bucket", gotCfg.Outputs.S3Configuration.BucketName)
	// Update replaces the whole block wholesale -- the old dataSources is gone.
	assert.Nil(t, gotCfg.DataSources)
}

// ----------------------------------------
// Running-experiment report generation
// ----------------------------------------

type experimentReportPollResult struct {
	Status struct {
		Status string `json:"status"`
	} `json:"status"`
	ExperimentReport struct {
		State struct {
			Error *struct {
				Code string `json:"code"`
			} `json:"error"`
			Status string `json:"status"`
		} `json:"state"`
		S3Reports []struct {
			Arn        string `json:"arn"`
			ReportType string `json:"reportType"`
		} `json:"s3Reports"`
	} `json:"experimentReport"`
}

func startExperimentAndPollTerminal(t *testing.T, h *fis.Handler, templateID string) experimentReportPollResult {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": templateID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var startResp struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	mustJSON(t, rec, &startResp)

	var result experimentReportPollResult

	require.Eventually(t, func() bool {
		r := doRequest(t, h, http.MethodGet, "/experiments/"+startResp.Experiment.ID, nil)
		if r.Code != http.StatusOK {
			return false
		}

		var full struct {
			Experiment experimentReportPollResult `json:"experiment"`
		}

		if err := json.Unmarshal(r.Body.Bytes(), &full); err != nil {
			return false
		}

		if full.Experiment.Status.Status != "completed" && full.Experiment.Status.Status != "failed" &&
			full.Experiment.Status.Status != "stopped" {
			return false
		}

		result = full.Experiment

		return true
	}, 5*time.Second, 20*time.Millisecond)

	return result
}

func TestStartExperiment_WithReportConfiguration_GeneratesReport(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := minimalTemplateBody()
	body["experimentReportConfiguration"] = reportConfigBody()

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	result := startExperimentAndPollTerminal(t, h, tplResp.ExperimentTemplate.ID)

	assert.Equal(t, "completed", result.ExperimentReport.State.Status)
	require.Len(t, result.ExperimentReport.S3Reports, 1)
	assert.Equal(t, "experiment-report", result.ExperimentReport.S3Reports[0].ReportType)
	assert.True(t, strings.HasPrefix(result.ExperimentReport.S3Reports[0].Arn, "arn:aws:s3:::my-fis-reports/reports/"))
}

func TestStartExperiment_ReportConfiguration_MissingS3Output_ReportFails(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := minimalTemplateBody()
	body["experimentReportConfiguration"] = map[string]any{
		"dataSources": map[string]any{
			"cloudWatchDashboards": []map[string]any{
				{"dashboardIdentifier": "arn:aws:cloudwatch::000000000000:dashboard/MyDashboard"},
			},
		},
	}

	rec := doRequest(t, h, http.MethodPost, "/experimentTemplates", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var tplResp struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	mustJSON(t, rec, &tplResp)

	result := startExperimentAndPollTerminal(t, h, tplResp.ExperimentTemplate.ID)

	assert.Equal(t, "failed", result.ExperimentReport.State.Status)
	require.NotNil(t, result.ExperimentReport.State.Error)
	assert.Equal(t, "MissingReportOutputConfiguration", result.ExperimentReport.State.Error.Code)
	assert.Empty(t, result.ExperimentReport.S3Reports)
}

func TestGetExperiment_NoReportConfig_OmitsReportFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tplID := seedTemplate(t, h)

	rec := doRequest(t, h, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": tplID,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var raw map[string]json.RawMessage

	mustJSON(t, rec, &raw)

	var exp map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(raw["experiment"], &exp))

	_, hasReportConfig := exp["experimentReportConfiguration"]
	_, hasReport := exp["experimentReport"]
	assert.False(t, hasReportConfig, "experimentReportConfiguration must be omitted when the template has none")
	assert.False(t, hasReport, "experimentReport must be omitted when the template has none")
}

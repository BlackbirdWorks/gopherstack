package fis

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestComputeExperimentReport whitebox-tests the unexported
// computeExperimentReport directly.
func TestComputeExperimentReport(t *testing.T) {
	t.Parallel()

	s3Config := &ExperimentReportConfiguration{
		Outputs: &ExperimentReportConfigurationOutputs{
			S3Configuration: &ExperimentReportConfigurationOutputsS3Configuration{
				BucketName: "my-fis-reports",
				Prefix:     "reports/",
			},
		},
	}

	tests := []struct {
		cfg            *ExperimentReportConfiguration
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
			cfg:            &ExperimentReportConfiguration{},
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

			report := computeExperimentReport(tt.cfg, "EXPtest1234567890", tt.terminalStatus)
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

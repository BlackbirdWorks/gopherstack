package backup_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReportPlanFlow_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run     func(t *testing.T, c *backupsdk.Client) error
		name    string
		wantErr string
	}{
		{
			name:    "unknown_template",
			wantErr: "InvalidParameterValueException",
			run: func(t *testing.T, c *backupsdk.Client) error {
				t.Helper()

				_, err := c.CreateReportPlan(t.Context(), &backupsdk.CreateReportPlanInput{
					ReportPlanName:        aws.String("rp"),
					ReportDeliveryChannel: &types.ReportDeliveryChannel{S3BucketName: aws.String("b")},
					ReportSetting:         &types.ReportSetting{ReportTemplate: aws.String("NOPE")},
				})

				return err
			},
		},
		{
			name: "known_template",
			run: func(t *testing.T, c *backupsdk.Client) error {
				t.Helper()

				_, err := c.CreateReportPlan(t.Context(), &backupsdk.CreateReportPlanInput{
					ReportPlanName:        aws.String("rp"),
					ReportDeliveryChannel: &types.ReportDeliveryChannel{S3BucketName: aws.String("b")},
					ReportSetting:         &types.ReportSetting{ReportTemplate: aws.String("COPY_JOB_REPORT")},
				})

				return err
			},
		},
		{
			name:    "start_job_unknown_plan",
			wantErr: "ResourceNotFoundException",
			run: func(t *testing.T, c *backupsdk.Client) error {
				t.Helper()

				_, err := c.StartReportJob(
					t.Context(),
					&backupsdk.StartReportJobInput{ReportPlanName: aws.String("nope")},
				)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClient(t)
			err := tt.run(t, client)

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantErr, apiErr.ErrorCode())
		})
	}
}

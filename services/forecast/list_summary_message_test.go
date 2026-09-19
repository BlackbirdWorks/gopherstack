package forecast_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListDatasetImportJobs_Message_RealClient covers gopherstack-21my:
// DatasetImportJobSummary.Message (forecast@v1.44.4
// api_op_ListDatasetImportJobs.go) is a real per-item field that
// summaryOutput (handler.go) never emitted -- unlike resourceOutput, used by
// Describe, which does. A CREATE_FAILED job's Message was visible via
// DescribeDatasetImportJob (fixed by an earlier pass, gopherstack-6flj) but
// silently dropped from the same job's entry in ListDatasetImportJobs.
func TestListDatasetImportJobs_Message_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "create_failed job surfaces message", path: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			client := newTestForecastClient(t, h)
			ctx := t.Context()

			schema := &types.Schema{Attributes: []types.SchemaAttribute{
				{AttributeName: aws.String("item_id"), AttributeType: types.AttributeTypeString},
			}}
			ds, err := client.CreateDataset(ctx, &forecastsdk.CreateDatasetInput{
				DatasetName: aws.String("list_msg_ds"),
				DatasetType: types.DatasetTypeTargetTimeSeries,
				Domain:      types.DomainRetail,
				Schema:      schema,
			})
			require.NoError(t, err)

			job, err := client.CreateDatasetImportJob(ctx, &forecastsdk.CreateDatasetImportJobInput{
				DatasetImportJobName: aws.String("list_msg_job"),
				DatasetArn:           ds.DatasetArn,
				DataSource: &types.DataSource{
					S3Config: &types.S3Config{
						Path:    aws.String(tt.path),
						RoleArn: aws.String("arn:aws:iam::000000000000:role/forecast"),
					},
				},
			})
			require.NoError(t, err)

			out, err := client.ListDatasetImportJobs(ctx, &forecastsdk.ListDatasetImportJobsInput{})
			require.NoError(t, err)
			require.Len(t, out.DatasetImportJobs, 1)

			item := out.DatasetImportJobs[0]
			require.Equal(t, aws.ToString(job.DatasetImportJobArn), aws.ToString(item.DatasetImportJobArn))
			require.Equal(t, "CREATE_FAILED", aws.ToString(item.Status))
			assert.NotEmpty(t, aws.ToString(item.Message),
				"a real client's per-item Message must explain the CREATE_FAILED status")
		})
	}
}

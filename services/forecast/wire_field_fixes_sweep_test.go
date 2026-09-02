package forecast_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeDatasetImportJob_Message_RealClient covers gopherstack-6flj:
// DescribeDatasetImportJobOutput.Message ("If an error occurred, an
// informational message about the error", forecast@v1.44.4
// api_op_DescribeDatasetImportJob.go) is a real member the emulator never
// emitted at all -- Resource had no Message field and resourceOutput never
// set the "Message" key, so a real client's Message was always nil even for
// the one reachable failure path this backend models: CreateDatasetImportJob
// with a DataSource.S3Config missing Path (handler.go's createFails). Every
// other Describe*Output.Message in this service is unreachable (no other
// kind's createFails path exists), so only this one is fixed; the rest are
// recorded as gaps in PARITY.md.
func TestDescribeDatasetImportJob_Message_RealClient(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newTestForecastClient(t, h)
	ctx := t.Context()

	schema := &types.Schema{Attributes: []types.SchemaAttribute{
		{AttributeName: aws.String("item_id"), AttributeType: types.AttributeTypeString},
	}}
	ds, err := client.CreateDataset(ctx, &forecastsdk.CreateDatasetInput{
		DatasetName: aws.String("msg_ds"),
		DatasetType: types.DatasetTypeTargetTimeSeries,
		Domain:      types.DomainRetail,
		Schema:      schema,
	})
	require.NoError(t, err)

	job, err := client.CreateDatasetImportJob(ctx, &forecastsdk.CreateDatasetImportJobInput{
		DatasetImportJobName: aws.String("msg_job"),
		DatasetArn:           ds.DatasetArn,
		DataSource: &types.DataSource{
			S3Config: &types.S3Config{
				Path:    aws.String(""),
				RoleArn: aws.String("arn:aws:iam::000000000000:role/forecast"),
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeDatasetImportJob(ctx, &forecastsdk.DescribeDatasetImportJobInput{
		DatasetImportJobArn: job.DatasetImportJobArn,
	})
	require.NoError(t, err)
	require.Equal(t, "CREATE_FAILED", aws.ToString(out.Status))
	assert.NotEmpty(t, aws.ToString(out.Message), "a real client's Message must explain the CREATE_FAILED status")
}

// TestDescribeMonitor_LastEvaluation_RealClient covers gopherstack-6flj:
// DescribeMonitorOutput.LastEvaluationState/LastEvaluationTime
// (forecast@v1.44.4 api_op_DescribeMonitor.go) are real members the emulator
// never surfaced, even though the backend already tracks exactly this data:
// CreateMonitor synthesizes one MonitorEvaluation (store.go's newEvaluation,
// EvaluationState "SUCCESS") that ListMonitorEvaluations already returns
// correctly -- DescribeMonitor just never read it back.
func TestDescribeMonitor_LastEvaluation_RealClient(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newTestForecastClient(t, h)
	ctx := t.Context()

	dsGroupOut, err := client.CreateDatasetGroup(ctx, &forecastsdk.CreateDatasetGroupInput{
		DatasetGroupName: aws.String("mon-dsg"),
		Domain:           types.DomainCustom,
	})
	require.NoError(t, err)

	predOut, err := client.CreatePredictor(ctx, &forecastsdk.CreatePredictorInput{
		PredictorName:   aws.String("mon-pred"),
		ForecastHorizon: aws.Int32(7),
		ForecastTypes:   []string{"0.5"},
		InputDataConfig: &types.InputDataConfig{
			DatasetGroupArn: dsGroupOut.DatasetGroupArn,
		},
		FeaturizationConfig: &types.FeaturizationConfig{
			ForecastFrequency: aws.String("D"),
		},
	})
	require.NoError(t, err)

	monOut, err := client.CreateMonitor(ctx, &forecastsdk.CreateMonitorInput{
		MonitorName: aws.String("mon1"),
		ResourceArn: predOut.PredictorArn,
	})
	require.NoError(t, err)

	out, err := client.DescribeMonitor(ctx, &forecastsdk.DescribeMonitorInput{
		MonitorArn: monOut.MonitorArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "SUCCESS", aws.ToString(out.LastEvaluationState))
	assert.NotNil(t, out.LastEvaluationTime)
}

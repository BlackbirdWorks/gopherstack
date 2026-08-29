package forecast_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"
	"github.com/aws/aws-sdk-go-v2/service/forecast/types"
	"github.com/stretchr/testify/require"
)

// sdkDatasetGroup creates a minimal DatasetGroup through the real SDK client
// and returns its ARN.
func sdkDatasetGroup(t *testing.T, client *forecastsdk.Client, name string) string {
	t.Helper()

	out, err := client.CreateDatasetGroup(t.Context(), &forecastsdk.CreateDatasetGroupInput{
		DatasetGroupName: aws.String(name),
		Domain:           types.DomainRetail,
	})
	require.NoError(t, err)

	return aws.ToString(out.DatasetGroupArn)
}

// sdkPredictor creates a minimal Predictor through the real SDK client and
// returns its ARN.
func sdkPredictor(t *testing.T, client *forecastsdk.Client, name, datasetGroupARN string) string {
	t.Helper()

	out, err := client.CreatePredictor(t.Context(), &forecastsdk.CreatePredictorInput{
		PredictorName:   aws.String(name),
		ForecastHorizon: aws.Int32(1),
		InputDataConfig: &types.InputDataConfig{DatasetGroupArn: aws.String(datasetGroupARN)},
		FeaturizationConfig: &types.FeaturizationConfig{
			ForecastFrequency: aws.String("D"),
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.PredictorArn)
}

// TestListPredictors_StatusFilter proves ListPredictors applies its Filters
// parameter (Key "Status", per aws-sdk-go-v2/service/forecast@v1.44.4's
// api_op_ListPredictors.go doc comment) instead of returning every predictor
// regardless of the filter, as handler.go's listOutput did before this fix
// (it read only MaxResults/NextToken from the request map, never Filters).
func TestListPredictors_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newTestForecastClient(t, h)

	dg := sdkDatasetGroup(t, client, "status-filter-dg")
	activeARN := sdkPredictor(t, client, "status-filter-active", dg)
	stoppedARN := sdkPredictor(t, client, "status-filter-stopped", dg)

	_, err := client.StopResource(t.Context(), &forecastsdk.StopResourceInput{ResourceArn: aws.String(stoppedARN)})
	require.NoError(t, err)

	tests := []struct {
		name      string
		condition types.FilterConditionString
		value     string
		want      []string
	}{
		{
			name:      "IS CREATE_PENDING excludes stopped",
			condition: types.FilterConditionStringIs,
			value:     "CREATE_PENDING",
			want:      []string{activeARN},
		},
		{
			name:      "IS_NOT CREATE_PENDING excludes active",
			condition: types.FilterConditionStringIsNot,
			value:     "CREATE_PENDING",
			want:      []string{stoppedARN},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, listErr := client.ListPredictors(t.Context(), &forecastsdk.ListPredictorsInput{
				Filters: []types.Filter{{
					Condition: tc.condition,
					Key:       aws.String("Status"),
					Value:     aws.String(tc.value),
				}},
			})
			require.NoError(t, listErr)

			got := make([]string, 0, len(out.Predictors))
			for _, p := range out.Predictors {
				got = append(got, aws.ToString(p.PredictorArn))
			}
			require.ElementsMatch(t, tc.want, got)
		})
	}
}

// TestListDatasetImportJobs_DatasetArnFilter proves ListDatasetImportJobs
// applies a Filters entry keyed on a Data field (DatasetArn), not just
// Status -- the generic path handler.go's listOutput takes for every
// Filter-bearing List operation.
func TestListDatasetImportJobs_DatasetArnFilter(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newTestForecastClient(t, h)

	schema := &types.Schema{Attributes: []types.SchemaAttribute{
		{AttributeName: aws.String("item_id"), AttributeType: types.AttributeTypeString},
	}}

	ds1, err := client.CreateDataset(t.Context(), &forecastsdk.CreateDatasetInput{
		DatasetName: aws.String("filter_ds_one"),
		DatasetType: types.DatasetTypeTargetTimeSeries,
		Domain:      types.DomainRetail,
		Schema:      schema,
	})
	require.NoError(t, err)
	ds2, err := client.CreateDataset(t.Context(), &forecastsdk.CreateDatasetInput{
		DatasetName: aws.String("filter_ds_two"),
		DatasetType: types.DatasetTypeTargetTimeSeries,
		Domain:      types.DomainRetail,
		Schema:      schema,
	})
	require.NoError(t, err)

	job1, err := client.CreateDatasetImportJob(t.Context(), &forecastsdk.CreateDatasetImportJobInput{
		DatasetImportJobName: aws.String("filter_job_one"),
		DatasetArn:           ds1.DatasetArn,
		DataSource: &types.DataSource{
			S3Config: &types.S3Config{
				Path:    aws.String("s3://bucket/one"),
				RoleArn: aws.String("arn:aws:iam::000000000000:role/forecast"),
			},
		},
	})
	require.NoError(t, err)
	_, err = client.CreateDatasetImportJob(t.Context(), &forecastsdk.CreateDatasetImportJobInput{
		DatasetImportJobName: aws.String("filter_job_two"),
		DatasetArn:           ds2.DatasetArn,
		DataSource: &types.DataSource{
			S3Config: &types.S3Config{
				Path:    aws.String("s3://bucket/two"),
				RoleArn: aws.String("arn:aws:iam::000000000000:role/forecast"),
			},
		},
	})
	require.NoError(t, err)

	out, err := client.ListDatasetImportJobs(t.Context(), &forecastsdk.ListDatasetImportJobsInput{
		Filters: []types.Filter{{
			Condition: types.FilterConditionStringIs,
			Key:       aws.String("DatasetArn"),
			Value:     ds1.DatasetArn,
		}},
	})
	require.NoError(t, err)
	require.Len(t, out.DatasetImportJobs, 1)
	require.Equal(t, aws.ToString(job1.DatasetImportJobArn), aws.ToString(out.DatasetImportJobs[0].DatasetImportJobArn))
}

// TestListMonitorEvaluations_EvaluationStateFilter proves
// ListMonitorEvaluations applies its Filters parameter (Key
// "EvaluationState", per api_op_ListMonitorEvaluations.go's doc comment)
// instead of ignoring Filters entirely, as
// handler.go's dispatchListMonitorEvaluations did before this fix.
func TestListMonitorEvaluations_EvaluationStateFilter(t *testing.T) {
	t.Parallel()

	h := newHandler()
	client := newTestForecastClient(t, h)

	dg := sdkDatasetGroup(t, client, "monitor-eval-dg")
	predictorARN := sdkPredictor(t, client, "monitor-eval-predictor", dg)

	monitor, err := client.CreateMonitor(t.Context(), &forecastsdk.CreateMonitorInput{
		MonitorName: aws.String("monitor-eval-filter"),
		ResourceArn: aws.String(predictorARN),
	})
	require.NoError(t, err)

	matching, err := client.ListMonitorEvaluations(t.Context(), &forecastsdk.ListMonitorEvaluationsInput{
		MonitorArn: monitor.MonitorArn,
		Filters: []types.Filter{{
			Condition: types.FilterConditionStringIs,
			Key:       aws.String("EvaluationState"),
			Value:     aws.String("SUCCESS"),
		}},
	})
	require.NoError(t, err)
	require.Len(t, matching.PredictorMonitorEvaluations, 1)

	excluding, err := client.ListMonitorEvaluations(t.Context(), &forecastsdk.ListMonitorEvaluationsInput{
		MonitorArn: monitor.MonitorArn,
		Filters: []types.Filter{{
			Condition: types.FilterConditionStringIs,
			Key:       aws.String("EvaluationState"),
			Value:     aws.String("FAILURE"),
		}},
	})
	require.NoError(t, err)
	require.Empty(t, excluding.PredictorMonitorEvaluations)
}

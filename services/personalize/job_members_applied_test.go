package personalize_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	personalizesdk "github.com/aws/aws-sdk-go-v2/service/personalize"
	"github.com/aws/aws-sdk-go-v2/service/personalize/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_JobMembersRoundTrip(t *testing.T) {
	t.Parallel()

	role := aws.String("arn:aws:iam::000000000000:role/personalize")

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "batch inference config, theme config and numResults",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)
				svArn := personalizeCreateSolutionVersion(t, h, "jm-sol")

				out, err := client.CreateBatchInferenceJob(t.Context(), &personalizesdk.CreateBatchInferenceJobInput{
					JobName:               aws.String("bij"),
					SolutionVersionArn:    aws.String(svArn),
					RoleArn:               role,
					NumResults:            aws.Int32(7),
					BatchInferenceJobMode: types.BatchInferenceJobModeThemeGeneration,
					BatchInferenceJobConfig: &types.BatchInferenceJobConfig{
						ItemExplorationConfig: map[string]string{"explorationWeight": "0.3"},
						RankingInfluence:      map[string]float64{"POPULARITY": 0.5},
					},
					ThemeGenerationConfig: &types.ThemeGenerationConfig{
						FieldsForThemeGeneration: &types.FieldsForThemeGeneration{ItemName: aws.String("TITLE")},
					},
					JobInput: &types.BatchInferenceJobInput{
						S3DataSource: &types.S3DataConfig{Path: aws.String("s3://in")},
					},
					JobOutput: &types.BatchInferenceJobOutput{
						S3DataDestination: &types.S3DataConfig{Path: aws.String("s3://out")},
					},
				})
				require.NoError(t, err)

				d, err := client.DescribeBatchInferenceJob(t.Context(), &personalizesdk.DescribeBatchInferenceJobInput{
					BatchInferenceJobArn: out.BatchInferenceJobArn,
				})
				require.NoError(t, err)

				job := d.BatchInferenceJob
				assert.Equal(t, int32(7), aws.ToInt32(job.NumResults))
				cfg := job.BatchInferenceJobConfig
				assert.Equal(t, map[string]string{"explorationWeight": "0.3"}, cfg.ItemExplorationConfig)
				assert.Equal(t, map[string]float64{"POPULARITY": 0.5}, cfg.RankingInfluence)
				assert.Equal(t, "TITLE", aws.ToString(job.ThemeGenerationConfig.FieldsForThemeGeneration.ItemName))
			},
		},
		{
			name: "batch segment numResults",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)
				svArn := personalizeCreateSolutionVersion(t, h, "jm-sol")

				out, err := client.CreateBatchSegmentJob(t.Context(), &personalizesdk.CreateBatchSegmentJobInput{
					JobName:            aws.String("bsj"),
					SolutionVersionArn: aws.String(svArn),
					RoleArn:            role,
					NumResults:         aws.Int32(11),
					JobInput: &types.BatchSegmentJobInput{
						S3DataSource: &types.S3DataConfig{Path: aws.String("s3://in")},
					},
					JobOutput: &types.BatchSegmentJobOutput{
						S3DataDestination: &types.S3DataConfig{Path: aws.String("s3://out")},
					},
				})
				require.NoError(t, err)

				d, err := client.DescribeBatchSegmentJob(t.Context(), &personalizesdk.DescribeBatchSegmentJobInput{
					BatchSegmentJobArn: out.BatchSegmentJobArn,
				})
				require.NoError(t, err)
				assert.Equal(t, int32(11), aws.ToInt32(d.BatchSegmentJob.NumResults))
			},
		},
		{
			name: "dataset import publishAttributionMetricsToS3",
			run: func(t *testing.T) {
				t.Helper()

				h, client := newPersonalizeClient(t)
				dsArn := personalizeCreateDataset(t, h, "jm-ds")

				out, err := client.CreateDatasetImportJob(t.Context(), &personalizesdk.CreateDatasetImportJobInput{
					JobName:                       aws.String("dij"),
					DatasetArn:                    aws.String(dsArn),
					RoleArn:                       role,
					PublishAttributionMetricsToS3: aws.Bool(true),
					DataSource:                    &types.DataSource{DataLocation: aws.String("s3://bucket/key")},
				})
				require.NoError(t, err)

				d, err := client.DescribeDatasetImportJob(t.Context(), &personalizesdk.DescribeDatasetImportJobInput{
					DatasetImportJobArn: out.DatasetImportJobArn,
				})
				require.NoError(t, err)
				require.NotNil(t, d.DatasetImportJob.PublishAttributionMetricsToS3)
				assert.True(t, *d.DatasetImportJob.PublishAttributionMetricsToS3)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

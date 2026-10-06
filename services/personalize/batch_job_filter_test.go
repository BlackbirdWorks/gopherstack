package personalize_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	personalizesdk "github.com/aws/aws-sdk-go-v2/service/personalize"
	"github.com/aws/aws-sdk-go-v2/service/personalize/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchJobs_FilterArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantErrCode string
		segment     bool
		unknown     bool
	}{
		{name: "inference echoes filter"},
		{name: "segment echoes filter", segment: true},
		{name: "inference unknown filter", unknown: true, wantErrCode: "ResourceNotFoundException"},
		{name: "segment unknown filter", segment: true, unknown: true, wantErrCode: "ResourceNotFoundException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, client := newPersonalizeClient(t)
			svArn := personalizeCreateSolutionVersion(t, h, "bj-sol")
			dgArn := personalizeCreateDatasetGroup(t, h, "bj-dg")

			f, err := client.CreateFilter(t.Context(), &personalizesdk.CreateFilterInput{
				Name:             aws.String("bj-filter"),
				DatasetGroupArn:  aws.String(dgArn),
				FilterExpression: aws.String("INCLUDE ItemID WHERE Items.CATEGORY IN ($CATEGORIES)"),
			})
			require.NoError(t, err)

			filterArn := aws.ToString(f.FilterArn)
			if tt.unknown {
				filterArn += "-missing"
			}

			role := aws.String("arn:aws:iam::000000000000:role/personalize")

			var gotFilter *string

			if tt.segment {
				out, cErr := client.CreateBatchSegmentJob(t.Context(), &personalizesdk.CreateBatchSegmentJobInput{
					JobName:            aws.String("bsj"),
					SolutionVersionArn: aws.String(svArn),
					RoleArn:            role,
					FilterArn:          aws.String(filterArn),
					JobInput: &types.BatchSegmentJobInput{
						S3DataSource: &types.S3DataConfig{Path: aws.String("s3://in")},
					},
					JobOutput: &types.BatchSegmentJobOutput{
						S3DataDestination: &types.S3DataConfig{Path: aws.String("s3://out")},
					},
				})
				err = cErr

				if err == nil {
					d, dErr := client.DescribeBatchSegmentJob(t.Context(), &personalizesdk.DescribeBatchSegmentJobInput{
						BatchSegmentJobArn: out.BatchSegmentJobArn,
					})
					require.NoError(t, dErr)

					gotFilter = d.BatchSegmentJob.FilterArn
				}
			} else {
				out, cErr := client.CreateBatchInferenceJob(t.Context(), &personalizesdk.CreateBatchInferenceJobInput{
					JobName:            aws.String("bij"),
					SolutionVersionArn: aws.String(svArn),
					RoleArn:            role,
					FilterArn:          aws.String(filterArn),
					JobInput: &types.BatchInferenceJobInput{
						S3DataSource: &types.S3DataConfig{Path: aws.String("s3://in")},
					},
					JobOutput: &types.BatchInferenceJobOutput{
						S3DataDestination: &types.S3DataConfig{Path: aws.String("s3://out")},
					},
				})
				err = cErr

				if err == nil {
					d, dErr := client.DescribeBatchInferenceJob(
						t.Context(),
						&personalizesdk.DescribeBatchInferenceJobInput{
							BatchInferenceJobArn: out.BatchInferenceJobArn,
						},
					)
					require.NoError(t, dErr)

					gotFilter = d.BatchInferenceJob.FilterArn
				}
			}

			if tt.wantErrCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrCode)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, filterArn, aws.ToString(gotFilter))
		})
	}
}

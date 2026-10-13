package databrew_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	databrewsdk "github.com/aws/aws-sdk-go-v2/service/databrew"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/databrew/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

func TestJobRun_DelayedLifecycle(t *testing.T) {
	t.Parallel()

	const delay = 10 * time.Second

	tests := []struct {
		name      string
		wantFinal string
		stopAfter time.Duration
	}{
		{name: "succeeds", wantFinal: "SUCCEEDED"},
		{name: "stopped while running", stopAfter: 6 * time.Second, wantFinal: "STOPPED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				b := newTestBackend()
				b.SetJobRunDelay(delay)

				_, err := b.CreateDataset(
					ctx,
					"ds",
					"CSV",
					s3Input("bucket", "k"),
					databrew.DatasetFormatOptions{},
					nil,
					nil,
				)
				require.NoError(t, err)
				_, err = b.CreateJob(ctx, "j", "PROFILE", "ds", "", "", "", nil, nil, databrew.JobExtras{})
				require.NoError(t, err)

				run, err := b.StartJobRun(ctx, "j")
				require.NoError(t, err)
				assert.Equal(t, "STARTING", run.State)

				state := func() string {
					got, dErr := b.DescribeJobRun(ctx, "j", run.RunID)
					require.NoError(t, dErr)

					return got.State
				}

				time.Sleep(delay/2 + time.Second)
				synctest.Wait()
				assert.Equal(t, "RUNNING", state())

				if tt.stopAfter > 0 {
					_, err = b.StopJobRun(ctx, "j", run.RunID)
					require.NoError(t, err)
				}

				time.Sleep(delay)
				synctest.Wait()
				assert.Equal(t, tt.wantFinal, state())
			})
		})
	}
}

func TestSDKValidation(t *testing.T) {
	t.Parallel()

	client := newRoundTripClient(t, newTestHandler())

	_, err := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
		Name: aws.String("ds"),
		Input: &dbtypes.Input{
			S3InputDefinition: &dbtypes.S3Location{Bucket: aws.String("bucket"), Key: aws.String("k")},
		},
	})
	require.NoError(t, err)

	tests := []struct {
		call    func() error
		name    string
		wantErr string
	}{
		{
			name: "bad dataset format",
			call: func() error {
				_, cErr := client.CreateDataset(t.Context(), &databrewsdk.CreateDatasetInput{
					Name:   aws.String("ds2"),
					Format: dbtypes.InputFormat("BOGUS"),
					Input: &dbtypes.Input{
						S3InputDefinition: &dbtypes.S3Location{Bucket: aws.String("bucket"), Key: aws.String("k")},
					},
				})

				return cErr
			},
			wantErr: "ValidationException",
		},
		{
			name: "bad list token",
			call: func() error {
				_, cErr := client.ListDatasets(
					t.Context(),
					&databrewsdk.ListDatasetsInput{NextToken: aws.String("junk")},
				)

				return cErr
			},
			wantErr: "ValidationException",
		},
		{
			name: "max results too large",
			call: func() error {
				_, cErr := client.ListJobs(t.Context(), &databrewsdk.ListJobsInput{MaxResults: aws.Int32(500)})

				return cErr
			},
			wantErr: "ValidationException",
		},
		{
			name: "custom rows without size",
			call: func() error {
				_, cErr := client.CreateProfileJob(t.Context(), &databrewsdk.CreateProfileJobInput{
					Name:            aws.String("pj"),
					DatasetName:     aws.String("ds"),
					RoleArn:         aws.String("arn:aws:iam::123456789012:role/r"),
					OutputLocation:  &dbtypes.S3Location{Bucket: aws.String("bucket")},
					JobSample:       &dbtypes.JobSample{Mode: dbtypes.SampleModeCustomRows},
					LogSubscription: dbtypes.LogSubscriptionEnable,
				})

				return cErr
			},
			wantErr: "ValidationException",
		},
		{
			name: "recipe job with unknown recipe version",
			call: func() error {
				_, cErr := client.CreateRecipe(t.Context(), &databrewsdk.CreateRecipeInput{
					Name: aws.String("r"),
					Steps: []dbtypes.RecipeStep{
						{Action: &dbtypes.RecipeAction{Operation: aws.String("UPPER_CASE")}},
					},
				})
				if cErr != nil {
					return cErr
				}

				_, cErr = client.CreateRecipeJob(t.Context(), &databrewsdk.CreateRecipeJobInput{
					Name:            aws.String("rj"),
					RoleArn:         aws.String("arn:aws:iam::123456789012:role/r"),
					DatasetName:     aws.String("ds"),
					RecipeReference: &dbtypes.RecipeReference{Name: aws.String("r"), RecipeVersion: aws.String("99.9")},
					Outputs: []dbtypes.Output{
						{Location: &dbtypes.S3Location{Bucket: aws.String("bucket")}},
					},
				})

				return cErr
			},
			wantErr: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var apiErr smithy.APIError

			require.ErrorAs(t, tt.call(), &apiErr)
			assert.Equal(t, tt.wantErr, apiErr.ErrorCode())
			assert.NotEmpty(t, apiErr.ErrorMessage())
			assert.NotContains(t, apiErr.ErrorMessage(), tt.wantErr)
		})
	}
}

func TestListDatasets_TokenIsOpaque(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	client := newRoundTripClient(t, databrew.NewHandler(b))

	for _, n := range []string{"ds-a", "ds-b", "ds-c"} {
		_, err := b.CreateDataset(
			t.Context(),
			n,
			"CSV",
			s3Input("bucket", "k"),
			databrew.DatasetFormatOptions{},
			nil,
			nil,
		)
		require.NoError(t, err)
	}

	out, err := client.ListDatasets(t.Context(), &databrewsdk.ListDatasetsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.NotNil(t, out.NextToken)
	assert.NotContains(t, aws.ToString(out.NextToken), "ds-")

	last, err := client.ListDatasets(t.Context(), &databrewsdk.ListDatasetsInput{MaxResults: aws.Int32(5)})
	require.NoError(t, err)
	assert.Nil(t, last.NextToken)
}

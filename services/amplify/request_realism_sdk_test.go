package amplify_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	amplifytypes "github.com/aws/aws-sdk-go-v2/service/amplify/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func TestRequestRealism_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(c *amplifysdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "app not found",
			call: func(c *amplifysdk.Client) error {
				_, err := c.GetApp(t.Context(), &amplifysdk.GetAppInput{AppId: aws.String("nope")})

				return err
			},
			wantCode: "NotFoundException",
		},
		{
			name: "bad next token",
			call: func(c *amplifysdk.Client) error {
				_, err := c.ListApps(t.Context(), &amplifysdk.ListAppsInput{NextToken: aws.String("zzz")})

				return err
			},
			wantCode: "BadRequestException",
		},
		{
			name: "max results too large",
			call: func(c *amplifysdk.Client) error {
				_, err := c.ListApps(t.Context(), &amplifysdk.ListAppsInput{MaxResults: 101})

				return err
			},
			wantCode: "BadRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAmplifyClient(
				t,
				amplify.NewHandler(amplify.NewInMemoryBackend("000000000000", "us-east-1")),
			)
			err := tt.call(client)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), tt.wantCode, apiErr.ErrorMessage())
		})
	}
}

func TestJobIDs_Sequential(t *testing.T) {
	t.Parallel()

	client := newTestAmplifyClient(t, amplify.NewHandler(amplify.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	app, err := client.CreateApp(ctx, &amplifysdk.CreateAppInput{Name: aws.String("a")})
	require.NoError(t, err)
	assert.Regexp(t, `^d[a-z0-9]{13}$`, aws.ToString(app.App.AppId))

	_, err = client.CreateBranch(ctx, &amplifysdk.CreateBranchInput{
		AppId: app.App.AppId, BranchName: aws.String("main"),
	})
	require.NoError(t, err)

	for i := 1; i <= 11; i++ {
		out, sErr := client.StartJob(ctx, &amplifysdk.StartJobInput{
			AppId: app.App.AppId, BranchName: aws.String("main"), JobType: amplifytypes.JobTypeRelease,
		})
		require.NoError(t, sErr)
		assert.Equal(t, strconv.Itoa(i), aws.ToString(out.JobSummary.JobId))
	}

	list, err := client.ListJobs(ctx, &amplifysdk.ListJobsInput{AppId: app.App.AppId, BranchName: aws.String("main")})
	require.NoError(t, err)
	require.Len(t, list.JobSummaries, 11)
	assert.Equal(t, "11", aws.ToString(list.JobSummaries[0].JobId))
}

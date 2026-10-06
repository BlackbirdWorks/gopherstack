package mediaconvert_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	"github.com/aws/aws-sdk-go-v2/service/mediaconvert/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJobsQueryAndVersions_Paging checks nextToken on StartJobsQuery/GetJobsQueryResults and ListVersions.
func TestJobsQueryAndVersions_Paging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		want     []int
		size     int32
		badToken bool
	}{
		{name: "two_then_one", size: 2, want: []int{2, 1}},
		{name: "exact_division", size: 3, want: []int{3}},
		{name: "single_page_default", size: 0, want: []int{3}},
		{name: "bad_token", badToken: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMediaConvertClient(t, newTestHandler(t))
			ctx := t.Context()

			for range 3 {
				_, err := client.CreateJob(ctx, &mediaconvertsdk.CreateJobInput{
					Role:     aws.String("arn:aws:iam::123456789012:role/MediaConvert_Default_Role"),
					Settings: &types.JobSettings{Inputs: []types.Input{{FileInput: aws.String("s3://b/k.mp4")}}},
				})
				require.NoError(t, err)
			}

			if tt.badToken {
				_, err := client.StartJobsQuery(ctx, &mediaconvertsdk.StartJobsQueryInput{NextToken: aws.String("!!")})
				require.ErrorContains(t, err, "BadRequestException")

				_, err = client.ListVersions(ctx, &mediaconvertsdk.ListVersionsInput{NextToken: aws.String("!!")})
				require.ErrorContains(t, err, "BadRequestException")

				return
			}

			var (
				token *string
				got   []int
			)

			for range 4 {
				in := &mediaconvertsdk.StartJobsQueryInput{NextToken: token, Order: types.OrderAscending}
				if tt.size > 0 {
					in.MaxResults = aws.Int32(tt.size)
				}

				start, err := client.StartJobsQuery(ctx, in)
				require.NoError(t, err)

				res, err := client.GetJobsQueryResults(ctx, &mediaconvertsdk.GetJobsQueryResultsInput{Id: start.Id})
				require.NoError(t, err)

				got = append(got, len(res.Jobs))
				if token = res.NextToken; token == nil {
					break
				}
			}

			assert.Equal(t, tt.want, got)

			versions, err := client.ListVersions(ctx, &mediaconvertsdk.ListVersionsInput{MaxResults: aws.Int32(1)})
			require.NoError(t, err)
			assert.Len(t, versions.Versions, 1)
			assert.Nil(t, versions.NextToken)
		})
	}
}

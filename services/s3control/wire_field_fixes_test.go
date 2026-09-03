package s3control_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

// TestListAccessPoints_FullPagination creates more access points than one
// page holds and drives the real SDK client through the full pagination
// loop, asserting the union is exactly the created set with no duplicates
// and nothing missing. store.Table.All() (pkgs/store/table.go:154) documents
// unspecified iteration order; ListAccessPoints previously fed that order
// directly into the handler's index-based nextToken pagination
// (s3cPaginate), so successive pages could repeat or skip access points.
func TestListAccessPoints_FullPagination(t *testing.T) {
	t.Parallel()

	backend := s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion)
	client := newTestS3ControlClient(t, s3control.NewHandler(backend))

	const total = 9

	want := make(map[string]bool, total)

	for i := range total {
		name := fmt.Sprintf("ap-%02d", i)
		_, err := client.CreateAccessPoint(t.Context(), &s3csdk.CreateAccessPointInput{
			AccountId: aws.String(createTagsTestAccountID),
			Name:      aws.String(name),
			Bucket:    aws.String("some-bucket"),
		})
		require.NoError(t, err)
		want[name] = true
	}

	got := make(map[string]bool, total)

	var nextToken *string
	for pages := 0; ; pages++ {
		require.Less(t, pages, total, "pagination loop did not terminate")

		out, err := client.ListAccessPoints(t.Context(), &s3csdk.ListAccessPointsInput{
			AccountId:  aws.String(createTagsTestAccountID),
			MaxResults: 4,
			NextToken:  nextToken,
		})
		require.NoError(t, err)
		require.LessOrEqual(t, len(out.AccessPointList), 4)

		for _, ap := range out.AccessPointList {
			name := aws.ToString(ap.Name)
			require.Falsef(t, got[name], "access point %q returned twice across pages", name)
			got[name] = true
		}

		if out.NextToken == nil {
			break
		}

		nextToken = out.NextToken
	}

	require.Equal(t, want, got)
}

// TestListJobs_FullPagination creates more batch jobs than one page holds
// and drives the real SDK client through the full pagination loop,
// asserting the union is exactly the created set with no duplicates and
// nothing missing. Same root cause as TestListAccessPoints_FullPagination:
// ListJobs fed store.Table.All()'s unspecified order into s3cPaginate.
func TestListJobs_FullPagination(t *testing.T) {
	t.Parallel()

	backend := s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion)
	client := newTestS3ControlClient(t, s3control.NewHandler(backend))

	const total = 9

	want := make(map[string]bool, total)

	for i := range total {
		out, err := client.CreateJob(t.Context(), &s3csdk.CreateJobInput{
			AccountId:          aws.String(createTagsTestAccountID),
			ClientRequestToken: aws.String(fmt.Sprintf("token-%d", i)),
			Operation: &types.JobOperation{
				LambdaInvoke: &types.LambdaInvokeOperation{
					FunctionArn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:fn"),
				},
			},
			Priority: aws.Int32(1),
			Report:   &types.JobReport{Enabled: false},
			RoleArn:  aws.String("arn:aws:iam::123456789012:role/batch-ops"),
		})
		require.NoError(t, err)
		want[aws.ToString(out.JobId)] = true
	}

	got := make(map[string]bool, total)

	var nextToken *string
	for pages := 0; ; pages++ {
		require.Less(t, pages, total, "pagination loop did not terminate")

		out, err := client.ListJobs(t.Context(), &s3csdk.ListJobsInput{
			AccountId:  aws.String(createTagsTestAccountID),
			MaxResults: aws.Int32(4),
			NextToken:  nextToken,
		})
		require.NoError(t, err)
		require.LessOrEqual(t, len(out.Jobs), 4)

		for _, j := range out.Jobs {
			id := aws.ToString(j.JobId)
			require.Falsef(t, got[id], "job %q returned twice across pages", id)
			got[id] = true
		}

		if out.NextToken == nil {
			break
		}

		nextToken = out.NextToken
	}

	require.Equal(t, want, got)
}

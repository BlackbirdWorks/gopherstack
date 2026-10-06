package s3control_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

func newTestHandler() *s3control.Handler {
	return s3control.NewHandler(
		s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion),
	)
}

func TestSDK_AccessGrantsPolicyOrganizationAndCreatedAt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		org  *string
		name string
	}{
		{name: "with organization", org: aws.String("o-abc123")},
		{name: "without organization", org: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestS3ControlClient(t, newTestHandler())

			put, err := client.PutAccessGrantsInstanceResourcePolicy(
				t.Context(),
				&s3csdk.PutAccessGrantsInstanceResourcePolicyInput{
					AccountId:    aws.String(createTagsTestAccountID),
					Policy:       aws.String(`{"Version":"2012-10-17"}`),
					Organization: tc.org,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(tc.org), aws.ToString(put.Organization))
			require.NotNil(t, put.CreatedAt)

			got, err := client.GetAccessGrantsInstanceResourcePolicy(
				t.Context(),
				&s3csdk.GetAccessGrantsInstanceResourcePolicyInput{
					AccountId: aws.String(createTagsTestAccountID),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(tc.org), aws.ToString(got.Organization))
			require.NotNil(t, got.CreatedAt)
			assert.True(t, put.CreatedAt.Equal(*got.CreatedAt))
		})
	}
}

func TestSDK_ListJobsStatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		statuses []types.JobStatus
		wantJobs int
	}{
		{name: "no filter", wantJobs: 1},
		{name: "matching status", statuses: []types.JobStatus{types.JobStatusNew}, wantJobs: 1},
		{name: "other status", statuses: []types.JobStatus{types.JobStatusComplete}, wantJobs: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestS3ControlClient(t, newTestHandler())

			_, err := client.CreateJob(t.Context(), &s3csdk.CreateJobInput{
				AccountId:          aws.String(createTagsTestAccountID),
				ClientRequestToken: aws.String("token-1"),
				Operation: &types.JobOperation{
					LambdaInvoke: &types.LambdaInvokeOperation{
						FunctionArn: aws.String("arn:aws:lambda:us-east-1:123456789012:function:f"),
					},
				},
				Priority: aws.Int32(1),
				Report:   &types.JobReport{Enabled: false},
				RoleArn:  aws.String("arn:aws:iam::123456789012:role/batch-ops"),
			})
			require.NoError(t, err)

			out, err := client.ListJobs(t.Context(), &s3csdk.ListJobsInput{
				AccountId:   aws.String(createTagsTestAccountID),
				JobStatuses: tc.statuses,
			})
			require.NoError(t, err)
			assert.Len(t, out.Jobs, tc.wantJobs)
		})
	}
}

func TestAccessGrantsPolicyMetaSurvivesSnapshot(t *testing.T) {
	t.Parallel()

	src := s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion)
	want := src.PutAccessGrantsInstanceResourcePolicyWithOrganization(createTagsTestAccountID, `{}`, "o-1")

	dst := s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion)
	require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

	assert.Equal(t, want, dst.GetAccessGrantsInstanceResourcePolicyMeta(createTagsTestAccountID))
}

func TestSDK_PutStorageLensConfigurationAppliesTags(t *testing.T) {
	t.Parallel()

	client := newTestS3ControlClient(t, newTestHandler())

	_, err := client.PutStorageLensConfiguration(t.Context(), &s3csdk.PutStorageLensConfigurationInput{
		AccountId: aws.String(createTagsTestAccountID),
		ConfigId:  aws.String("lens-1"),
		StorageLensConfiguration: &types.StorageLensConfiguration{
			Id:           aws.String("lens-1"),
			IsEnabled:    true,
			AccountLevel: &types.AccountLevel{BucketLevel: &types.BucketLevel{}},
		},
		Tags: []types.StorageLensTag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	require.NoError(t, err)

	out, err := client.GetStorageLensConfigurationTagging(t.Context(), &s3csdk.GetStorageLensConfigurationTaggingInput{
		AccountId: aws.String(createTagsTestAccountID),
		ConfigId:  aws.String("lens-1"),
	})
	require.NoError(t, err)
	require.Len(t, out.Tags, 1)
	assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
	assert.Equal(t, "dev", aws.ToString(out.Tags[0].Value))
}

func TestSDK_ListAccessPointsForDirectoryBucketsFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter *string
		name   string
		want   []string
	}{
		{name: "no filter lists all", want: []string{"ap-a", "ap-b"}},
		{name: "filter by bucket", filter: aws.String("bucket-a--usw2-az1--x-s3"), want: []string{"ap-a"}},
		{name: "unknown bucket", filter: aws.String("nope--usw2-az1--x-s3")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestS3ControlClient(t, newTestHandler())

			for name, bucket := range map[string]string{"ap-a": "bucket-a--usw2-az1--x-s3", "ap-b": "bucket-b--usw2-az1--x-s3"} {
				_, err := client.CreateAccessPoint(t.Context(), &s3csdk.CreateAccessPointInput{
					AccountId: aws.String(createTagsTestAccountID), Name: aws.String(name), Bucket: aws.String(bucket),
				})
				require.NoError(t, err)
			}

			out, err := client.ListAccessPointsForDirectoryBuckets(
				t.Context(),
				&s3csdk.ListAccessPointsForDirectoryBucketsInput{
					AccountId: aws.String(createTagsTestAccountID), DirectoryBucket: tc.filter,
				},
			)
			require.NoError(t, err)

			var got []string
			for _, ap := range out.AccessPointList {
				got = append(got, aws.ToString(ap.Name))
			}

			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

package integration_test

import (
	"testing"

	smithy "github.com/aws/smithy-go"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_S3_BucketTagging verifies PutBucketTagging, GetBucketTagging,
// and DeleteBucketTagging operations.
func TestIntegration_S3_BucketTagging(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		setup    func(t *testing.T, client *s3.Client, bucket string)
		name     string
		wantTags []types.Tag
		wantErr  bool
	}{
		{
			name: "put and get single tag",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketTagging(t.Context(), &s3.PutBucketTaggingInput{
					Bucket: aws.String(bucket),
					Tagging: &types.Tagging{
						TagSet: []types.Tag{
							{Key: aws.String("env"), Value: aws.String("prod")},
						},
					},
				})
				require.NoError(t, err)
			},
			wantTags: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")},
			},
		},
		{
			name: "put and get multiple tags",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				_, err := client.PutBucketTagging(t.Context(), &s3.PutBucketTaggingInput{
					Bucket: aws.String(bucket),
					Tagging: &types.Tagging{
						TagSet: []types.Tag{
							{Key: aws.String("env"), Value: aws.String("dev")},
							{Key: aws.String("team"), Value: aws.String("infra")},
						},
					},
				})
				require.NoError(t, err)
			},
			wantTags: []types.Tag{
				{Key: aws.String("env"), Value: aws.String("dev")},
				{Key: aws.String("team"), Value: aws.String("infra")},
			},
		},
		{
			name: "put replaces previous tags",
			setup: func(t *testing.T, client *s3.Client, bucket string) {
				t.Helper()
				// Store initial tags.
				_, err := client.PutBucketTagging(t.Context(), &s3.PutBucketTaggingInput{
					Bucket: aws.String(bucket),
					Tagging: &types.Tagging{
						TagSet: []types.Tag{
							{Key: aws.String("old"), Value: aws.String("value")},
						},
					},
				})
				require.NoError(t, err)

				// Overwrite with new tags.
				_, err = client.PutBucketTagging(t.Context(), &s3.PutBucketTaggingInput{
					Bucket: aws.String(bucket),
					Tagging: &types.Tagging{
						TagSet: []types.Tag{
							{Key: aws.String("new"), Value: aws.String("tag")},
						},
					},
				})
				require.NoError(t, err)
			},
			wantTags: []types.Tag{
				{Key: aws.String("new"), Value: aws.String("tag")},
			},
		},
		{
			name: "get returns NoSuchTagSet when not set",
			setup: func(_ *testing.T, _ *s3.Client, _ string) {
				// No tags set.
			},
			wantErr: true,
		},
	}

	client := createS3Client(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			bucket := "bkt-tagging-integ-" + uuid.NewString()

			_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			t.Cleanup(func() {
				_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
			})

			tt.setup(t, client, bucket)

			out, err := client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
				Bucket: aws.String(bucket),
			})

			if tt.wantErr {
				require.Error(t, err)

				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, "NoSuchTagSet", apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			require.Len(t, out.TagSet, len(tt.wantTags))

			for i, want := range tt.wantTags {
				got := out.TagSet[i]
				assert.Equal(t, aws.ToString(want.Key), aws.ToString(got.Key))
				assert.Equal(t, aws.ToString(want.Value), aws.ToString(got.Value))
			}
		})
	}
}

// TestIntegration_S3_DeleteBucketTagging verifies that DeleteBucketTagging removes all tags.
func TestIntegration_S3_DeleteBucketTagging(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createS3Client(t)
	bucket := "bkt-del-tagging-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	// Store tags.
	_, err = client.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
		Bucket: aws.String(bucket),
		Tagging: &types.Tagging{
			TagSet: []types.Tag{
				{Key: aws.String("k"), Value: aws.String("v")},
			},
		},
	})
	require.NoError(t, err)

	// Confirm tags are present.
	out, err := client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	require.Len(t, out.TagSet, 1)

	// Delete tags.
	_, err = client.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	// Confirm tags are gone.
	_, err = client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: aws.String(bucket)})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NoSuchTagSet", apiErr.ErrorCode())
}

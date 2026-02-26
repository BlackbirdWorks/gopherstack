package integration_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_S3_LifecycleEnforcement verifies that the S3 janitor enforces
// lifecycle expiration rules and removes matching objects.
func TestIntegration_S3_LifecycleEnforcement(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createS3Client(t)
	ctx := t.Context()
	bucket := "lc-enforce-" + uuid.NewString()

	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	t.Cleanup(func() {
		out, _ := client.ListObjects(t.Context(), &s3.ListObjectsInput{Bucket: aws.String(bucket)})
		if out != nil {
			for _, obj := range out.Contents {
				_, _ = client.DeleteObject(t.Context(), &s3.DeleteObjectInput{
					Bucket: aws.String(bucket), Key: obj.Key,
				})
			}
		}
		_, _ = client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	// Put objects under the logs/ prefix — these should be expired by lifecycle.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("logs/app.log"),
		Body: strings.NewReader("log data"),
	})
	require.NoError(t, err)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("logs/error.log"),
		Body: strings.NewReader("error data"),
	})
	require.NoError(t, err)

	// Put an object outside the prefix — it should NOT be expired.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("data/keep.txt"),
		Body: strings.NewReader("keep me"),
	})
	require.NoError(t, err)

	// Set a 0-day lifecycle rule: expire objects under logs/ immediately.
	_, err = client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("expire-logs"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String("logs/")},
					Expiration: &types.LifecycleExpiration{
						Days: aws.Int32(0),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Wait for the janitor to expire the logs/ objects (max 10 s with 500 ms check).
	require.Eventually(t, func() bool {
		out, listErr := client.ListObjects(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(bucket), Prefix: aws.String("logs/"),
		})

		return listErr == nil && len(out.Contents) == 0
	}, 10*time.Second, 500*time.Millisecond, "logs/ objects should be expired by lifecycle rule")

	// Verify data/keep.txt is still present.
	out, err := client.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(bucket), Prefix: aws.String("data/"),
	})
	require.NoError(t, err)
	assert.Len(t, out.Contents, 1)
	assert.Equal(t, "data/keep.txt", aws.ToString(out.Contents[0].Key))
}

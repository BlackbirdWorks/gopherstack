package s3_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListBuckets_BucketRegionFilter proves the bucket-region query
// parameter (api_op_ListBuckets.go's BucketRegion: "Limits the response to
// buckets that are located in the specified Amazon Web Services Region") is
// applied -- previously the handler never read it into ListBucketsInput,
// and the backend never filtered on it even when set, so every ListBuckets
// call returned buckets from every region regardless of the filter.
func TestListBuckets_BucketRegionFilter(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	ctx := t.Context()

	_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{
		Bucket: aws.String("west-bucket"),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraintUsWest2,
		},
	})
	require.NoError(t, err)

	// us-east-1 is the "classic" region with no LocationConstraint value
	// (bucket_ops.go's own comment on this); omitting CreateBucketConfiguration
	// leaves these buckets at the backend's default region, us-east-1.
	_, err = client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String("east-bucket-1")})
	require.NoError(t, err)

	_, err = client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String("east-bucket-2")})
	require.NoError(t, err)

	westOnly, err := client.ListBuckets(ctx, &sdk_s3.ListBucketsInput{
		BucketRegion: aws.String("us-west-2"),
	})
	require.NoError(t, err)
	require.Len(t, westOnly.Buckets, 1, "bucket-region filter must exclude buckets in other regions")
	assert.Equal(t, "west-bucket", aws.ToString(westOnly.Buckets[0].Name))

	eastOnly, err := client.ListBuckets(ctx, &sdk_s3.ListBucketsInput{
		BucketRegion: aws.String("us-east-1"),
	})
	require.NoError(t, err)
	assert.Len(t, eastOnly.Buckets, 2)

	all, err := client.ListBuckets(ctx, &sdk_s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Len(t, all.Buckets, 3)
}

package s3_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListBuckets_SDKRoundTrip_Pagination drives the real SDK client across two pages
// of ListBuckets and asserts the pages are disjoint. Before the fix, the backend's
// ListBuckets always ignored ContinuationToken/MaxBuckets (both real ListBucketsInput
// members, httpQuery-bound per aws-sdk-go-v2/service/s3 serializers.go) -- the handler
// didn't even parse them off the request -- and always returned every bucket in one
// unbounded page with no ContinuationToken.
func TestListBuckets_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)

	const total = 25

	for i := range total {
		_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
			Bucket: aws.String(fmt.Sprintf("paginated-bucket-%02d", i)),
		})
		require.NoError(t, err)
	}

	page1, err := client.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{
		MaxBuckets: aws.Int32(10),
	})
	require.NoError(t, err)
	require.Len(t, page1.Buckets, 10)
	require.NotNil(t, page1.ContinuationToken)

	page2, err := client.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{
		MaxBuckets:        aws.Int32(10),
		ContinuationToken: page1.ContinuationToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Buckets, 10)

	seen := make(map[string]bool, 20)
	for _, b := range page1.Buckets {
		seen[aws.ToString(b.Name)] = true
	}

	for _, b := range page2.Buckets {
		assert.False(t, seen[aws.ToString(b.Name)], "page 2 repeated bucket %s from page 1", aws.ToString(b.Name))
		seen[aws.ToString(b.Name)] = true
	}

	assert.Len(t, seen, 20)
}

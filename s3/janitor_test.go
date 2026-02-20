package s3_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/s3"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// newFastJanitor creates a Janitor with a short interval for deterministic tests.
func newFastJanitor(b *s3.InMemoryBackend) *s3.Janitor {
	j := s3.NewJanitor(b, logger.NewTestLogger())
	j.Interval = 5 * time.Millisecond

	return j
}

func TestS3Janitor_EmptyBucketRemovedImmediately(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)
	mustCreateBucket(t, b, "bucket-a")

	// Delete marks as pending.
	_, err := b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("bucket-a")})
	require.NoError(t, err)

	// Bucket should appear gone to clients immediately (DeletePending state).
	_, err = b.HeadBucket(t.Context(), &sdk_s3.HeadBucketInput{Bucket: aws.String("bucket-a")})
	require.ErrorIs(t, err, s3.ErrNoSuchBucket)

	// Run janitor and wait for it to fire.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	j := newFastJanitor(b)
	go j.Run(ctx)

	require.Eventually(t, func() bool {
		listed, listErr := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
		return listErr == nil && len(listed.Buckets) == 0
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestS3Janitor_BucketWithObjectsRemovedAfterDrain(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)
	mustCreateBucket(t, b, "full-bucket")

	for i := 0; i < 5; i++ {
		mustPutObject(t, b, "full-bucket", fmt.Sprintf("key-%d", i), []byte("data"))
	}

	// DeleteBucket should now succeed (async).
	_, err := b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("full-bucket")})
	require.NoError(t, err)

	// Objects inside a pending-delete bucket are invisible to clients.
	_, err = b.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("full-bucket"),
		Key:    aws.String("key-a"),
	})
	require.ErrorIs(t, err, s3.ErrNoSuchBucket)

	// Run janitor until the bucket is fully gone.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	j := newFastJanitor(b)
	go j.Run(ctx)

	require.Eventually(t, func() bool {
		listed, listErr := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
		return listErr == nil && len(listed.Buckets) == 0
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestS3Janitor_DeleteIsIdempotent(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)
	mustCreateBucket(t, b, "idem-bucket")

	_, err := b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("idem-bucket")})
	require.NoError(t, err)

	// Second delete should also succeed.
	_, err = b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("idem-bucket")})
	require.NoError(t, err)
}

func TestS3Janitor_ListBucketsExcludesPending(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)
	mustCreateBucket(t, b, "keep")
	mustCreateBucket(t, b, "gone")

	_, err := b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("gone")})
	require.NoError(t, err)

	listed, err := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Len(t, listed.Buckets, 1)
	assert.Equal(t, "keep", aws.ToString(listed.Buckets[0].Name))
}

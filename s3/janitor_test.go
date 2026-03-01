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

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/s3"
)

// newFastJanitor creates a Janitor with a short interval for deterministic tests.
func newFastJanitor(b *s3.InMemoryBackend) *s3.Janitor {
	return s3.NewJanitor(b, logger.NewTestLogger(), s3.Settings{JanitorInterval: 5 * time.Millisecond})
}

func TestS3Janitor_EmptyBucketRemovedImmediately(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil, nil)
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

	b := s3.NewInMemoryBackend(nil, nil)
	mustCreateBucket(t, b, "full-bucket")

	for i := range 5 {
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

	b := s3.NewInMemoryBackend(nil, nil)
	mustCreateBucket(t, b, "idem-bucket")

	_, err := b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("idem-bucket")})
	require.NoError(t, err)

	// Second delete should also succeed.
	_, err = b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("idem-bucket")})
	require.NoError(t, err)
}

func TestS3Janitor_ListBucketsExcludesPending(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil, nil)
	mustCreateBucket(t, b, "keep")
	mustCreateBucket(t, b, "gone")

	_, err := b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{Bucket: aws.String("gone")})
	require.NoError(t, err)

	listed, err := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Len(t, listed.Buckets, 1)
	assert.Equal(t, "keep", aws.ToString(listed.Buckets[0].Name))
}

func TestS3Janitor_LifecycleExpiry_ZeroDays(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil, nil)
	mustCreateBucket(t, b, "lc-bucket")
	mustPutObject(t, b, "lc-bucket", "logs/old.txt", []byte("data"))
	mustPutObject(t, b, "lc-bucket", "logs/old2.txt", []byte("data"))

	// 0-day expiration means objects created today are already expired.
	lifecycleXML := `<LifecycleConfiguration>
<Rule>
  <ID>expire-all</ID>
  <Status>Enabled</Status>
  <Filter><Prefix>logs/</Prefix></Filter>
  <Expiration><Days>0</Days></Expiration>
</Rule>
</LifecycleConfiguration>`
	err := b.PutBucketLifecycleConfiguration(t.Context(), "lc-bucket", lifecycleXML)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	j := newFastJanitor(b)
	go j.Run(ctx)

	require.Eventually(t, func() bool {
		out, listErr := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
			Bucket: aws.String("lc-bucket"),
		})

		return listErr == nil && len(out.Contents) == 0
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestS3Janitor_LifecycleExpiry_PrefixFilter(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil, nil)
	mustCreateBucket(t, b, "prefix-bucket")
	mustPutObject(t, b, "prefix-bucket", "logs/old.txt", []byte("data"))
	mustPutObject(t, b, "prefix-bucket", "data/keep.txt", []byte("keep"))

	lifecycleXML := `<LifecycleConfiguration>
<Rule>
  <ID>expire-logs</ID>
  <Status>Enabled</Status>
  <Filter><Prefix>logs/</Prefix></Filter>
  <Expiration><Days>0</Days></Expiration>
</Rule>
</LifecycleConfiguration>`
	err := b.PutBucketLifecycleConfiguration(t.Context(), "prefix-bucket", lifecycleXML)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	j := newFastJanitor(b)
	go j.Run(ctx)

	require.Eventually(t, func() bool {
		out, listErr := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
			Bucket: aws.String("prefix-bucket"),
		})
		if listErr != nil {
			return false
		}
		// logs/ object gone, data/ object kept
		for _, obj := range out.Contents {
			if aws.ToString(obj.Key) == "logs/old.txt" {
				return false
			}
		}

		return true
	}, 500*time.Millisecond, 10*time.Millisecond)

	// Verify data/keep.txt is still present
	out, err := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
		Bucket: aws.String("prefix-bucket"),
	})
	require.NoError(t, err)
	assert.Len(t, out.Contents, 1)
	assert.Equal(t, "data/keep.txt", aws.ToString(out.Contents[0].Key))
}

func TestS3Janitor_LifecycleExpiry_DisabledRuleIgnored(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)
	mustCreateBucket(t, b, "disabled-bucket")
	mustPutObject(t, b, "disabled-bucket", "key.txt", []byte("data"))

	lifecycleXML := `<LifecycleConfiguration>
<Rule>
  <ID>disabled-rule</ID>
  <Status>Disabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Expiration><Days>0</Days></Expiration>
</Rule>
</LifecycleConfiguration>`
	err := b.PutBucketLifecycleConfiguration(t.Context(), "disabled-bucket", lifecycleXML)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	j := newFastJanitor(b)
	go j.Run(ctx)

	// Wait a few ticks then verify the object is NOT deleted.
	time.Sleep(50 * time.Millisecond)
	cancel()

	out, err := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
		Bucket: aws.String("disabled-bucket"),
	})
	require.NoError(t, err)
	assert.Len(t, out.Contents, 1)
}

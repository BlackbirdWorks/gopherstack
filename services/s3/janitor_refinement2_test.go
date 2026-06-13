package s3_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// ---------------------------------------------------------------------------
// Lifecycle Days=nil guard (fix #2)
// ---------------------------------------------------------------------------

// TestRefinement2_Lifecycle_NoExpirationElement verifies that a lifecycle rule
// with only AbortIncompleteMultipartUpload and no <Expiration> element does NOT
// delete existing objects (Days field was previously int zero-value = 0).
func TestRefinement2_Lifecycle_NoExpirationElement(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)

	mustCreateBucket(t, b, "no-expiry-bucket")
	mustPutObject(t, b, "no-expiry-bucket", "keep-me.txt", []byte("data"))

	// Rule has no <Expiration> element — only AbortIncompleteMultipartUpload.
	lc := `<LifecycleConfiguration>
<Rule>
  <ID>abort-only</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload>
</Rule>
</LifecycleConfiguration>`

	err := b.PutBucketLifecycleConfiguration(t.Context(), "no-expiry-bucket", lc)
	require.NoError(t, err)

	j := newFastJanitor(b)
	j.SweepOnce(t.Context())

	out, err := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
		Bucket: aws.String("no-expiry-bucket"),
	})
	require.NoError(t, err)
	assert.Len(t, out.Contents, 1, "object must NOT be deleted when <Expiration> is absent")
}

// ---------------------------------------------------------------------------
// Object-lock bypass fix (#3, #4)
// ---------------------------------------------------------------------------

// TestRefinement2_Lifecycle_SkipsLockedObjects verifies that lifecycle expiry
// does not delete objects protected by an active retention period.
func TestRefinement2_Lifecycle_SkipsLockedObjects(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)

	mustCreateBucket(t, b, "locked-bucket")
	mustPutObject(t, b, "locked-bucket", "locked.txt", []byte("worm"))
	mustPutObject(t, b, "locked-bucket", "free.txt", []byte("free"))

	// Put retention on locked.txt.
	err := b.PutObjectRetention(t.Context(), "locked-bucket", "locked.txt", nil,
		string(sdk_s3types.ObjectLockRetentionModeCompliance), time.Now().Add(24*time.Hour))
	require.NoError(t, err)

	lc := `<LifecycleConfiguration>
<Rule>
  <ID>expire-all</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Expiration><Days>0</Days></Expiration>
</Rule>
</LifecycleConfiguration>`

	err = b.PutBucketLifecycleConfiguration(t.Context(), "locked-bucket", lc)
	require.NoError(t, err)

	j := newFastJanitor(b)
	j.SweepOnce(t.Context())

	out, err := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
		Bucket: aws.String("locked-bucket"),
	})
	require.NoError(t, err)

	keys := make([]string, 0, len(out.Contents))
	for _, obj := range out.Contents {
		keys = append(keys, aws.ToString(obj.Key))
	}

	assert.Contains(
		t,
		keys,
		"locked.txt",
		"retention-locked object must not be deleted by lifecycle",
	)
	assert.NotContains(t, keys, "free.txt", "unlocked object should be evicted")
}

// TestRefinement2_Lifecycle_SkipsLegalHoldObjects verifies that lifecycle expiry
// does not delete objects protected by a legal hold.
func TestRefinement2_Lifecycle_SkipsLegalHoldObjects(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)

	mustCreateBucket(t, b, "legal-hold-bucket")
	mustPutObject(t, b, "legal-hold-bucket", "held.txt", []byte("evidence"))

	err := b.PutObjectLegalHold(t.Context(), "legal-hold-bucket", "held.txt", nil,
		string(sdk_s3types.ObjectLockLegalHoldStatusOn))
	require.NoError(t, err)

	lc := `<LifecycleConfiguration>
<Rule>
  <ID>expire-all</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Expiration><Days>0</Days></Expiration>
</Rule>
</LifecycleConfiguration>`

	err = b.PutBucketLifecycleConfiguration(t.Context(), "legal-hold-bucket", lc)
	require.NoError(t, err)

	j := newFastJanitor(b)
	j.SweepOnce(t.Context())

	out, err := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
		Bucket: aws.String("legal-hold-bucket"),
	})
	require.NoError(t, err)

	keys := make([]string, 0, len(out.Contents))
	for _, obj := range out.Contents {
		keys = append(keys, aws.ToString(obj.Key))
	}

	assert.Contains(t, keys, "held.txt", "legal-hold object must not be deleted by lifecycle")
}

// TestRefinement2_NoncurrentVersionEviction_SkipsLockedVersions verifies that
// noncurrent version eviction respects object-lock on individual versions.
func TestRefinement2_NoncurrentVersionEviction_SkipsLockedVersions(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)

	// Create a versioned bucket.
	mustCreateBucket(t, b, "nc-lock-bucket")
	_, err := b.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket: aws.String("nc-lock-bucket"),
		VersioningConfiguration: &sdk_s3types.VersioningConfiguration{
			Status: sdk_s3types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	// Put initial version (becomes noncurrent after next put).
	_, err = b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("nc-lock-bucket"),
		Key:    aws.String("obj.txt"),
		Body:   bytes.NewReader([]byte("v1")),
	})
	require.NoError(t, err)

	// Put a second version to push the first to noncurrent.
	_, err = b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("nc-lock-bucket"),
		Key:    aws.String("obj.txt"),
		Body:   bytes.NewReader([]byte("v2")),
	})
	require.NoError(t, err)

	// Add retention to the noncurrent version.
	versions, err := b.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
		Bucket: aws.String("nc-lock-bucket"),
		Prefix: aws.String("obj.txt"),
	})
	require.NoError(t, err)

	// Find the noncurrent version.
	var ncVersionID string
	for _, v := range versions.Versions {
		if !aws.ToBool(v.IsLatest) {
			ncVersionID = aws.ToString(v.VersionId)

			break
		}
	}
	require.NotEmpty(t, ncVersionID, "must have a noncurrent version")

	err = b.PutObjectRetention(t.Context(), "nc-lock-bucket", "obj.txt", &ncVersionID,
		string(sdk_s3types.ObjectLockRetentionModeCompliance), time.Now().Add(24*time.Hour))
	require.NoError(t, err)

	// Run a noncurrent lifecycle rule.
	lc := `<LifecycleConfiguration>
<Rule>
  <ID>nc-expire</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <NoncurrentVersionExpiration><NoncurrentDays>0</NoncurrentDays></NoncurrentVersionExpiration>
</Rule>
</LifecycleConfiguration>`

	err = b.PutBucketLifecycleConfiguration(t.Context(), "nc-lock-bucket", lc)
	require.NoError(t, err)

	j := newFastJanitor(b)
	j.SweepOnce(t.Context())

	// The locked noncurrent version must still exist.
	versions2, err := b.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
		Bucket: aws.String("nc-lock-bucket"),
		Prefix: aws.String("obj.txt"),
	})
	require.NoError(t, err)

	var found bool
	for _, v := range versions2.Versions {
		if aws.ToString(v.VersionId) == ncVersionID {
			found = true

			break
		}
	}
	assert.True(t, found, "locked noncurrent version must not be evicted")
}

// ---------------------------------------------------------------------------
// S3 Reset() mutex leak fix (#5)
// ---------------------------------------------------------------------------

// TestRefinement2_S3Reset_NoMutexLeak verifies that Reset does not panic and
// leaves the backend in a clean state (no buckets).
func TestRefinement2_S3Reset_NoMutexLeak(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)

	for i := range 5 {
		mustCreateBucket(t, b, bucketName(i))
		for j := range 3 {
			mustPutObject(t, b, bucketName(i), keyName(i, j), []byte("data"))
		}
	}

	require.NotPanics(t, func() { b.Reset() })

	// All buckets should be gone.
	out, err := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.Buckets, "all buckets should be removed after Reset")
}

// TestRefinement2_S3Reset_MultipleResets verifies consecutive Resets do not panic.
func TestRefinement2_S3Reset_MultipleResets(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)
	mustCreateBucket(t, b, "reset-bucket")
	mustPutObject(t, b, "reset-bucket", "obj.txt", []byte("data"))

	require.NotPanics(t, func() {
		b.Reset()
		b.Reset() // second reset on empty backend must not panic
	})
}

// ---------------------------------------------------------------------------
// S3 Purge() mutex leak fix (#6)
// ---------------------------------------------------------------------------

// TestRefinement2_S3Purge_ClosesObjectMutexes verifies that Purge on old buckets
// does not panic and removes buckets and their objects cleanly.
func TestRefinement2_S3Purge_ClosesObjectMutexes(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)

	for i := range 3 {
		mustCreateBucket(t, b, bucketName(i))
		for j := range 5 {
			mustPutObject(t, b, bucketName(i), keyName(i, j), []byte("data"))
		}
	}

	require.NotPanics(t, func() {
		b.Purge(t.Context(), time.Now().Add(time.Hour)) // future cutoff — removes all
	})

	out, err := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.Buckets, "all buckets purged before cutoff must be removed")
}

// ---------------------------------------------------------------------------
// drain goroutine recover() order fix (#10)
// ---------------------------------------------------------------------------

// TestRefinement2_S3Janitor_DrainGoroutine_NoPanic verifies that a normal
// janitor run on multiple buckets does not panic and properly drains.
func TestRefinement2_S3Janitor_DrainGoroutine_NoPanic(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)

	for i := range 3 {
		mustCreateBucket(t, b, bucketName(i))
		for j := range 5 {
			mustPutObject(t, b, bucketName(i), keyName(i, j), []byte("data"))
		}
	}

	j := newFastJanitor(b)
	require.NotPanics(t, func() { j.SweepOnce(t.Context()) })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func bucketName(i int) string { return fmt.Sprintf("r2-bucket-%d", i) }
func keyName(i, j int) string { return fmt.Sprintf("dir/obj-%d-%d.txt", i, j) }

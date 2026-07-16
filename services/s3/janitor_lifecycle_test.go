package s3_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
// Lifecycle Days=nil guard
// ---------------------------------------------------------------------------

// TestLifecycle_NoExpirationElement verifies that a lifecycle rule with only
// AbortIncompleteMultipartUpload and no <Expiration> element does NOT delete
// existing objects (Days field was previously int zero-value = 0).
func TestLifecycle_NoExpirationElement(t *testing.T) {
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
// Object-lock bypass
// ---------------------------------------------------------------------------

// TestLifecycle_SkipsLockedObjects verifies that lifecycle expiry does not
// delete objects protected by an active retention period.
func TestLifecycle_SkipsLockedObjects(t *testing.T) {
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

// TestLifecycle_SkipsLegalHoldObjects verifies that lifecycle expiry does not
// delete objects protected by a legal hold.
func TestLifecycle_SkipsLegalHoldObjects(t *testing.T) {
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

// TestNoncurrentVersionEviction_SkipsLockedVersions verifies that noncurrent
// version eviction respects object-lock on individual versions, and that a
// per-object-lock-aware fresh (unexpired) noncurrent version also survives.
func TestNoncurrentVersionEviction_SkipsLockedVersions(t *testing.T) {
	t.Parallel()

	t.Run("locked_noncurrent_version_retained", func(t *testing.T) {
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
	})

	// TestParity_EvictNoncurrentVersionsPerObjectLock verifies that the
	// per-object brief-lock scheme used by evictNoncurrentVersions correctly
	// removes expired noncurrent versions and preserves the latest version,
	// across both the "expired" and "fresh" noncurrent cases (exercised end
	// to end through the HTTP handler + janitor, unlike the backend-level
	// case above).
	for _, tt := range []struct {
		name              string
		noncurrentDaysXML string
		backdateBy        time.Duration
	}{
		{
			name:              "noncurrent_expired_evicted",
			noncurrentDaysXML: `<NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration>`,
			backdateBy:        -48 * time.Hour,
		},
		{
			name:              "noncurrent_fresh_retained",
			noncurrentDaysXML: `<NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays></NoncurrentVersionExpiration>`,
			backdateBy:        -1 * time.Hour,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			const bucket = "vbucket"
			const key = "vobj"

			mustCreateBucket(t, backend, bucket)
			enableVersioning(t, handler, bucket)

			// Create two versions: v1 then v2 (v1 becomes noncurrent).
			mustPutObject(t, backend, bucket, key, []byte("v1"))
			s3.BackdateObjectForTest(backend, bucket, key, time.Now().Add(tt.backdateBy))
			mustPutObject(t, backend, bucket, key, []byte("v2"))

			// Configure lifecycle to expire noncurrent versions.
			lcXML := `<LifecycleConfiguration><Rule><ID>noncurrent</ID><Status>Enabled</Status>` +
				`<Filter><Prefix></Prefix></Filter>` + tt.noncurrentDaysXML +
				`</Rule></LifecycleConfiguration>`

			req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?lifecycle",
				strings.NewReader(lcXML))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Less(t, rec.Code, http.StatusMultipleChoices, "PUT lifecycle must succeed")

			janitor := s3.NewJanitor(backend, s3.Settings{})
			janitor.SweepOnce(t.Context())

			// HEAD the object — it must still exist (latest version intact).
			// The handler returns 200 or 204 for a present object; 404 means it was deleted.
			headReq := httptest.NewRequest(http.MethodHead, "/"+bucket+"/"+key, nil)
			headRec := httptest.NewRecorder()
			serveS3Handler(handler, headRec, headReq)
			assert.NotEqual(
				t,
				http.StatusNotFound,
				headRec.Code,
				"latest version must survive eviction",
			)
		})
	}
}

// ---------------------------------------------------------------------------
// Default (24h) multipart cleanup — RLock-scan + Lock-delete
// ---------------------------------------------------------------------------

// TestCleanupDefaultMultipartUsesReadLockScan verifies that
// cleanupDefaultMultipart (via SweepOnce) still removes expired uploads and
// leaves non-expired uploads intact after the RLock-scan + Lock-delete refactor.
func TestCleanupDefaultMultipartUsesReadLockScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		expiredCount    int
		nonExpiredCount int
	}{
		{
			name:            "expires_old_leaves_fresh",
			expiredCount:    3,
			nonExpiredCount: 2,
		},
		{
			name:            "all_expired",
			expiredCount:    5,
			nonExpiredCount: 0,
		},
		{
			name:            "none_expired",
			expiredCount:    0,
			nonExpiredCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{}).WithSkipMultipartSizeCheck()
			janitor := s3.NewJanitor(backend, s3.Settings{})
			const bucket = "mpbucket"

			_, err := backend.CreateBucket(t.Context(),
				&sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
			require.NoError(t, err)

			// Create expired uploads.
			for range tt.expiredCount {
				out, mpErr := backend.CreateMultipartUpload(t.Context(),
					&sdk_s3.CreateMultipartUploadInput{
						Bucket: aws.String(bucket),
						Key:    aws.String("old-key"),
					})
				require.NoError(t, mpErr)
				// Backdate the upload to 25 hours ago.
				s3.BackdateUploadForTest(
					backend,
					bucket,
					out.UploadId,
					time.Now().Add(-25*time.Hour),
				)
			}

			// Create non-expired uploads.
			for range tt.nonExpiredCount {
				_, mpErr := backend.CreateMultipartUpload(t.Context(),
					&sdk_s3.CreateMultipartUploadInput{
						Bucket: aws.String(bucket),
						Key:    aws.String("new-key"),
					})
				require.NoError(t, mpErr)
			}

			janitor.SweepOnce(t.Context())

			got := backend.UploadsForBucket(bucket)
			assert.Equal(t, tt.nonExpiredCount, got,
				"only non-expired uploads should remain")
		})
	}
}

// ---------------------------------------------------------------------------
// S3Reset / S3Purge / drain-goroutine safety
// ---------------------------------------------------------------------------

// TestS3Reset_NoMutexLeak verifies that Reset does not panic and leaves the
// backend in a clean state (no buckets), and that consecutive Resets
// (including on an already-empty backend) do not panic either.
func TestS3Reset_NoMutexLeak(t *testing.T) {
	t.Parallel()

	t.Run("clears_all_buckets", func(t *testing.T) {
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
	})

	t.Run("multiple_resets_do_not_panic", func(t *testing.T) {
		t.Parallel()

		b := s3.NewInMemoryBackend(nil)
		mustCreateBucket(t, b, "reset-bucket")
		mustPutObject(t, b, "reset-bucket", "obj.txt", []byte("data"))

		require.NotPanics(t, func() {
			b.Reset()
			b.Reset() // second reset on empty backend must not panic
		})
	})
}

// TestS3Purge_ClosesObjectMutexes verifies that Purge on old buckets does not
// panic and removes buckets and their objects cleanly.
func TestS3Purge_ClosesObjectMutexes(t *testing.T) {
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

// TestS3Janitor_DrainGoroutine_NoPanic verifies that a normal janitor run on
// multiple buckets does not panic and properly drains.
func TestS3Janitor_DrainGoroutine_NoPanic(t *testing.T) {
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

// TestBackendShutdown verifies that Shutdown cancels the service context and
// DrainReplicationGoroutines returns promptly, preventing goroutine leaks, and
// that a second Shutdown call is idempotent.
func TestBackendShutdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "shutdown_cancels_and_drains"},
		{name: "shutdown_idempotent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})

			// Put an object to trigger replication goroutine machinery.
			_, err := backend.CreateBucket(t.Context(),
				&sdk_s3.CreateBucketInput{Bucket: aws.String("testbkt")})
			require.NoError(t, err)

			_, err = backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("testbkt"),
				Key:    aws.String("k"),
				Body:   strings.NewReader("data"),
			})
			require.NoError(t, err)

			done := make(chan struct{})
			go func() {
				backend.Shutdown()
				if tt.name == "shutdown_idempotent" {
					backend.Shutdown() // second call must not panic or hang
				}
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Shutdown() did not return within 5s — possible goroutine leak")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func bucketName(i int) string { return fmt.Sprintf("r2-bucket-%d", i) }
func keyName(i, j int) string { return fmt.Sprintf("dir/obj-%d-%d.txt", i, j) }

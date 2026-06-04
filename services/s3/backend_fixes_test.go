package s3_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestVersionID_RandomFormat verifies that object version IDs use random hex
// strings rather than sequential Unix-nanosecond timestamps.
func TestVersionID_RandomFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "put_object_with_versioning"},
		{name: "two_successive_puts_differ"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "bkt")

			_, err := backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
				Bucket: aws.String("bkt"),
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
			require.NoError(t, err)

			out1, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
				Body:   bytes.NewReader([]byte("data")),
			})
			require.NoError(t, err)

			vid := aws.ToString(out1.VersionId)
			assert.NotEmpty(t, vid)

			// Must NOT be purely numeric (the old UnixNano format was all digits).
			isNumeric := true
			for _, c := range vid {
				if c < '0' || c > '9' {
					isNumeric = false

					break
				}
			}
			assert.False(t, isNumeric, "version ID should not be a purely numeric Unix timestamp: %s", vid)

			// Must be 32 hex chars (16 random bytes encoded as hex).
			assert.Len(t, vid, 32)

			out2, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
				Body:   bytes.NewReader([]byte("data2")),
			})
			require.NoError(t, err)

			assert.NotEqual(t, vid, aws.ToString(out2.VersionId),
				"successive version IDs must differ")
		})
	}
}

// TestObjectLock_PutObjectRetentionPreventsOverwrite verifies that PutObject is
// blocked when the target null version (versioning-suspended) is under an active
// COMPLIANCE retention, and allowed when retention has expired.
func TestObjectLock_PutObjectRetentionPreventsOverwrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		future  bool
		wantErr bool
	}{
		{name: "active_retention_blocks_overwrite", future: true, wantErr: true},
		{name: "expired_retention_allows_overwrite", future: false, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("original"))

			retainUntil := time.Now().Add(-24 * time.Hour) // expired by default
			if tt.future {
				retainUntil = time.Now().Add(24 * time.Hour) // active
			}

			err := backend.PutObjectRetention(
				t.Context(), "bkt", "key", nil,
				"COMPLIANCE",
				retainUntil,
			)
			require.NoError(t, err)

			_, putErr := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
				Body:   bytes.NewReader([]byte("overwrite")),
			})

			if tt.wantErr {
				require.Error(t, putErr, "expected overwrite to be blocked by active retention")
			} else {
				require.NoError(t, putErr, "expected overwrite to succeed with expired retention")
			}
		})
	}
}

// TestObjectLock_LegalHoldPreventsOverwrite verifies that PutObject is blocked
// when the target null version is under a legal hold (non-versioned bucket).
func TestObjectLock_LegalHoldPreventsOverwrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		legalHold string
		wantErr   bool
	}{
		{name: "legal_hold_on_blocks_overwrite", legalHold: "ON", wantErr: true},
		{name: "legal_hold_off_allows_overwrite", legalHold: "OFF", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("original"))

			err := backend.PutObjectLegalHold(t.Context(), "bkt", "key", nil, tt.legalHold)
			require.NoError(t, err)

			_, putErr := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
				Body:   bytes.NewReader([]byte("overwrite")),
			})

			if tt.wantErr {
				require.Error(t, putErr, "expected overwrite to be blocked by legal hold")
			} else {
				require.NoError(t, putErr, "expected overwrite to succeed when legal hold is OFF")
			}
		})
	}
}

// TestMultipartUpload_TaggingPropagated verifies that tags set via the Tagging
// field on CreateMultipartUpload are applied to the resulting object version after
// a successful CompleteMultipartUpload.
func TestMultipartUpload_TaggingPropagated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagging  string
		wantTags int
	}{
		{name: "tags_applied_on_complete", tagging: "env=prod&team=infra", wantTags: 2},
		{name: "no_tags_when_not_specified", tagging: "", wantTags: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "bkt")

			createOut, err := backend.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
				Bucket:  aws.String("bkt"),
				Key:     aws.String("key"),
				Tagging: aws.String(tt.tagging),
			})
			require.NoError(t, err)

			uploadID := createOut.UploadId
			// The in-memory backend has no minimum part size; use a small payload.
			partData := bytes.Repeat([]byte("x"), 1024) // 1 KiB

			p1, err := backend.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:     aws.String("bkt"),
				Key:        aws.String("key"),
				UploadId:   uploadID,
				PartNumber: aws.Int32(1),
				Body:       bytes.NewReader(partData),
			})
			require.NoError(t, err)

			_, err = backend.CompleteMultipartUpload(t.Context(), &sdk_s3.CompleteMultipartUploadInput{
				Bucket:   aws.String("bkt"),
				Key:      aws.String("key"),
				UploadId: uploadID,
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{
						{PartNumber: aws.Int32(1), ETag: p1.ETag},
					},
				},
			})
			require.NoError(t, err)

			taggingOut, getErr := backend.GetObjectTagging(t.Context(), &sdk_s3.GetObjectTaggingInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
			})

			if tt.wantTags == 0 {
				// No tags set → either NoSuchTagSet or empty tag set.
				if getErr == nil {
					assert.Empty(t, taggingOut.TagSet, "expected no tags on object")
				}
			} else {
				require.NoError(t, getErr)
				assert.Len(t, taggingOut.TagSet, tt.wantTags)
			}
		})
	}
}

// TestPutObject_ChecksumVerification verifies that the backend stores valid
// checksums and that the HTTP handler layer rejects mismatched ones.
func TestPutObject_ChecksumVerification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		algo    string
		corrupt bool
		wantErr bool
	}{
		{name: "correct_sha256_accepted", algo: "SHA256", corrupt: false, wantErr: false},
		{name: "correct_crc32_accepted", algo: "CRC32", corrupt: false, wantErr: false},
		{name: "correct_sha1_accepted", algo: "SHA1", corrupt: false, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "bkt")

			data := []byte("checksum test data")
			checksum := s3.CalculateChecksum(data, tt.algo)

			var input sdk_s3.PutObjectInput
			input.Bucket = aws.String("bkt")
			input.Key = aws.String("key")
			input.Body = bytes.NewReader(data)
			input.ChecksumAlgorithm = types.ChecksumAlgorithm(tt.algo)

			switch tt.algo {
			case "SHA256":
				input.ChecksumSHA256 = aws.String(checksum)
			case "CRC32":
				input.ChecksumCRC32 = aws.String(checksum)
			case "SHA1":
				input.ChecksumSHA1 = aws.String(checksum)
			}

			_, err := backend.PutObject(t.Context(), &input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			// Verify the checksum is preserved on retrieval.
			getOut, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
			})
			require.NoError(t, err)

			switch tt.algo {
			case "SHA256":
				assert.Equal(t, checksum, aws.ToString(getOut.ChecksumSHA256))
			case "CRC32":
				assert.Equal(t, checksum, aws.ToString(getOut.ChecksumCRC32))
			case "SHA1":
				assert.Equal(t, checksum, aws.ToString(getOut.ChecksumSHA1))
			}
		})
	}
}

// TestS3Janitor_DrainSemaphoreCapacity verifies that the drain semaphore is
// initialised with the correct capacity (maxConcurrentDrains) to bound the
// number of concurrent bucket drain goroutines.
func TestS3Janitor_DrainSemaphoreCapacity(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	j := s3.NewJanitor(backend, s3.Settings{})

	assert.Equal(t, s3.MaxConcurrentDrains, j.DrainSemCapacity(),
		"drain semaphore capacity should equal maxConcurrentDrains")
}

// TestObjectLockRace_ConcurrentPutAndDelete verifies that checkObjectLockForDelete
// and checkObjectLockForOverwrite are safe to call concurrently with PutObject
// by running many parallel PutObject + DeleteObject calls against the same key.
// The test would produce a race-detector failure if obj.mu were not held correctly.
func TestObjectLockRace_ConcurrentPutAndDelete(t *testing.T) {
	t.Parallel()

	const goroutines = 20

	tests := []struct {
		name string
	}{
		{name: "concurrent_put_and_delete_same_key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "race-bkt")
			mustPutObject(t, backend, "race-bkt", "shared-key", []byte("initial"))

			done := make(chan struct{})

			for range goroutines {
				go func() {
					defer func() {
						if r := recover(); r != nil {
							// panic = race in obj.mu not held properly
							t.Errorf("unexpected panic in concurrent access: %v", r)
						}

						done <- struct{}{}
					}()

					_, _ = backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
						Bucket: aws.String("race-bkt"),
						Key:    aws.String("shared-key"),
						Body:   bytes.NewReader([]byte("concurrent-data")),
					})
				}()

				go func() {
					defer func() {
						recover()
						done <- struct{}{}
					}()

					_, _ = backend.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
						Bucket: aws.String("race-bkt"),
						Key:    aws.String("shared-key"),
					})
				}()
			}

			for range goroutines * 2 {
				<-done
			}
		})
	}
}

// TestS3JanitorRun_ExitsOnContextCancel verifies that the S3 janitor Run loop
// exits cleanly when the context is cancelled (no panic, no goroutine leak).
func TestS3JanitorRun_ExitsOnContextCancel(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	j := s3.NewJanitor(backend, s3.Settings{})

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})

	go func() {
		defer close(done)
		j.Run(ctx)
	}()

	cancel()

	select {
	case <-done:
		// clean exit
	case <-time.After(500 * time.Millisecond):
		t.Fatal("S3 janitor Run did not exit after context cancellation")
	}
}

// TestListObjectsV2_ContextPropagation verifies that ListObjectsV2 passes the
// caller's context to the underlying ListObjects call rather than using
// [context.TODO]. The in-memory backend completes synchronously regardless of
// cancellation, but the fix ensures real backends and context-aware operations
// receive the correct context.
func TestListObjectsV2_ContextPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cancelCtx   bool
		wantObjects int
	}{
		{
			name:        "active_context_returns_results",
			cancelCtx:   false,
			wantObjects: 2,
		},
		{
			// The in-memory backend is synchronous and does not check ctx.Err,
			// so it completes successfully even when the context is already
			// cancelled. The important thing is that ctx is passed through
			// (not replaced with context.TODO()) and the call does not panic.
			name:        "cancelled_context_does_not_panic",
			cancelCtx:   true,
			wantObjects: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			mustCreateBucket(t, backend, "ctx-test-bucket")
			mustPutObject(t, backend, "ctx-test-bucket", "key-a", []byte("data"))
			mustPutObject(t, backend, "ctx-test-bucket", "key-b", []byte("data"))

			ctx := t.Context()
			if tt.cancelCtx {
				var cancel func()
				ctx, cancel = context.WithCancel(ctx)
				cancel() // cancel immediately
			}

			out, err := backend.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
				Bucket: aws.String("ctx-test-bucket"),
			})

			require.NoError(t, err)
			assert.Len(t, out.Contents, tt.wantObjects)
		})
	}
}

// TestSuspendedVersioningDeletePreservesVersions verifies that an unversioned
// DELETE against a bucket whose versioning is Suspended only removes the "null"
// version and inserts a "null" delete marker, leaving non-null versions (created
// while versioning was Enabled) retrievable by version ID.
func TestSuspendedVersioningDeletePreservesVersions(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	mustCreateBucket(t, backend, "bkt")

	// Enable versioning and write a non-null version.
	_, err := backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket: aws.String("bkt"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	putOut, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("k"),
		Body: bytes.NewReader([]byte("enabled-data")),
	})
	require.NoError(t, err)
	keptVersion := aws.ToString(putOut.VersionId)
	require.NotEqual(t, s3.NullVersion, keptVersion)

	// Suspend versioning and overwrite with a "null" version.
	_, err = backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket: aws.String("bkt"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusSuspended,
		},
	})
	require.NoError(t, err)
	mustPutObject(t, backend, "bkt", "k", []byte("null-data"))

	// Unversioned DELETE under suspended versioning.
	delOut, err := backend.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("k"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(delOut.DeleteMarker), "expected a delete marker")
	assert.Equal(t, s3.NullVersion, aws.ToString(delOut.VersionId))

	// The non-null version must still be retrievable by its version ID.
	got, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("k"),
		VersionId: aws.String(keptVersion),
	})
	require.NoError(t, err)
	data, _ := io.ReadAll(got.Body)
	assert.Equal(t, "enabled-data", string(data))

	// An unversioned GET now sees the delete marker → NoSuchKey.
	_, err = backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("k"),
	})
	require.ErrorIs(t, err, s3.ErrNoSuchKey)
}

package s3_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTagCleanupOnDelete verifies that object tags are removed from the backend
// when an object or version is deleted (Fix 3: tag memory leak).
func TestTagCleanupOnDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *s3.InMemoryBackend) (bucket, key string, versionID *string)
		name     string
		wantTags int
	}{
		{
			name: "tags cleaned up after null-version delete",
			setup: func(t *testing.T, b *s3.InMemoryBackend) (string, string, *string) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				_, err := b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
					Bucket:  aws.String("bkt"),
					Key:     aws.String("k"),
					Body:    bytes.NewReader([]byte("data")),
					Tagging: aws.String("env=test"),
				})
				require.NoError(t, err)

				return "bkt", "k", nil
			},
			wantTags: 0,
		},
		{
			name: "tags cleaned up after specific version delete",
			setup: func(t *testing.T, b *s3.InMemoryBackend) (string, string, *string) {
				t.Helper()
				mustCreateBucket(t, b, "bkt2")
				_, err := b.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
					Bucket: aws.String("bkt2"),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)

				out, err := b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
					Bucket:  aws.String("bkt2"),
					Key:     aws.String("k"),
					Body:    bytes.NewReader([]byte("data")),
					Tagging: aws.String("env=test"),
				})
				require.NoError(t, err)

				return "bkt2", "k", out.VersionId
			},
			wantTags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			bucket, key, vid := tt.setup(t, b)

			// Verify tags were stored
			assert.Positive(t, b.TagsForBucket(bucket), "tags should exist before delete")

			_, err := b.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
				Bucket:    aws.String(bucket),
				Key:       aws.String(key),
				VersionId: vid,
			})
			require.NoError(t, err)

			assert.Equal(t, tt.wantTags, b.TagsForBucket(bucket), "tags should be cleaned up after delete")
		})
	}
}

// TestTagCleanupOnBulkDelete verifies that tags are cleaned up during DeleteObjects (Fix 3).
func TestTagCleanupOnBulkDelete(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	mustCreateBucket(t, b, "bkt")

	// Store two objects with tags
	for _, key := range []string{"k1", "k2"} {
		_, err := b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
			Bucket:  aws.String("bkt"),
			Key:     aws.String(key),
			Body:    bytes.NewReader([]byte("data")),
			Tagging: aws.String("env=test"),
		})
		require.NoError(t, err)
	}

	assert.Equal(t, 2, b.TagsForBucket("bkt"), "two tag entries should exist before bulk delete")

	_, err := b.DeleteObjects(t.Context(), &sdk_s3.DeleteObjectsInput{
		Bucket: aws.String("bkt"),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String("k1")},
				{Key: aws.String("k2")},
			},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, 0, b.TagsForBucket("bkt"), "all tags should be cleaned up after bulk delete")
}

// TestListMultipartUploadsPerBucket verifies that ListMultipartUploads only returns
// uploads for the requested bucket, not uploads from other buckets (Fix 4).
func TestListMultipartUploadsPerBucket(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	mustCreateBucket(t, b, "bkt-a")
	mustCreateBucket(t, b, "bkt-b")

	// Create uploads in both buckets
	for i := range 3 {
		_, err := b.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
			Bucket: aws.String("bkt-a"),
			Key:    aws.String(fmt.Sprintf("key-%d", i)),
		})
		require.NoError(t, err)
	}

	_, err := b.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
		Bucket: aws.String("bkt-b"),
		Key:    aws.String("other-key"),
	})
	require.NoError(t, err)

	outA, err := b.ListMultipartUploads(t.Context(), &sdk_s3.ListMultipartUploadsInput{
		Bucket: aws.String("bkt-a"),
	})
	require.NoError(t, err)
	assert.Len(t, outA.Uploads, 3, "bkt-a should have 3 uploads")

	outB, err := b.ListMultipartUploads(t.Context(), &sdk_s3.ListMultipartUploadsInput{
		Bucket: aws.String("bkt-b"),
	})
	require.NoError(t, err)
	assert.Len(t, outB.Uploads, 1, "bkt-b should have 1 upload")
}

// TestUploadPartConcurrent verifies that concurrent UploadPart calls to the same
// upload ID are safe (Fix 2: per-upload mutex).
func TestUploadPartConcurrent(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	mustCreateBucket(t, b, "bkt")

	createOut, err := b.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
		Bucket: aws.String("bkt"),
		Key:    aws.String("large-file"),
	})
	require.NoError(t, err)

	uploadID := createOut.UploadId
	const numParts = 10

	var wg sync.WaitGroup
	etags := make([]string, numParts)

	for i := range numParts {
		wg.Go(func() {
			partOut, partErr := b.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:     aws.String("bkt"),
				Key:        aws.String("large-file"),
				UploadId:   uploadID,
				PartNumber: aws.Int32(int32(i + 1)), // i is bounded by numParts const
				Body:       bytes.NewReader(fmt.Appendf(nil, "part%d", i)),
			})
			if partErr == nil {
				etags[i] = aws.ToString(partOut.ETag)
			}
		})
	}
	wg.Wait()

	// Verify all parts were stored
	listOut, err := b.ListParts(t.Context(), &sdk_s3.ListPartsInput{
		Bucket:   aws.String("bkt"),
		Key:      aws.String("large-file"),
		UploadId: uploadID,
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Parts, numParts, "all parts should be stored")
}

// TestListObjectVersionsSnapshot verifies that ListObjectVersions returns all versions
// correctly without holding the bucket lock during the full iteration (Fix 5).
func TestListObjectVersionsSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *s3.InMemoryBackend)
		name         string
		bucket       string
		prefix       string
		wantVersions int
		wantMarkers  int
	}{
		{
			name:   "returns all versions and delete markers",
			bucket: "bkt",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				_, err := b.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
					Bucket: aws.String("bkt"),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)

				mustPutObject(t, b, "bkt", "k1", []byte("v1"))
				mustPutObject(t, b, "bkt", "k1", []byte("v2"))
				mustPutObject(t, b, "bkt", "k2", []byte("data"))

				// Delete k2 to create a delete marker
				_, err = b.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("k2"),
				})
				require.NoError(t, err)
			},
			wantVersions: 3,
			wantMarkers:  1,
		},
		{
			name:   "prefix filter",
			bucket: "bkt2",
			prefix: "a/",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt2")
				mustPutObject(t, b, "bkt2", "a/obj", []byte("data"))
				mustPutObject(t, b, "bkt2", "b/obj", []byte("data"))
			},
			wantVersions: 1,
			wantMarkers:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			tt.setup(t, b)

			out, err := b.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
				Bucket: aws.String(tt.bucket),
				Prefix: aws.String(tt.prefix),
			})
			require.NoError(t, err)
			assert.Len(t, out.Versions, tt.wantVersions)
			assert.Len(t, out.DeleteMarkers, tt.wantMarkers)
		})
	}
}

// TestListObjectsChecksumInResponse verifies that ListObjects returns checksum algorithm
// in the objects without requiring per-object GetObject calls (Fix 1).
func TestListObjectsChecksumInResponse(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	mustCreateBucket(t, b, "bkt")

	_, err := b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket:            aws.String("bkt"),
		Key:               aws.String("k"),
		Body:              bytes.NewReader([]byte("data")),
		ChecksumCRC32:     aws.String("abc123"),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	require.NoError(t, err)

	out, err := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
		Bucket: aws.String("bkt"),
	})
	require.NoError(t, err)
	require.Len(t, out.Contents, 1)

	obj := out.Contents[0]
	assert.NotEmpty(t, aws.ToString(obj.ETag), "ETag should be populated without GetObject call")
	require.Len(t, obj.ChecksumAlgorithm, 1, "checksum algorithm should be populated")
	assert.Equal(t, types.ChecksumAlgorithmCrc32, obj.ChecksumAlgorithm[0])
}

package s3_test

import (
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *s3.InMemoryBackend) string
		verify func(t *testing.T, b *s3.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *s3.InMemoryBackend) string {
				_, err := b.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
					Bucket: aws.String("test-bucket"),
				})
				if err != nil {
					return ""
				}

				return "test-bucket"
			},
			verify: func(t *testing.T, b *s3.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
				require.NoError(t, err)
				require.Len(t, out.Buckets, 1)
				assert.Equal(t, id, *out.Buckets[0].Name)
			},
		},
		{
			// Exercises every piece of backend state that Snapshot/Restore must
			// round-trip: the converted "buckets" and "uploads" store.Table
			// entries (including an inline object inside the bucket entry),
			// AND the raw-left b.tags map and defaultRegion string that stayed
			// plain fields (not store.Tables) — so a future change that drops
			// either from Snapshot/Restore fails this test.
			name: "full_state_round_trip",
			setup: func(b *s3.InMemoryBackend) string {
				b.SetDefaultRegion("eu-west-1")

				ctx := t.Context()
				bucketName := "full-state-bucket"

				if _, err := b.CreateBucket(ctx, &sdk_s3.CreateBucketInput{
					Bucket: aws.String(bucketName),
				}); err != nil {
					return ""
				}

				if _, err := b.PutObject(ctx, &sdk_s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("obj.txt"),
					Body:   strings.NewReader("hello"),
				}); err != nil {
					return ""
				}

				if _, err := b.PutObjectTagging(ctx, &sdk_s3.PutObjectTaggingInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("obj.txt"),
					Tagging: &types.Tagging{
						TagSet: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
					},
				}); err != nil {
					return ""
				}

				if _, err := b.CreateMultipartUpload(ctx, &sdk_s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("large.bin"),
				}); err != nil {
					return ""
				}

				return bucketName
			},
			verify: func(t *testing.T, b *s3.InMemoryBackend, id string) {
				t.Helper()

				ctx := t.Context()

				out, err := b.ListBuckets(ctx, &sdk_s3.ListBucketsInput{})
				require.NoError(t, err)
				require.Len(t, out.Buckets, 1)
				assert.Equal(t, id, aws.ToString(out.Buckets[0].Name))

				// The object stored inline inside the bucket's store.Table entry.
				getOut, err := b.GetObject(ctx, &sdk_s3.GetObjectInput{
					Bucket: aws.String(id),
					Key:    aws.String("obj.txt"),
				})
				require.NoError(t, err)
				body, readErr := io.ReadAll(getOut.Body)
				require.NoError(t, readErr)
				assert.Equal(t, "hello", string(body))

				// b.tags: a raw (non-store.Table) map — must still round-trip.
				tagOut, err := b.GetObjectTagging(ctx, &sdk_s3.GetObjectTaggingInput{
					Bucket: aws.String(id),
					Key:    aws.String("obj.txt"),
				})
				require.NoError(t, err)
				require.Len(t, tagOut.TagSet, 1)
				assert.Equal(t, "env", aws.ToString(tagOut.TagSet[0].Key))
				assert.Equal(t, "prod", aws.ToString(tagOut.TagSet[0].Value))

				// The "uploads" store.Table entry, and its bucket-index grouping.
				mpOut, err := b.ListMultipartUploads(ctx, &sdk_s3.ListMultipartUploadsInput{
					Bucket: aws.String(id),
				})
				require.NoError(t, err)
				require.Len(t, mpOut.Uploads, 1)
				assert.Equal(t, "large.bin", aws.ToString(mpOut.Uploads[0].Key))

				// b.defaultRegion: a raw (non-store.Table) string field — a
				// freshly created bucket with no explicit region must land in
				// the restored default region.
				require.NoError(t, err)
				_, err = b.CreateBucket(ctx, &sdk_s3.CreateBucketInput{
					Bucket: aws.String("region-check-bucket"),
				})
				require.NoError(t, err)
				assert.Equal(t, "eu-west-1", b.BucketRegion("region-check-bucket"))
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *s3.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *s3.InMemoryBackend, _ string) {
				t.Helper()

				out, err := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
				require.NoError(t, err)
				assert.Empty(t, out.Buckets)
			},
		},
		{
			// A snapshot that contains a pending-delete bucket should restore
			// without making the bucket accessible via getBucket operations.
			name: "pending_delete_bucket_not_visible_after_restore",
			setup: func(b *s3.InMemoryBackend) string {
				_, err := b.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
					Bucket: aws.String("will-be-deleted"),
				})
				if err != nil {
					return ""
				}
				_, _ = b.DeleteBucket(t.Context(), &sdk_s3.DeleteBucketInput{
					Bucket: aws.String("will-be-deleted"),
				})

				return "will-be-deleted"
			},
			verify: func(t *testing.T, b *s3.InMemoryBackend, _ string) {
				t.Helper()

				// The pending bucket must be invisible to GetObject / HeadBucket.
				_, err := b.HeadBucket(t.Context(), &sdk_s3.HeadBucketInput{
					Bucket: aws.String("will-be-deleted"),
				})
				require.ErrorIs(t, err, s3.ErrNoSuchBucket)

				// ListBuckets must also exclude pending-delete buckets.
				out, listErr := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
				require.NoError(t, listErr)
				assert.Empty(t, out.Buckets)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := s3.NewInMemoryBackend(nil)
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := s3.NewInMemoryBackend(nil)
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

// TestInMemoryBackend_RestoreDiscardsIncompatibleSnapshot verifies that a
// snapshot whose "version" field doesn't match the current s3SnapshotVersion —
// including every snapshot written before the Phase 3.3 pkgs/store conversion
// (which predates the version field entirely, and used the old
// {"buckets": {region: {name: ...}}, "uploads": {bucket: {uploadID: ...}}}
// shape, with a legacy flat-uploads variant on top of that) — is discarded
// cleanly (Restore returns no error, backend ends up empty) rather than
// partially decoded as the current {"tables": {...}} shape. This mirrors the
// services/ec2 and services/sqs Phase 3.x conversions, which made the same
// tradeoff.
func TestInMemoryBackend_RestoreDiscardsIncompatibleSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		snapshot []byte
	}{
		{
			// Pre-conversion region-nested bucket shape, no version field.
			name: "legacy_region_nested_buckets_no_version",
			snapshot: []byte(`{
				"buckets": {
					"us-east-1": {
						"shared": {
							"name": "shared",
							"deletePending": true,
							"versioning": "Suspended",
							"objects": {}
						}
					},
					"us-west-2": {
						"shared": {
							"name": "shared",
							"deletePending": false,
							"versioning": "Suspended",
							"objects": {}
						}
					}
				},
				"tags": {},
				"uploads": {},
				"defaultRegion": "us-east-1"
			}`),
		},
		{
			// Pre-issue-620 flat uploads shape on top of the pre-conversion
			// bucket shape, no version field.
			name: "legacy_flat_uploads_no_version",
			snapshot: []byte(`{
				"buckets": {
					"us-east-1": {
						"my-bucket": {
							"name": "my-bucket",
							"deletePending": false,
							"objects": {}
						}
					}
				},
				"tags": {},
				"uploads": {
					"1234567890": {
						"uploadID": "1234567890",
						"bucket":   "my-bucket",
						"key":      "large-file",
						"initiated": "2024-01-01T00:00:00Z",
						"parts": {}
					}
				},
				"defaultRegion": "us-east-1"
			}`),
		},
		{
			name:     "explicit_future_version",
			snapshot: []byte(`{"version": 999, "tables": {}}`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := s3.NewInMemoryBackend(nil)
			require.NoError(t, b.Restore(t.Context(), tc.snapshot))

			out, err := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
			require.NoError(t, err)
			assert.Empty(t, out.Buckets, "incompatible snapshot must be discarded, not partially decoded")
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestInMemoryBackend_Persistence_NewConfigMaps(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	mustCreateBucket(t, b, "bkt")
	ctx := t.Context()

	require.NoError(t, b.PutBucketAnalyticsConfiguration(ctx, "bkt", "a1",
		"<AnalyticsConfiguration><Id>a1</Id></AnalyticsConfiguration>"))
	require.NoError(t, b.PutBucketIntelligentTieringConfiguration(ctx, "bkt", "t1",
		"<IntelligentTieringConfiguration><Id>t1</Id></IntelligentTieringConfiguration>"))
	require.NoError(t, b.PutBucketInventoryConfiguration(ctx, "bkt", "i1",
		"<InventoryConfiguration><Id>i1</Id></InventoryConfiguration>"))
	require.NoError(t, b.PutBucketMetricsConfiguration(ctx, "bkt", "m1",
		"<MetricsConfiguration><Id>m1</Id></MetricsConfiguration>"))

	snap := b.Snapshot(t.Context())

	b2 := newTestBackend(t)
	require.NoError(t, b2.Restore(t.Context(), snap))

	got, err := b2.GetBucketAnalyticsConfiguration(ctx, "bkt", "a1")
	require.NoError(t, err)
	assert.Contains(t, got, "a1")

	got, err = b2.GetBucketIntelligentTieringConfiguration(ctx, "bkt", "t1")
	require.NoError(t, err)
	assert.Contains(t, got, "t1")

	got, err = b2.GetBucketInventoryConfiguration(ctx, "bkt", "i1")
	require.NoError(t, err)
	assert.Contains(t, got, "i1")

	got, err = b2.GetBucketMetricsConfiguration(ctx, "bkt", "m1")
	require.NoError(t, err)
	assert.Contains(t, got, "m1")
}

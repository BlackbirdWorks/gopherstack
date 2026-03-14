package s3_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				_, err := b.CreateBucket(context.Background(), &sdk_s3.CreateBucketInput{
					Bucket: aws.String("test-bucket"),
				})
				if err != nil {
					return ""
				}

				return "test-bucket"
			},
			verify: func(t *testing.T, b *s3.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.ListBuckets(context.Background(), &sdk_s3.ListBucketsInput{})
				require.NoError(t, err)
				require.Len(t, out.Buckets, 1)
				assert.Equal(t, id, *out.Buckets[0].Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *s3.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *s3.InMemoryBackend, _ string) {
				t.Helper()

				out, err := b.ListBuckets(context.Background(), &sdk_s3.ListBucketsInput{})
				require.NoError(t, err)
				assert.Empty(t, out.Buckets)
			},
		},
		{
			// A snapshot that contains a pending-delete bucket should restore
			// without making the bucket accessible via getBucket operations.
			name: "pending_delete_bucket_not_visible_after_restore",
			setup: func(b *s3.InMemoryBackend) string {
				_, err := b.CreateBucket(context.Background(), &sdk_s3.CreateBucketInput{
					Bucket: aws.String("will-be-deleted"),
				})
				if err != nil {
					return ""
				}
				_, _ = b.DeleteBucket(context.Background(), &sdk_s3.DeleteBucketInput{
					Bucket: aws.String("will-be-deleted"),
				})

				return "will-be-deleted"
			},
			verify: func(t *testing.T, b *s3.InMemoryBackend, _ string) {
				t.Helper()

				// The pending bucket must be invisible to GetObject / HeadBucket.
				_, err := b.HeadBucket(context.Background(), &sdk_s3.HeadBucketInput{
					Bucket: aws.String("will-be-deleted"),
				})
				require.ErrorIs(t, err, s3.ErrNoSuchBucket)

				// ListBuckets must also exclude pending-delete buckets.
				out, listErr := b.ListBuckets(context.Background(), &sdk_s3.ListBucketsInput{})
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

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := s3.NewInMemoryBackend(nil)
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

// TestInMemoryBackend_RestoreActivePrecedesOverPending verifies that
// buildBucketIndex prefers the active bucket over a pending-delete entry when
// a snapshot (e.g. from an older version) contains both for the same name.
// The snapshot is crafted as raw JSON to exercise the path directly.
func TestInMemoryBackend_RestoreActivePrecedesOverPending(t *testing.T) {
	t.Parallel()

	// Craft a snapshot JSON that has:
	//   us-east-1 / "shared": DeletePending = true   (pending-delete)
	//   us-west-2 / "shared": DeletePending = false  (active)
	snap := []byte(`{
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
	}`)

	b := s3.NewInMemoryBackend(nil)
	require.NoError(t, b.Restore(snap))

	// getBucket must resolve to the active (us-west-2) entry.
	_, err := b.HeadBucket(t.Context(), &sdk_s3.HeadBucketInput{Bucket: aws.String("shared")})
	require.NoError(t, err, "active bucket must be reachable after restore")

	// ListBuckets must show exactly one entry (the active bucket).
	out, err := b.ListBuckets(t.Context(), &sdk_s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Len(t, out.Buckets, 1)
	assert.Equal(t, "shared", aws.ToString(out.Buckets[0].Name))
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

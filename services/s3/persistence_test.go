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

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := s3.NewInMemoryBackend(nil)
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

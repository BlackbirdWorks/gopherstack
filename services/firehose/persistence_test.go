package firehose_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/firehose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *firehose.InMemoryBackend) string
		verify func(t *testing.T, b *firehose.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *firehose.InMemoryBackend) string {
				stream, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "test-stream"})
				if err != nil {
					return ""
				}

				return stream.Name
			},
			verify: func(t *testing.T, b *firehose.InMemoryBackend, id string) {
				t.Helper()

				stream, err := b.DescribeDeliveryStream(id)
				require.NoError(t, err)
				assert.Equal(t, id, stream.Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *firehose.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *firehose.InMemoryBackend, _ string) {
				t.Helper()

				streams := b.ListDeliveryStreams()
				assert.Empty(t, streams)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

// TestRestore_ClosesExistingTagsBeforeReplace verifies that Restore closes Tags on
// streams that are being replaced, preventing Prometheus registry leaks.
func TestRestore_ClosesExistingTagsBeforeReplace(t *testing.T) {
	t.Parallel()

	// Populate a backend with two tagged streams.
	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "old-stream"})
	require.NoError(t, err)
	require.NoError(t, b.TagDeliveryStream("old-stream", map[string]string{"gen": "old"}))

	// Build a snapshot that contains a different set of streams.
	newBackend := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err = newBackend.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{Name: "new-stream"})
	require.NoError(t, err)
	snap := newBackend.Snapshot()
	require.NotNil(t, snap)

	// Restore onto the populated backend — must not panic and must close old Tags.
	require.NoError(t, b.Restore(snap))

	// Only the new stream should be visible now.
	names := b.ListDeliveryStreams()
	assert.Equal(t, []string{"new-stream"}, names)
}

// TestRestore_RecalculatesBufferSizeBytes verifies that bufferSizeBytes is correctly
// recomputed after a snapshot/restore cycle, so that size-based flush thresholds
// fire correctly on the restored backend.
func TestRestore_RecalculatesBufferSizeBytes(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}

	// Create a stream and buffer records without triggering a flush.
	original := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	original.SetS3Backend(s3mock)
	_, err := original.CreateDeliveryStream(firehose.CreateDeliveryStreamInput{
		Name: "restore-size-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::restore-bucket",
			BufferingHints: &firehose.BufferingHints{
				SizeInMBs:         1,
				IntervalInSeconds: 300,
			},
		},
	})
	require.NoError(t, err)

	// Put 900 KB — below threshold so no flush yet.
	require.NoError(t, original.PutRecord("restore-size-stream", make([]byte, 900*1024)))
	assert.Empty(t, s3mock.calls)

	// Snapshot and restore onto a fresh backend.
	snap := original.Snapshot()
	require.NotNil(t, snap)

	restored := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	restored.SetS3Backend(s3mock)
	require.NoError(t, restored.Restore(snap))

	// Adding another 200 KB should now push over the 1 MB threshold and flush.
	require.NoError(t, restored.PutRecord("restore-size-stream", make([]byte, 200*1024)))
	assert.Len(t, s3mock.calls, 1, "size-based flush should fire after restore")
}

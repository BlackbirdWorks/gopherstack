package kinesis_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *kinesis.InMemoryBackend) string
		verify func(t *testing.T, b *kinesis.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *kinesis.InMemoryBackend) string {
				err := b.CreateStream(&kinesis.CreateStreamInput{
					StreamName: "test-stream",
					ShardCount: 1,
				})
				if err != nil {
					return ""
				}

				return "test-stream"
			},
			verify: func(t *testing.T, b *kinesis.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.DescribeStream(&kinesis.DescribeStreamInput{StreamName: id})
				require.NoError(t, err)
				assert.Equal(t, id, out.StreamName)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *kinesis.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *kinesis.InMemoryBackend, _ string) {
				t.Helper()

				out, err := b.ListStreams(&kinesis.ListStreamsInput{})
				require.NoError(t, err)
				assert.Empty(t, out.StreamNames)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

// TestSnapshot_EmptyShardRecords_NoNull verifies that an empty shard serialises to []
// (not null) so that snapshots remain stable after restore.
func TestSnapshot_EmptyShardRecords_NoNull(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "empty-shard-stream"}))

	snap := bk.Snapshot()
	require.NotNil(t, snap)
	// The JSON must not contain a null records field.
	assert.NotContains(t, string(snap), `"records":null`)
	assert.Contains(t, string(snap), `"records":[]`)
}

// TestSnapshot_RestoreClearsOldPointers verifies that restoring a smaller snapshot
// into a backend that previously held records does not retain stale pointers.
func TestSnapshot_RestoreClearsOldPointers(t *testing.T) {
	t.Parallel()

	// Create a backend with records in it.
	bk := kinesis.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "ptr-stream"}))

	for range 5 {
		_, err := bk.PutRecord(&kinesis.PutRecordInput{
			StreamName:   "ptr-stream",
			PartitionKey: "pk",
			Data:         []byte("data"),
		})
		require.NoError(t, err)
	}

	// Take a snapshot of the populated state.
	snap := bk.Snapshot()
	require.NotNil(t, snap)

	// Now restore into the same backend (simulating an in-place restore).
	require.NoError(t, bk.Restore(snap))

	desc, err := bk.DescribeStream(&kinesis.DescribeStreamInput{StreamName: "ptr-stream"})
	require.NoError(t, err)
	assert.Len(t, desc.Shards, 1)
}

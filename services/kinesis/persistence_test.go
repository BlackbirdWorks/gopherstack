package kinesis_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

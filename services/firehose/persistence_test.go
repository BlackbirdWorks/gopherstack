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

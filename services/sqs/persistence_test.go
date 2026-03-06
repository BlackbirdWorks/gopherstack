package sqs_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *sqs.InMemoryBackend) string
		verify func(t *testing.T, b *sqs.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *sqs.InMemoryBackend) string {
				out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "test-queue"})
				if err != nil {
					return ""
				}

				return out.QueueURL
			},
			verify: func(t *testing.T, b *sqs.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.ListQueues(&sqs.ListQueuesInput{})
				require.NoError(t, err)
				require.Len(t, out.QueueURLs, 1)
				assert.Equal(t, id, out.QueueURLs[0])
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *sqs.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *sqs.InMemoryBackend, _ string) {
				t.Helper()

				out, err := b.ListQueues(&sqs.ListQueuesInput{})
				require.NoError(t, err)
				assert.Empty(t, out.QueueURLs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

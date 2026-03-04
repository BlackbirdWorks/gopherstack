package sns_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *sns.InMemoryBackend) string
		verify func(t *testing.T, b *sns.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *sns.InMemoryBackend) string {
				topic, err := b.CreateTopic("test-topic", nil)
				if err != nil {
					return ""
				}

				return topic.TopicArn
			},
			verify: func(t *testing.T, b *sns.InMemoryBackend, id string) {
				t.Helper()

				attrs, err := b.GetTopicAttributes(id)
				require.NoError(t, err)
				assert.NotEmpty(t, attrs)
				assert.Equal(t, id, attrs["TopicArn"])
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *sns.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *sns.InMemoryBackend, _ string) {
				t.Helper()

				topics, _, err := b.ListTopics("")
				require.NoError(t, err)
				assert.Empty(t, topics)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

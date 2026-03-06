package eventbridge_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *eventbridge.InMemoryBackend) string
		verify func(t *testing.T, b *eventbridge.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *eventbridge.InMemoryBackend) string {
				bus, err := b.CreateEventBus("test-bus", "")
				if err != nil {
					return ""
				}

				return bus.Name
			},
			verify: func(t *testing.T, b *eventbridge.InMemoryBackend, id string) {
				t.Helper()

				bus, err := b.DescribeEventBus(id)
				require.NoError(t, err)
				assert.Equal(t, id, bus.Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *eventbridge.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *eventbridge.InMemoryBackend, _ string) {
				t.Helper()

				// The default event bus always exists; just verify restore worked
				buses, _, err := b.ListEventBuses("", "")
				require.NoError(t, err)
				assert.NotNil(t, buses)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := eventbridge.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := eventbridge.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

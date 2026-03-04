package cloudwatchlogs_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *cloudwatchlogs.InMemoryBackend) string
		verify func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *cloudwatchlogs.InMemoryBackend) string {
				_, err := b.CreateLogGroup("test-group")
				if err != nil {
					return ""
				}

				return "test-group"
			},
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, id string) {
				t.Helper()

				groups, _, err := b.DescribeLogGroups("", "", 100)
				require.NoError(t, err)
				require.Len(t, groups, 1)
				assert.Equal(t, id, groups[0].LogGroupName)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *cloudwatchlogs.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *cloudwatchlogs.InMemoryBackend, _ string) {
				t.Helper()

				groups, _, err := b.DescribeLogGroups("", "", 100)
				require.NoError(t, err)
				assert.Empty(t, groups)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := cloudwatchlogs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

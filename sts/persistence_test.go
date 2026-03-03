package sts_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *sts.InMemoryBackend) string
		verify func(t *testing.T, b *sts.InMemoryBackend, id string)
		name   string
	}{
		{
			name:  "round_trip_no_state",
			setup: func(_ *sts.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *sts.InMemoryBackend, _ string) {
				t.Helper()
				// STS has no mutable state; just verify the backend is functional after restore
				assert.NotNil(t, b)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := sts.NewInMemoryBackendWithConfig("000000000000")
			_ = tt.setup(original)

			snap := original.Snapshot()
			// STS returns nil snapshot since it has no state
			assert.Nil(t, snap)

			fresh := sts.NewInMemoryBackendWithConfig("000000000000")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, "")
		})
	}
}

package s3control_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/s3control"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *s3control.InMemoryBackend) string
		verify func(t *testing.T, b *s3control.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *s3control.InMemoryBackend) string {
				b.PutPublicAccessBlock(s3control.PublicAccessBlock{
					AccountID:       "000000000000",
					BlockPublicAcls: true,
				})

				return "000000000000"
			},
			verify: func(t *testing.T, b *s3control.InMemoryBackend, id string) {
				t.Helper()

				cfg, err := b.GetPublicAccessBlock(id)
				require.NoError(t, err)
				assert.True(t, cfg.BlockPublicAcls)
				assert.Equal(t, id, cfg.AccountID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *s3control.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *s3control.InMemoryBackend, _ string) {
				t.Helper()

				_, err := b.GetPublicAccessBlock("nonexistent")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := s3control.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := s3control.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

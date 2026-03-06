package route53_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/route53"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *route53.InMemoryBackend) string
		verify func(t *testing.T, b *route53.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *route53.InMemoryBackend) string {
				zone, err := b.CreateHostedZone("example.com.", "ref-001", "test zone", false)
				if err != nil {
					return ""
				}

				return zone.ID
			},
			verify: func(t *testing.T, b *route53.InMemoryBackend, id string) {
				t.Helper()

				zone, err := b.GetHostedZone(id)
				require.NoError(t, err)
				assert.Equal(t, id, zone.ID)
				assert.Equal(t, "example.com.", zone.Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *route53.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *route53.InMemoryBackend, _ string) {
				t.Helper()

				zones, err := b.ListHostedZones()
				require.NoError(t, err)
				assert.Empty(t, zones)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := route53.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := route53.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

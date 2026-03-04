package support_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/support"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *support.InMemoryBackend) string
		verify func(t *testing.T, b *support.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *support.InMemoryBackend) string {
				c, err := b.CreateCase("test subject", "amazon-dynamodb", "general-guidance", "low", "test body")
				if err != nil {
					return ""
				}

				return c.CaseID
			},
			verify: func(t *testing.T, b *support.InMemoryBackend, id string) {
				t.Helper()

				cases := b.DescribeCases([]string{id})
				require.Len(t, cases, 1)
				assert.Equal(t, id, cases[0].CaseID)
				assert.Equal(t, "test subject", cases[0].Subject)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *support.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *support.InMemoryBackend, _ string) {
				t.Helper()

				cases := b.DescribeCases(nil)
				assert.Empty(t, cases)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := support.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := support.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := support.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

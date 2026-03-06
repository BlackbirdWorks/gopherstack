package ses_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/ses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *ses.InMemoryBackend) string
		verify func(t *testing.T, b *ses.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *ses.InMemoryBackend) string {
				err := b.VerifyEmailIdentity("test@example.com")
				if err != nil {
					return ""
				}

				return "test@example.com"
			},
			verify: func(t *testing.T, b *ses.InMemoryBackend, id string) {
				t.Helper()

				identities := b.ListIdentities("", 0).Data
				require.Len(t, identities, 1)
				assert.Equal(t, id, identities[0])
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *ses.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *ses.InMemoryBackend, _ string) {
				t.Helper()

				identities := b.ListIdentities("", 0).Data
				assert.Empty(t, identities)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := ses.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := ses.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

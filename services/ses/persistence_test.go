package ses_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ses"
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

func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := ses.NewHandler(ses.NewInMemoryBackend())
	require.NoError(t, h.Backend.VerifyEmailIdentity("delegate@test.com"))

	snap := h.Snapshot()
	require.NotNil(t, snap)

	h2 := ses.NewHandler(ses.NewInMemoryBackend())
	require.NoError(t, h2.Restore(snap))

	identities := h2.Backend.ListIdentities("", 0).Data
	require.Len(t, identities, 1)
	assert.Equal(t, "delegate@test.com", identities[0])
}

func TestInMemoryBackend_RestorePreservesEmails(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("persist@test.com"))

	_, err := b.SendEmail("persist@test.com", []string{"to@test.com"}, "Test", "", "body")
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	fresh := ses.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(snap))

	emails := fresh.ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "persist@test.com", emails[0].From)
	assert.Equal(t, "Test", emails[0].Subject)
}

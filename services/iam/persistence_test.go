package iam_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *iam.InMemoryBackend) string
		verify func(t *testing.T, b *iam.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *iam.InMemoryBackend) string {
				user, err := b.CreateUser("testuser", "/")
				if err != nil {
					return ""
				}

				return user.UserName
			},
			verify: func(t *testing.T, b *iam.InMemoryBackend, id string) {
				t.Helper()

				user, err := b.GetUser(id)
				require.NoError(t, err)
				assert.Equal(t, id, user.UserName)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *iam.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *iam.InMemoryBackend, _ string) {
				t.Helper()

				users, err := b.ListUsers()
				require.NoError(t, err)
				assert.Empty(t, users)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := iam.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := iam.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

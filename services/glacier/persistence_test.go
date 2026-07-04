package glacier_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

func TestGlacier_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *glacier.InMemoryBackend)
		verify func(t *testing.T, b *glacier.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty_backend",
			setup: func(_ *glacier.InMemoryBackend) {},
			verify: func(t *testing.T, b *glacier.InMemoryBackend) {
				t.Helper()

				vaults := b.ListVaults("123", "us-east-1")
				assert.Empty(t, vaults)
			},
		},
		{
			name: "vault_with_archive_preserved",
			setup: func(b *glacier.InMemoryBackend) {
				_, err := b.CreateVault("123", "us-east-1", "my-vault")
				if err != nil {
					return
				}

				_, _ = b.UploadArchive("123", "us-east-1", "my-vault", "desc", "hash", 1024, []byte("data"))
			},
			verify: func(t *testing.T, b *glacier.InMemoryBackend) {
				t.Helper()

				vaults := b.ListVaults("123", "us-east-1")
				require.Len(t, vaults, 1)
				assert.Equal(t, "my-vault", vaults[0].VaultName)
				assert.Equal(t, int64(1), vaults[0].NumberOfArchives)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := glacier.NewInMemoryBackend()
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}

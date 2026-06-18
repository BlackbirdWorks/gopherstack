package glacier_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// TestDeleteVault_IndexBounded verifies that creating and then deleting vaults keeps
// the vaultsByAccountRegion index in sync with the primary vault store, so neither
// grows without bound across repeated create/delete cycles.
func TestDeleteVault_IndexBounded(t *testing.T) {
	t.Parallel()

	const (
		accountID = "123456789012"
		region    = "us-east-1"
	)

	tests := []struct {
		name   string
		vaults []string
	}{
		{name: "single vault", vaults: []string{"vault-a"}},
		{name: "multiple vaults", vaults: []string{"vault-a", "vault-b", "vault-c", "vault-d", "vault-e"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()

			for _, name := range tc.vaults {
				_, err := b.CreateVault(accountID, region, name)
				require.NoError(t, err)
			}

			require.Equal(t, len(tc.vaults), glacier.VaultCount(b), "vault count before delete")
			require.Equal(t, len(tc.vaults), glacier.VaultIndexCount(b, accountID, region), "index count before delete")

			for _, name := range tc.vaults {
				err := b.DeleteVault(accountID, region, name)
				require.NoError(t, err)
			}

			require.Equal(t, 0, glacier.VaultCount(b), "vault store must be empty after delete")
			require.Equal(t, 0, glacier.VaultIndexCount(b, accountID, region),
				"vault index must be empty after delete — no leak")
		})
	}
}

// TestListVaults_UsesIndex verifies that ListVaults returns only vaults belonging to
// the specified account+region, not vaults from other accounts or regions.
func TestListVaults_UsesIndex(t *testing.T) {
	t.Parallel()

	b := glacier.NewInMemoryBackend()

	_, err := b.CreateVault("account-A", "us-east-1", "vault-a1")
	require.NoError(t, err)
	_, err = b.CreateVault("account-A", "us-east-1", "vault-a2")
	require.NoError(t, err)
	_, err = b.CreateVault("account-A", "eu-west-1", "vault-other-region")
	require.NoError(t, err)
	_, err = b.CreateVault("account-B", "us-east-1", "vault-other-account")
	require.NoError(t, err)

	got := b.ListVaults("account-A", "us-east-1")

	require.Len(t, got, 2, "only vaults in account-A/us-east-1")
	require.Equal(t, "vault-a1", got[0].VaultName)
	require.Equal(t, "vault-a2", got[1].VaultName)

	require.Equal(t, 2, glacier.VaultIndexCount(b, "account-A", "us-east-1"), "index count matches list count")
}

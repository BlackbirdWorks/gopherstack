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

// TestDeleteVault_CascadeCleansMultipartParts verifies that deleting a vault with
// in-progress multipart uploads cleans up their (raw map, not a *store.Table)
// multipartParts rows too, not just the multipartUploads table entry -- regression
// test for a leak where DeleteVault cascade-deleted the MultipartUpload row but left
// its uploaded-part rows orphaned in multipartParts forever.
func TestDeleteVault_CascadeCleansMultipartParts(t *testing.T) {
	t.Parallel()

	const (
		accountID = "123456789012"
		region    = "us-east-1"
		vaultName = "mpu-vault"
	)

	b := glacier.NewInMemoryBackend()

	_, err := b.CreateVault(accountID, region, vaultName)
	require.NoError(t, err)

	up, err := b.InitiateMultipartUpload(accountID, region, vaultName, "desc", 1<<20)
	require.NoError(t, err)

	err = b.UploadMultipartPart(accountID, region, vaultName, up.MultipartUploadID, "0-1048575", "deadbeef")
	require.NoError(t, err)

	require.Equal(t, 1, glacier.MultipartPartsRowCount(b), "part row present before delete")

	err = b.DeleteVault(accountID, region, vaultName)
	require.NoError(t, err)

	require.Equal(t, 0, glacier.MultipartUploadCount(b), "multipart upload table must be empty after delete")
	require.Equal(t, 0, glacier.MultipartPartsRowCount(b),
		"multipartParts must be empty after vault delete — no leak")
}

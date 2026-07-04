package glacier_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// TestGlacierRegionIsolation proves that a same-named vault created in two
// different regions stays fully isolated: each region sees only its own vault,
// the ARNs carry the correct region, and deleting in one region leaves the
// other region's vault intact.
//
// Glacier isolates by a composite map key (accountID + region + vaultName), so
// the region is part of every resource's identity. This test locks that
// behaviour in.
func TestGlacierRegionIsolation(t *testing.T) {
	t.Parallel()

	const (
		account = "000000000000"
		east    = "us-east-1"
		west    = "us-west-2"
		vault   = "shared-name"
	)

	b := glacier.NewInMemoryBackend()

	// 1. Create a vault named "shared-name" in us-east-1.
	eastVault, err := b.CreateVault(account, east, vault)
	require.NoError(t, err)
	assert.Contains(t, eastVault.VaultARN, ":"+east+":")

	// 2. Create a vault with the SAME NAME in us-west-2 — must NOT collide.
	westVault, err := b.CreateVault(account, west, vault)
	require.NoError(t, err)
	assert.Contains(t, westVault.VaultARN, ":"+west+":")

	// 3. Each region lists exactly its own vault.
	eastList := b.ListVaults(account, east)
	require.Len(t, eastList, 1)
	assert.Equal(t, vault, eastList[0].VaultName)
	assert.Contains(t, eastList[0].VaultARN, ":"+east+":")

	westList := b.ListVaults(account, west)
	require.Len(t, westList, 1)
	assert.Equal(t, vault, westList[0].VaultName)
	assert.Contains(t, westList[0].VaultARN, ":"+west+":")

	// 4. Describe is region-scoped.
	gotEast, err := b.DescribeVault(account, east, vault)
	require.NoError(t, err)
	assert.Contains(t, gotEast.VaultARN, ":"+east+":")

	// 5. Deleting in us-east-1 leaves us-west-2 intact.
	require.NoError(t, b.DeleteVault(account, east, vault))

	_, err = b.DescribeVault(account, east, vault)
	require.Error(t, err)

	stillWest, err := b.DescribeVault(account, west, vault)
	require.NoError(t, err)
	assert.Contains(t, stillWest.VaultARN, ":"+west+":")

	assert.Empty(t, b.ListVaults(account, east))
	assert.Len(t, b.ListVaults(account, west), 1)
}

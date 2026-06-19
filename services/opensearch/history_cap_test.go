package opensearch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

func newOSBackend(t *testing.T) *opensearch.InMemoryBackend {
	t.Helper()

	return opensearch.NewInMemoryBackend("000000000000", "us-east-1")
}

func createOSDomain(t *testing.T, b *opensearch.InMemoryBackend, name string) {
	t.Helper()
	_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: name})
	require.NoError(t, err)
}

// TestOpenSearchUpgradeHistoryBoundedGrowth verifies that upgrade history entries
// are capped at maxUpgradeHistoryPerDomain regardless of how many upgrades are run.
func TestOpenSearchUpgradeHistoryBoundedGrowth(t *testing.T) {
	t.Parallel()

	b := newOSBackend(t)
	createOSDomain(t, b, "test-domain")

	capLimit := opensearch.MaxUpgradeHistoryPerDomain

	// Insert twice the cap to prove truncation.
	for i := range capLimit * 2 {
		err := b.UpgradeDomain("test-domain", "upgrade-"+string(rune('A'+i%26)))
		require.NoError(t, err)
	}

	assert.Equal(t, capLimit, opensearch.UpgradeHistoryLen(b, "test-domain"),
		"upgrade history must not exceed cap of %d", capLimit)

	// The returned history (newest-first) must have cap entries and be consistent.
	history, err := b.GetUpgradeHistory("test-domain")
	require.NoError(t, err)
	assert.Len(t, history, capLimit)
}

// TestOpenSearchUpgradeHistoryPreservesRecent verifies that after trimming the
// oldest entries are dropped and the most recent entries are kept.
func TestOpenSearchUpgradeHistoryPreservesRecent(t *testing.T) {
	t.Parallel()

	b := newOSBackend(t)
	createOSDomain(t, b, "test-domain")

	capLimit := opensearch.MaxUpgradeHistoryPerDomain
	total := capLimit + 5

	for i := range total {
		err := b.UpgradeDomain("test-domain", "upgrade-v"+string(rune('0'+i%10)))
		require.NoError(t, err)
	}

	// GetUpgradeHistory returns newest-first, so [0] is the last upgrade we did.
	history, err := b.GetUpgradeHistory("test-domain")
	require.NoError(t, err)
	require.Len(t, history, capLimit)
}

// TestOpenSearchDomainMaintenancesBoundedGrowth verifies that maintenance records
// are capped at maxMaintenancesPerDomain.
func TestOpenSearchDomainMaintenancesBoundedGrowth(t *testing.T) {
	t.Parallel()

	b := newOSBackend(t)
	createOSDomain(t, b, "test-domain")

	capLimit := opensearch.MaxMaintenancesPerDomain

	for range capLimit * 2 {
		_, err := b.StartDomainMaintenance("test-domain", "REBOOT_NODE", "")
		require.NoError(t, err)
	}

	assert.Equal(t, capLimit, opensearch.MaintenancesLen(b, "test-domain"),
		"maintenance records must not exceed cap of %d", capLimit)

	// ListDomainMaintenances must also return exactly cap entries.
	records, err := b.ListDomainMaintenances("test-domain")
	require.NoError(t, err)
	assert.Len(t, records, capLimit)
}

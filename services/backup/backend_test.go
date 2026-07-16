package backup_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestBackendReset verifies Reset clears all state.
func TestBackendReset(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := backup.NewInMemoryBackend("123456789012", "us-east-1").CreateFramework("fw", "", nil)
	require.NoError(t, err)

	// Populate the backend.
	_, err = backend.CreateBackupVault("my-vault", "", "", nil)
	require.NoError(t, err)

	_, err = backend.CreateFramework("fw", "", nil)
	require.NoError(t, err)

	_, err = backend.CreateReportPlan("rp", "", nil, nil)
	require.NoError(t, err)

	handler := backup.NewHandler(backend)
	handler.Reset()

	// After reset, vault should not exist.
	vaults := backend.ListBackupVaults()
	assert.Empty(t, vaults)

	// Framework can be recreated.
	_, err = backend.CreateFramework("fw", "", nil)
	assert.NoError(t, err)
}

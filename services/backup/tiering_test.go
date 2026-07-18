package backup_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestTieringConfig(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	b.CreateBackupVault("tier-vault", "", "", nil)

	t.Run("create and get", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		b2.CreateBackupVault("tc-vault", "", "", nil)
		require.NoError(t, b2.CreateTieringConfiguration("tc-vault"))
		tc, err := b2.GetTieringConfiguration("tc-vault")
		require.NoError(t, err)
		assert.Equal(t, "tc-vault", tc.BackupVaultName)
	})

	t.Run("list configurations", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		b2.CreateBackupVault("v1", "", "", nil)
		b2.CreateBackupVault("v2", "", "", nil)
		_ = b2.CreateTieringConfiguration("v1")
		_ = b2.CreateTieringConfiguration("v2")
		tcs := b2.ListTieringConfigurations()
		require.Len(t, tcs, 2)
	})

	t.Run("delete removes config", func(t *testing.T) {
		t.Parallel()
		b2 := backup.NewInMemoryBackend("000000000000", "us-east-1")
		b2.CreateBackupVault("del-vault", "", "", nil)
		_ = b2.CreateTieringConfiguration("del-vault")
		require.NoError(t, b2.DeleteTieringConfiguration("del-vault"))
		tcs := b2.ListTieringConfigurations()
		assert.Empty(t, tcs)
	})
	_ = b
}

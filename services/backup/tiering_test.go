package backup_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func tieringResourceSelection() []backup.ResourceSelection {
	return []backup.ResourceSelection{{
		ResourceType:              "S3",
		Resources:                 []string{"*"},
		TieringDownSettingsInDays: 90,
	}}
}

func TestTieringConfigCreate(t *testing.T) {
	t.Parallel()

	t.Run("keyed by TieringConfigurationName not vault name", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		tc, err := b.CreateTieringConfiguration("tc_vault", "vault-x", tieringResourceSelection(), "")
		require.NoError(t, err)
		assert.Equal(t, "tc_vault", tc.TieringConfigurationName)
		assert.Equal(t, "vault-x", tc.BackupVaultName)
		assert.NotEmpty(t, tc.TieringConfigurationArn)
		assert.False(t, tc.CreationTime.IsZero())
	})

	t.Run("wildcard vault name is accepted", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		tc, err := b.CreateTieringConfiguration("tc_all", "*", tieringResourceSelection(), "")
		require.NoError(t, err)
		assert.Equal(t, "*", tc.BackupVaultName)
	})

	t.Run("duplicate name without matching CreatorRequestId is AlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateTieringConfiguration("dup", "*", tieringResourceSelection(), "")
		require.NoError(t, err)
		_, err = b.CreateTieringConfiguration("dup", "*", tieringResourceSelection(), "")
		require.ErrorIs(t, err, backup.ErrAlreadyExists)
	})

	t.Run("duplicate name with matching CreatorRequestId is idempotent", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		first, err := b.CreateTieringConfiguration("idem", "*", tieringResourceSelection(), "req-1")
		require.NoError(t, err)
		second, err := b.CreateTieringConfiguration("idem", "*", tieringResourceSelection(), "req-1")
		require.NoError(t, err)
		assert.Equal(t, first.TieringConfigurationArn, second.TieringConfigurationArn)
	})

	t.Run("missing name is validation error", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateTieringConfiguration("", "*", tieringResourceSelection(), "")
		require.ErrorIs(t, err, backup.ErrValidation)
	})

	t.Run("name with invalid characters is rejected", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateTieringConfiguration("bad name!", "*", tieringResourceSelection(), "")
		require.ErrorIs(t, err, backup.ErrValidation)
	})

	t.Run("missing BackupVaultName is validation error", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateTieringConfiguration("novault", "", tieringResourceSelection(), "")
		require.ErrorIs(t, err, backup.ErrValidation)
	})

	t.Run("empty ResourceSelection is validation error", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateTieringConfiguration("noselect", "*", nil, "")
		require.ErrorIs(t, err, backup.ErrValidation)
	})

	t.Run("TieringDownSettingsInDays below minimum is rejected", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		sel := []backup.ResourceSelection{{ResourceType: "S3", Resources: []string{"*"}, TieringDownSettingsInDays: 1}}
		_, err := b.CreateTieringConfiguration("toolow", "*", sel, "")
		require.ErrorIs(t, err, backup.ErrValidation)
	})

	t.Run("TieringDownSettingsInDays above maximum is rejected", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		sel := []backup.ResourceSelection{
			{ResourceType: "S3", Resources: []string{"*"}, TieringDownSettingsInDays: 99999},
		}
		_, err := b.CreateTieringConfiguration("toohigh", "*", sel, "")
		require.ErrorIs(t, err, backup.ErrValidation)
	})
}

func TestTieringConfigGet(t *testing.T) {
	t.Parallel()

	t.Run("returns created configuration", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateTieringConfiguration("g1", "vault-g", tieringResourceSelection(), "")
		require.NoError(t, err)
		tc, err := b.GetTieringConfiguration("g1")
		require.NoError(t, err)
		assert.Equal(t, "vault-g", tc.BackupVaultName)
	})

	t.Run("unknown name returns not found", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.GetTieringConfiguration("nope")
		require.ErrorIs(t, err, backup.ErrNotFound)
	})
}

func TestTieringConfigList(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateTieringConfiguration("v2", "*", tieringResourceSelection(), "")
	require.NoError(t, err)
	_, err = b.CreateTieringConfiguration("v1", "*", tieringResourceSelection(), "")
	require.NoError(t, err)

	tcs := b.ListTieringConfigurations()
	require.Len(t, tcs, 2)
	// Sorted by TieringConfigurationName.
	assert.Equal(t, "v1", tcs[0].TieringConfigurationName)
	assert.Equal(t, "v2", tcs[1].TieringConfigurationName)
}

func TestTieringConfigUpdate(t *testing.T) {
	t.Parallel()

	t.Run("replaces vault and selection, sets LastUpdatedTime", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateTieringConfiguration("upd", "vault-a", tieringResourceSelection(), "")
		require.NoError(t, err)

		newSel := []backup.ResourceSelection{{
			ResourceType:              "S3",
			Resources:                 []string{"arn:aws:s3:::specific"},
			TieringDownSettingsInDays: 200,
		}}
		tc, err := b.UpdateTieringConfiguration("upd", "vault-b", newSel)
		require.NoError(t, err)
		assert.Equal(t, "vault-b", tc.BackupVaultName)
		require.Len(t, tc.ResourceSelection, 1)
		assert.Equal(t, int64(200), tc.ResourceSelection[0].TieringDownSettingsInDays)
		require.NotNil(t, tc.LastUpdatedTime)

		// TieringConfigurationName is immutable and stays the lookup key.
		got, err := b.GetTieringConfiguration("upd")
		require.NoError(t, err)
		assert.Equal(t, "upd", got.TieringConfigurationName)
	})

	t.Run("unknown name returns not found", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.UpdateTieringConfiguration("nope", "*", tieringResourceSelection())
		require.ErrorIs(t, err, backup.ErrNotFound)
	})
}

func TestTieringConfigDelete(t *testing.T) {
	t.Parallel()

	t.Run("removes configuration", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		_, err := b.CreateTieringConfiguration("del", "*", tieringResourceSelection(), "")
		require.NoError(t, err)
		require.NoError(t, b.DeleteTieringConfiguration("del"))
		_, err = b.GetTieringConfiguration("del")
		require.ErrorIs(t, err, backup.ErrNotFound)
	})

	t.Run("unknown name returns not found", func(t *testing.T) {
		t.Parallel()
		b := backup.NewInMemoryBackend("000000000000", "us-east-1")
		err := b.DeleteTieringConfiguration("nope")
		require.ErrorIs(t, err, backup.ErrNotFound)
	})
}

package secretsmanager_test

// restoresecret_test.go consolidates every RestoreSecret-specific test that was
// previously scattered across several older test files. Ported verbatim
// (assertions unchanged).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// RestoreSecret comprehensive
// ---------------------------------------------------------------------------

func TestRestoreSecret_ClearsDeletedDate(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "restore-me", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "restore-me"})
	require.NoError(t, err)

	_, err = b.RestoreSecret(context.Background(), &secretsmanager.RestoreSecretInput{SecretID: "restore-me"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "restore-me"})
	require.NoError(t, err)
	assert.Nil(t, desc.DeletedDate, "DeletedDate must be cleared after RestoreSecret")
}

func TestRestoreSecret_ActiveSecretFails(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "active-restore", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.RestoreSecret(context.Background(), &secretsmanager.RestoreSecretInput{SecretID: "active-restore"})
	require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter,
		"restoring a non-deleted secret must return InvalidRequestException")
}

func TestRestoreSecret_WritableAfterRestore(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "write-after-restore", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "write-after-restore"})
	require.NoError(t, err)

	_, err = b.RestoreSecret(context.Background(), &secretsmanager.RestoreSecretInput{SecretID: "write-after-restore"})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:     "write-after-restore",
		SecretString: "v2",
	})
	require.NoError(t, err, "PutSecretValue must succeed after RestoreSecret")
}

func TestRestoreSecret_NotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.RestoreSecret(context.Background(), &secretsmanager.RestoreSecretInput{SecretID: "missing"})
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// RestoreSecret state machine
// ---------------------------------------------------------------------------

func TestRestoreSecret_RestoredSecretIsReadable(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "restore-test",
		SecretString: "important-value",
	})
	require.NoError(t, err)

	_, err = b.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretID:             "restore-test",
		RecoveryWindowInDays: ptr64(30),
	})
	require.NoError(t, err)

	// Secret in deleted state — GetSecretValue must fail.
	_, err = b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "restore-test"})
	require.Error(t, err)

	// Restore the secret.
	_, err = b.RestoreSecret(ctx, &secretsmanager.RestoreSecretInput{SecretID: "restore-test"})
	require.NoError(t, err)

	// Secret must now be readable again.
	out, err := b.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretID: "restore-test"})
	require.NoError(t, err)
	assert.Equal(t, "important-value", out.SecretString)
}

func TestRestoreSecret_ClearsDeletedDateOnCustomWindow(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "restore-clear-test",
		SecretString: "v",
	})
	require.NoError(t, err)

	_, err = b.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretID:             "restore-clear-test",
		RecoveryWindowInDays: ptr64(7),
	})
	require.NoError(t, err)

	_, err = b.RestoreSecret(ctx, &secretsmanager.RestoreSecretInput{SecretID: "restore-clear-test"})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretID: "restore-clear-test"})
	require.NoError(t, err)
	assert.Nil(t, desc.DeletedDate, "DeletedDate must be nil after RestoreSecret")
}

// ---------------------------------------------------------------------------
// RestoreSecret on an active (non-deleted) secret via HTTP
// ---------------------------------------------------------------------------

// TestRestoreSecret_RestoreActiveSecretFails verifies that RestoreSecret on a
// non-deleted (active) secret returns an error via the HTTP handler.
func TestRestoreSecret_RestoreActiveSecretFails(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"active-restore-http","SecretString":"v"}`)
	require.Equal(t, 200, rec.Code)

	// Attempt to restore an active (non-deleted) secret – should fail.
	rec = doR1Request(t, h, "secretsmanager.RestoreSecret",
		`{"SecretId":"active-restore-http"}`)
	assert.Equal(t, 400, rec.Code)
}

// ---------------------------------------------------------------------------
// Backend scenarios (ported from table-style subtests)
// ---------------------------------------------------------------------------

func TestRestoreSecret_BackendScenarios(t *testing.T) {
	t.Parallel()

	t.Run("RestoreNotFound", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		_, err := backend.RestoreSecret(context.Background(), &secretsmanager.RestoreSecretInput{SecretID: "missing"})
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})

	t.Run("DeleteAndRestore", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name:         "restorable2",
			SecretString: "data",
		})

		_, err := backend.DeleteSecret(
			context.Background(),
			&secretsmanager.DeleteSecretInput{SecretID: "restorable2"},
		)
		require.NoError(t, err)

		restOut, err := backend.RestoreSecret(
			context.Background(),
			&secretsmanager.RestoreSecretInput{SecretID: "restorable2"},
		)
		require.NoError(t, err)
		assert.Equal(t, "restorable2", restOut.Name)

		_, err = backend.GetSecretValue(
			context.Background(),
			&secretsmanager.GetSecretValueInput{SecretID: "restorable2"},
		)
		require.NoError(t, err)
	})
}

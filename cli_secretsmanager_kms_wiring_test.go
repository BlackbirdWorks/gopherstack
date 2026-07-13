package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// TestWireSecretsManagerKMS_EncryptsAndDecryptsViaRealKMS exercises the exact
// composition-root wiring cli.go's setup uses (wireSecretsManagerKMS) against
// REAL Secrets Manager and KMS backends -- not test doubles -- to prove a
// secret's value is actually round-tripped through KMS Encrypt/Decrypt
// instead of being stored opaquely, mirroring the SSM->KMS precedent
// (wireSSMKMS).
func TestWireSecretsManagerKMS_EncryptsAndDecryptsViaRealKMS(t *testing.T) {
	t.Parallel()

	smBk := secretsmanagerbackend.NewInMemoryBackend()
	smH := secretsmanagerbackend.NewHandler(smBk)

	kmsBk := kmsbackend.NewInMemoryBackend()
	kmsH := kmsbackend.NewHandler(kmsBk)

	// This is the exact call cli.go's setupServices makes.
	wireSecretsManagerKMS(smH, kmsH)

	ctx := context.Background()

	const plaintext = "hunter2-database-password"

	_, err := smBk.CreateSecret(ctx, &secretsmanagerbackend.CreateSecretInput{
		Name:         "wiring-test-secret",
		SecretString: plaintext,
	})
	require.NoError(t, err)

	// --- The persisted/stored form must be ciphertext, never the plaintext. ---
	snapshot := smBk.Snapshot(ctx)
	require.NotEmpty(t, snapshot)
	assert.NotContains(t, string(snapshot), plaintext,
		"a snapshot of the backend must never contain the plaintext secret value once KMS is wired")
	assert.Contains(t, string(snapshot), `"Ciphertext"`,
		"the stored secret version must carry a KMS ciphertext blob")

	// --- GetSecretValue must transparently decrypt it back. ---
	out, err := smBk.GetSecretValue(ctx, &secretsmanagerbackend.GetSecretValueInput{SecretID: "wiring-test-secret"})
	require.NoError(t, err)
	assert.Equal(t, plaintext, out.SecretString,
		"GetSecretValue must decrypt the value sealed via the wired real KMS backend")

	// --- A real KMS key must actually have been used to Encrypt (and then Decrypt). ---
	keys, err := kmsBk.ListKeys(ctx, &kmsbackend.ListKeysInput{})
	require.NoError(t, err)
	require.Len(t, keys.Keys, 1,
		"CreateSecret with no KmsKeyId must lazily provision the default Secrets Manager KMS key")

	usage, err := kmsBk.GetKeyLastUsage(ctx, &kmsbackend.GetKeyLastUsageInput{KeyID: keys.Keys[0].KeyID})
	require.NoError(t, err)
	require.NotNil(t, usage.KeyLastUsage, "the KMS backend must have recorded a real cryptographic operation")
	assert.Equal(t, "Decrypt", usage.KeyLastUsage.Operation,
		"GetSecretValue must have driven a real KMS Decrypt against the backing key")
}

// TestWireSecretsManagerKMS_ExplicitKeyIDUsed proves a secret created with an
// explicit KmsKeyId encrypts under that key (via a real KMS CreateKey/Encrypt
// call) rather than the default managed-key alias.
func TestWireSecretsManagerKMS_ExplicitKeyIDUsed(t *testing.T) {
	t.Parallel()

	smBk := secretsmanagerbackend.NewInMemoryBackend()
	smH := secretsmanagerbackend.NewHandler(smBk)

	kmsBk := kmsbackend.NewInMemoryBackend()
	kmsH := kmsbackend.NewHandler(kmsBk)

	wireSecretsManagerKMS(smH, kmsH)

	ctx := context.Background()

	createKeyOut, err := kmsBk.CreateKey(ctx, &kmsbackend.CreateKeyInput{Description: "customer key"})
	require.NoError(t, err)
	customKeyID := createKeyOut.KeyMetadata.KeyID

	_, err = smBk.CreateSecret(ctx, &secretsmanagerbackend.CreateSecretInput{
		Name:         "explicit-key-secret",
		SecretString: "value",
		KmsKeyID:     customKeyID,
	})
	require.NoError(t, err)

	usage, err := kmsBk.GetKeyLastUsage(ctx, &kmsbackend.GetKeyLastUsageInput{KeyID: customKeyID})
	require.NoError(t, err)
	require.NotNil(t, usage.KeyLastUsage)
	assert.Equal(t, "Encrypt", usage.KeyLastUsage.Operation)

	out, err := smBk.GetSecretValue(ctx, &secretsmanagerbackend.GetSecretValueInput{SecretID: "explicit-key-secret"})
	require.NoError(t, err)
	assert.Equal(t, "value", out.SecretString)
}

// TestWireSecretsManagerKMS_Unwired_NoOp verifies that when SetKMSEncryptor is
// never called (e.g. either backend missing/misconfigured at wiring time),
// Secrets Manager falls back to its pre-KMS-integration behaviour: plaintext
// storage, exactly as before this feature existed.
func TestWireSecretsManagerKMS_Unwired_NoOp(t *testing.T) {
	t.Parallel()

	smBk := secretsmanagerbackend.NewInMemoryBackend()
	ctx := context.Background()

	_, err := smBk.CreateSecret(ctx, &secretsmanagerbackend.CreateSecretInput{
		Name:         "unwired-secret",
		SecretString: "plain-value",
	})
	require.NoError(t, err)

	snapshot := smBk.Snapshot(ctx)
	assert.Contains(t, string(snapshot), "plain-value",
		"without KMS wiring the snapshot must still carry the plaintext, unchanged from before KMS integration")
	assert.NotContains(t, string(snapshot), `"Ciphertext"`)

	out, err := smBk.GetSecretValue(ctx, &secretsmanagerbackend.GetSecretValueInput{SecretID: "unwired-secret"})
	require.NoError(t, err)
	assert.Equal(t, "plain-value", out.SecretString)
}

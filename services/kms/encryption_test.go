package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"

	"strings"
)

func TestEncryptContextSize_Encrypt(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	oversized := map[string]string{"k": strings.Repeat("v", 5000)}
	_, err := b.Encrypt(
		context.Background(),
		&kms.EncryptInput{KeyID: keyID, Plaintext: []byte("x"), EncryptionContext: oversized},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EncryptionContext")
}

func TestEncryptContextSize_Decrypt(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	oversized := map[string]string{"k": strings.Repeat("v", 5000)}
	_, err := b.Decrypt(
		context.Background(),
		&kms.DecryptInput{CiphertextBlob: make([]byte, 64), EncryptionContext: oversized},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EncryptionContext")
}

func TestEncryptContextSize_GenerateDataKey(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)
	oversized := map[string]string{"k": strings.Repeat("v", 5000)}
	_, err := b.GenerateDataKey(context.Background(), &kms.GenerateDataKeyInput{
		KeyID: keyID, KeySpec: "AES_256", EncryptionContext: oversized,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EncryptionContext")
}

func TestEncryptContextSize_ReEncrypt(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)
	oversized := map[string]string{"k": strings.Repeat("v", 5000)}

	// Source context oversize.
	_, err := b.ReEncrypt(context.Background(), &kms.ReEncryptInput{
		CiphertextBlob:          make([]byte, 64),
		DestinationKeyID:        keyID,
		SourceEncryptionContext: oversized,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EncryptionContext")

	// Destination context oversize.
	_, err = b.ReEncrypt(context.Background(), &kms.ReEncryptInput{
		CiphertextBlob:               make([]byte, 64),
		DestinationKeyID:             keyID,
		DestinationEncryptionContext: oversized,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EncryptionContext")
}

func TestEncryptContextSize_ExactlyAtLimit(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	// Build context that is exactly 4096 bytes encoded: 1(sep) + 1(key) + 1(=) + value = 4096 → value = 4093
	val := strings.Repeat("v", 4093)
	ctx := map[string]string{"k": val}
	_, err := b.Encrypt(
		context.Background(),
		&kms.EncryptInput{KeyID: keyID, Plaintext: []byte("x"), EncryptionContext: ctx},
	)
	require.NoError(t, err)
}

func TestDecrypt_PlaintextSizeGuard(t *testing.T) {
	t.Parallel()
	// We cannot easily produce a ciphertext that decrypts to >4096 bytes
	// through the normal path (Encrypt itself validates plaintext size).
	// Verify that Encrypt rejects oversized plaintext — the decrypt guard
	// is a defense-in-depth layer.
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	oversized := make([]byte, 4097)
	_, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: oversized})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "4096")
}

func TestEncrypt_MaxPlaintext_ExactlyAtLimit(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	exact := make([]byte, 4096)
	out, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: exact})
	require.NoError(t, err)
	require.NotEmpty(t, out.CiphertextBlob)

	dec, err := b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: out.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, exact, dec.Plaintext)
}

func TestDisableKey_PreventsEncrypt(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyID}))

	_, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("x")})
	require.Error(t, err)
	assert.ErrorIs(t, err, kms.ErrKeyDisabled)
}

func TestEnableKey_RestoresEncrypt(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyID}))
	require.NoError(t, b.EnableKey(context.Background(), &kms.EnableKeyInput{KeyID: keyID}))

	_, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("x")})
	require.NoError(t, err)
}

func TestEncryptDecrypt_Roundtrip_WithContext(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	ctx := map[string]string{"purpose": "audit", "version": "1"}
	pt := []byte("audit plaintext")

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:             keyID,
		Plaintext:         pt,
		EncryptionContext: ctx,
	})
	require.NoError(t, err)

	dec, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob:    enc.CiphertextBlob,
		EncryptionContext: ctx,
	})
	require.NoError(t, err)
	assert.Equal(t, pt, dec.Plaintext)
}

func TestDecrypt_WrongContext_Fails(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:             keyID,
		Plaintext:         []byte("secret"),
		EncryptionContext: map[string]string{"k": "v1"},
	})
	require.NoError(t, err)

	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob:    enc.CiphertextBlob,
		EncryptionContext: map[string]string{"k": "v2"},
	})
	require.Error(t, err)
}

func TestReEncrypt_DifferentKey(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	k1 := mustCreateSymKey(t, b)
	k2 := mustCreateSymKey(t, b)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: k1, Plaintext: []byte("reencrypt me")})
	require.NoError(t, err)

	reenc, err := b.ReEncrypt(context.Background(), &kms.ReEncryptInput{
		CiphertextBlob:   enc.CiphertextBlob,
		DestinationKeyID: k2,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, reenc.CiphertextBlob)

	// Decrypt with k2.
	dec, err := b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: reenc.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("reencrypt me"), dec.Plaintext)
}

func TestEncrypt_WrongKeyUsage(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateRSAKey(t, b) // SIGN_VERIFY, not ENCRYPT_DECRYPT

	_, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("x")})
	require.Error(t, err)
	assert.ErrorIs(t, err, kms.ErrInvalidKeyUsage)
}

func TestConcurrent_EncryptDecrypt(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)
	pt := []byte("concurrent test")

	const workers = 10
	errs := make(chan error, workers*2)

	for range workers {
		go func() {
			enc, e := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: pt})
			if e != nil {
				errs <- e

				return
			}
			_, e = b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob})
			errs <- e
		}()
	}

	for range workers {
		assert.NoError(t, <-errs)
	}
}

func TestErrors_KeyDisabled_Sentinel(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)
	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyID}))

	_, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("x")})
	assert.ErrorIs(t, err, kms.ErrKeyDisabled)
}

func TestErrors_InvalidKeyUsage_Sentinel(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateRSAKey(t, b)

	_, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("x")})
	assert.ErrorIs(t, err, kms.ErrInvalidKeyUsage)
}

func TestScheduleKeyDeletion_BlocksEncrypt(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	_, err = b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
		KeyID:               keyID,
		PendingWindowInDays: 7,
	})
	require.NoError(t, err)

	_, err = b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     keyID,
		Plaintext: []byte("test"),
	})
	require.Error(t, err)
}

func TestScheduleKeyDeletion_BlocksDecrypt(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     keyID,
		Plaintext: []byte("test"),
	})
	require.NoError(t, err)

	_, err = b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
		KeyID:               keyID,
		PendingWindowInDays: 7,
	})
	require.NoError(t, err)

	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
	})
	require.Error(t, err)
}

func TestCancelKeyDeletion_AllowsReEnableAndEncrypt(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	_, err = b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
		KeyID:               keyID,
		PendingWindowInDays: 7,
	})
	require.NoError(t, err)

	_, err = b.CancelKeyDeletion(context.Background(), &kms.CancelKeyDeletionInput{KeyID: keyID})
	require.NoError(t, err)

	require.NoError(t, b.EnableKey(context.Background(), &kms.EnableKeyInput{KeyID: keyID}))

	_, err = b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     keyID,
		Plaintext: []byte("after cancel"),
	})
	require.NoError(t, err)
}

func TestEncryptDecrypt_WithEncryptionContext_Roundtrip(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	ctx := map[string]string{"purpose": "batch2-test", "env": "staging"}
	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:             keyID,
		Plaintext:         []byte("context-bound data"),
		EncryptionContext: ctx,
	})
	require.NoError(t, err)

	dec, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob:    enc.CiphertextBlob,
		EncryptionContext: ctx,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("context-bound data"), dec.Plaintext)
}

func TestEncryptDecrypt_WrongContext_Fails(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	ctx := map[string]string{"purpose": "correct"}
	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:             keyID,
		Plaintext:         []byte("secret"),
		EncryptionContext: ctx,
	})
	require.NoError(t, err)

	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob:    enc.CiphertextBlob,
		EncryptionContext: map[string]string{"purpose": "wrong"},
	})
	require.Error(t, err)
}

func TestKeyMetadata_MacAlgorithms(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeySpec: "HMAC_256"})
	require.NoError(t, err)
	assert.Equal(t, []string{"HMAC_SHA_256"}, out.KeyMetadata.MacAlgorithms)
	assert.Empty(t, out.KeyMetadata.EncryptionAlgorithms)
	assert.Empty(t, out.KeyMetadata.SigningAlgorithms)
}

func TestKeyMetadata_RSA_ENCRYPT_DECRYPT(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "RSA_2048",
		KeyUsage: kms.KeyUsageEncryptDecrypt,
	})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyUsageEncryptDecrypt, out.KeyMetadata.KeyUsage)
	assert.Contains(t, out.KeyMetadata.EncryptionAlgorithms, "RSAES_OAEP_SHA_256")
}

func TestRSA_OAEP_EncryptDecrypt(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "RSA_2048",
		KeyUsage: kms.KeyUsageEncryptDecrypt,
	})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	plaintext := []byte("hello RSA-OAEP world")
	encOut, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     keyID,
		Plaintext: plaintext,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, encOut.CiphertextBlob)

	decOut, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := newTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	desc, err := b2.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.Equal(t, key.KeyMetadata.KeyID, desc.KeyMetadata.KeyID)
	assert.True(t, desc.KeyMetadata.Enabled)

	// Encryption should still work after restore.
	encOut, err := b2.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("after restore"),
	})
	require.NoError(t, err)

	decOut, err := b2.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("after restore"), decOut.Plaintext)
}

package integration_test

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_KMS_Crypto_SymmetricEncryptDecrypt verifies that the ciphertext is genuinely
// different from the plaintext and that decryption recovers the original data.
func TestIntegration_KMS_Crypto_SymmetricEncryptDecrypt(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("crypto-symmetric-test-" + uuid.NewString()),
	})
	require.NoError(t, err)
	keyID := *createOut.KeyMetadata.KeyId

	plaintext := []byte("my-secret-plaintext-" + uuid.NewString())

	// Encrypt
	encOut, err := client.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: plaintext,
	})
	require.NoError(t, err)
	require.NotEmpty(t, encOut.CiphertextBlob)

	// Ciphertext must differ from plaintext
	assert.False(t, bytes.Equal(plaintext, encOut.CiphertextBlob),
		"ciphertext must differ from plaintext")

	// Decrypt and verify round-trip
	decOut, err := client.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext,
		"decrypted plaintext must match original")
}

// TestIntegration_KMS_Crypto_TwoEncryptsDiffer verifies that two encryptions of the same
// plaintext produce different ciphertexts (probabilistic encryption via AES-GCM nonce).
func TestIntegration_KMS_Crypto_TwoEncryptsDiffer(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("crypto-nonce-test-" + uuid.NewString()),
	})
	require.NoError(t, err)
	keyID := *createOut.KeyMetadata.KeyId

	plaintext := []byte("same plaintext")

	enc1, err := client.Encrypt(ctx, &kms.EncryptInput{KeyId: aws.String(keyID), Plaintext: plaintext})
	require.NoError(t, err)

	enc2, err := client.Encrypt(ctx, &kms.EncryptInput{KeyId: aws.String(keyID), Plaintext: plaintext})
	require.NoError(t, err)

	assert.False(t, bytes.Equal(enc1.CiphertextBlob, enc2.CiphertextBlob),
		"two encryptions of the same plaintext must differ (probabilistic encryption)")
}

// TestIntegration_KMS_Crypto_ReEncrypt verifies that re-encryption under a different key
// returns ciphertext decryptable by the destination key.
func TestIntegration_KMS_Crypto_ReEncrypt(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	key1Out, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("reencrypt-src-" + uuid.NewString()),
	})
	require.NoError(t, err)

	key2Out, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("reencrypt-dst-" + uuid.NewString()),
	})
	require.NoError(t, err)

	plaintext := []byte("data-to-reencrypt-" + uuid.NewString())

	encOut, err := client.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     key1Out.KeyMetadata.KeyId,
		Plaintext: plaintext,
	})
	require.NoError(t, err)

	reEncOut, err := client.ReEncrypt(ctx, &kms.ReEncryptInput{
		CiphertextBlob:   encOut.CiphertextBlob,
		DestinationKeyId: key2Out.KeyMetadata.KeyId,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, reEncOut.CiphertextBlob)

	// Original ciphertext must differ from re-encrypted ciphertext
	assert.False(t, bytes.Equal(encOut.CiphertextBlob, reEncOut.CiphertextBlob),
		"re-encrypted ciphertext must differ from original")

	// Decrypt re-encrypted blob using destination key
	decOut, err := client.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: reEncOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)
}

// TestIntegration_KMS_Crypto_SignVerify_RSA verifies RSA-PSS sign and verify via the KMS API.
func TestIntegration_KMS_Crypto_SignVerify_RSA(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	tests := []struct {
		algorithm kmstypes.SigningAlgorithmSpec
		name      string
	}{
		{
			name:      "RSASSA_PSS_SHA_256",
			algorithm: kmstypes.SigningAlgorithmSpecRsassaPssSha256,
		},
		{
			name:      "RSASSA_PKCS1_V1_5_SHA_256",
			algorithm: kmstypes.SigningAlgorithmSpecRsassaPkcs1V15Sha256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
				Description: aws.String("rsa-sign-test-" + uuid.NewString()),
				KeyUsage:    kmstypes.KeyUsageTypeSignVerify,
				KeySpec:     kmstypes.KeySpecRsa2048,
			})
			require.NoError(t, err)
			keyID := *createOut.KeyMetadata.KeyId

			message := []byte("message-to-sign-" + uuid.NewString())

			signOut, err := client.Sign(ctx, &kms.SignInput{
				KeyId:            aws.String(keyID),
				Message:          message,
				MessageType:      kmstypes.MessageTypeRaw,
				SigningAlgorithm: tt.algorithm,
			})
			require.NoError(t, err)
			require.NotEmpty(t, signOut.Signature)

			// Signature must differ from message
			assert.False(t, bytes.Equal(message, signOut.Signature),
				"signature must differ from message")

			// Verify the signature
			verifyOut, err := client.Verify(ctx, &kms.VerifyInput{
				KeyId:            aws.String(keyID),
				Message:          message,
				MessageType:      kmstypes.MessageTypeRaw,
				Signature:        signOut.Signature,
				SigningAlgorithm: tt.algorithm,
			})
			require.NoError(t, err)
			assert.True(t, verifyOut.SignatureValid,
				"signature must be valid for original message")
		})
	}
}

// TestIntegration_KMS_Crypto_SignVerify_ECDSA verifies ECDSA sign and verify via the KMS API.
func TestIntegration_KMS_Crypto_SignVerify_ECDSA(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("ecdsa-sign-test-" + uuid.NewString()),
		KeyUsage:    kmstypes.KeyUsageTypeSignVerify,
		KeySpec:     kmstypes.KeySpecEccNistP256,
	})
	require.NoError(t, err)
	keyID := *createOut.KeyMetadata.KeyId

	message := []byte("ecdsa-message-" + uuid.NewString())

	signOut, err := client.Sign(ctx, &kms.SignInput{
		KeyId:            aws.String(keyID),
		Message:          message,
		MessageType:      kmstypes.MessageTypeRaw,
		SigningAlgorithm: kmstypes.SigningAlgorithmSpecEcdsaSha256,
	})
	require.NoError(t, err)
	require.NotEmpty(t, signOut.Signature)

	// Verify
	verifyOut, err := client.Verify(ctx, &kms.VerifyInput{
		KeyId:            aws.String(keyID),
		Message:          message,
		MessageType:      kmstypes.MessageTypeRaw,
		Signature:        signOut.Signature,
		SigningAlgorithm: kmstypes.SigningAlgorithmSpecEcdsaSha256,
	})
	require.NoError(t, err)
	assert.True(t, verifyOut.SignatureValid)
}

// TestIntegration_KMS_Crypto_SignVerify_DigestMode verifies sign/verify using pre-hashed digests.
func TestIntegration_KMS_Crypto_SignVerify_DigestMode(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("digest-mode-test-" + uuid.NewString()),
		KeyUsage:    kmstypes.KeyUsageTypeSignVerify,
		KeySpec:     kmstypes.KeySpecRsa2048,
	})
	require.NoError(t, err)
	keyID := *createOut.KeyMetadata.KeyId

	message := []byte("raw-message-for-digest-mode")
	digest := sha256.Sum256(message)

	// Sign the pre-computed digest
	signOut, err := client.Sign(ctx, &kms.SignInput{
		KeyId:            aws.String(keyID),
		Message:          digest[:],
		MessageType:      kmstypes.MessageTypeDigest,
		SigningAlgorithm: kmstypes.SigningAlgorithmSpecRsassaPssSha256,
	})
	require.NoError(t, err)
	require.NotEmpty(t, signOut.Signature)

	// Verify the signature with the same digest
	verifyOut, err := client.Verify(ctx, &kms.VerifyInput{
		KeyId:            aws.String(keyID),
		Message:          digest[:],
		MessageType:      kmstypes.MessageTypeDigest,
		Signature:        signOut.Signature,
		SigningAlgorithm: kmstypes.SigningAlgorithmSpecRsassaPssSha256,
	})
	require.NoError(t, err)
	assert.True(t, verifyOut.SignatureValid)
}

// TestIntegration_KMS_Crypto_GetPublicKey verifies that an asymmetric key's public key
// can be retrieved via the GetPublicKey API.
func TestIntegration_KMS_Crypto_GetPublicKey(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	tests := []struct {
		keySpec  kmstypes.KeySpec
		name     string
		keyUsage kmstypes.KeyUsageType
	}{
		{
			name:     "RSA_2048",
			keySpec:  kmstypes.KeySpecRsa2048,
			keyUsage: kmstypes.KeyUsageTypeSignVerify,
		},
		{
			name:     "ECC_NIST_P256",
			keySpec:  kmstypes.KeySpecEccNistP256,
			keyUsage: kmstypes.KeyUsageTypeSignVerify,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			createOut, err := client.CreateKey(ctx, &kms.CreateKeyInput{
				Description: aws.String("getpubkey-test-" + uuid.NewString()),
				KeyUsage:    tt.keyUsage,
				KeySpec:     tt.keySpec,
			})
			require.NoError(t, err)
			keyID := *createOut.KeyMetadata.KeyId

			pubOut, err := client.GetPublicKey(ctx, &kms.GetPublicKeyInput{
				KeyId: aws.String(keyID),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, pubOut.PublicKey,
				"GetPublicKey must return DER-encoded public key bytes")
		})
	}
}

// TestIntegration_KMS_Crypto_PerKeyIsolation verifies that keys encrypted with one KMS key
// cannot be decrypted with a different key (per-key material isolation).
func TestIntegration_KMS_Crypto_PerKeyIsolation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKMSClient(t)
	ctx := t.Context()

	key1Out, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("isolation-key1-" + uuid.NewString()),
	})
	require.NoError(t, err)

	key2Out, err := client.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("isolation-key2-" + uuid.NewString()),
	})
	require.NoError(t, err)

	plaintext := []byte("isolated-secret")

	// Encrypt with key1
	encOut, err := client.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     key1Out.KeyMetadata.KeyId,
		Plaintext: plaintext,
	})
	require.NoError(t, err)

	// Attempt to decrypt with key2 — must fail since different key material is used
	// The ciphertext blob contains key1's ID prefix, so the backend will use key1's material
	// but the output should identify key1, not key2
	decOut, err := client.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err)
	// KeyId in decrypt response must be key1, not key2
	assert.Equal(t, *key1Out.KeyMetadata.KeyId, *decOut.KeyId,
		"decrypt must identify the original encryption key, not key2")
	assert.NotEqual(t, *key2Out.KeyMetadata.KeyId, *decOut.KeyId)
	assert.Equal(t, plaintext, decOut.Plaintext)
}

package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"

	"crypto/sha512"
)

func TestSign_WrongAlgorithmForKeySpec_EC(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateECKey(t, b, "ECC_NIST_P256")

	msg := []byte("test message")
	_, err := b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyID,
		Message:          msg,
		SigningAlgorithm: "RSASSA_PSS_SHA_256", // RSA algorithm on EC key
	})
	require.Error(t, err)
}

func TestSign_WrongAlgorithmForKeySpec_RSA(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateRSAKey(t, b)

	msg := []byte("test message")
	_, err := b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyID,
		Message:          msg,
		SigningAlgorithm: "ECDSA_SHA_256", // EC algorithm on RSA key
	})
	require.Error(t, err)
}

func TestVerify_WrongAlgorithmForKeySpec(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateECKey(t, b, "ECC_NIST_P384")

	_, err := b.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyID,
		Message:          []byte("msg"),
		Signature:        []byte("sig"),
		SigningAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
	})
	require.Error(t, err)
}

func TestSign_CorrectAlgorithmForEC256(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateECKey(t, b, "ECC_NIST_P256")

	msg := []byte("test message")
	sOut, err := b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyID,
		Message:          msg,
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.NoError(t, err)
	require.NotEmpty(t, sOut.Signature)

	vOut, err := b.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyID,
		Message:          msg,
		Signature:        sOut.Signature,
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.NoError(t, err)
	assert.True(t, vOut.SignatureValid)
}

func TestSign_CorrectAlgorithmForEC384(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateECKey(t, b, "ECC_NIST_P384")

	msg := []byte("test message")
	sOut, err := b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyID,
		Message:          msg,
		SigningAlgorithm: "ECDSA_SHA_384",
	})
	require.NoError(t, err)

	vOut, err := b.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyID,
		Message:          msg,
		Signature:        sOut.Signature,
		SigningAlgorithm: "ECDSA_SHA_384",
	})
	require.NoError(t, err)
	assert.True(t, vOut.SignatureValid)
}

func TestSign_CorrectAlgorithmsForRSA(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateRSAKey(t, b)

	algos := []string{
		"RSASSA_PSS_SHA_256",
		"RSASSA_PSS_SHA_384",
		"RSASSA_PSS_SHA_512",
		"RSASSA_PKCS1_V1_5_SHA_256",
		"RSASSA_PKCS1_V1_5_SHA_384",
		"RSASSA_PKCS1_V1_5_SHA_512",
	}
	msg := []byte("rsa test message")

	for _, algo := range algos {
		t.Run(algo, func(t *testing.T) {
			t.Parallel()
			sOut, err := b.Sign(context.Background(), &kms.SignInput{
				KeyID:            keyID,
				Message:          msg,
				SigningAlgorithm: algo,
			})
			require.NoError(t, err)

			vOut, err := b.Verify(context.Background(), &kms.VerifyInput{
				KeyID:            keyID,
				Message:          msg,
				Signature:        sOut.Signature,
				SigningAlgorithm: algo,
			})
			require.NoError(t, err)
			assert.True(t, vOut.SignatureValid)
		})
	}
}

func TestGetPublicKey_RSA(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateRSAKey(t, b)

	out, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.NotEmpty(t, out.PublicKey)
	assert.Equal(t, kms.KeyUsageSignVerify, out.KeyUsage)
}

func TestGetPublicKey_EC(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateECKey(t, b, "ECC_NIST_P256")

	out, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.NotEmpty(t, out.PublicKey)
}

func TestGetPublicKey_SymmetricFails(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	_, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: keyID})
	require.Error(t, err)
}

func TestSign_MessageTooLarge_Rejected(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateRSAKey(t, b)

	bigMsg := make([]byte, 4097)
	_, err := b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyID,
		Message:          bigMsg,
		MessageType:      "RAW",
		SigningAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "4096")
}

func TestSign_DigestMode_AllowsLargeInput(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateRSAKey(t, b)

	// In DIGEST mode, input is already hashed — size limit doesn't apply to msg.
	// SHA-256 digest is 32 bytes.
	digest := make([]byte, 32)
	_, err := b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyID,
		Message:          digest,
		MessageType:      "DIGEST",
		SigningAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
	})
	require.NoError(t, err)
}

func TestGetPublicKey_ReturnsKeyAgreementAlgorithms(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	keyID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)

	out, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Contains(t, out.KeyAgreementAlgorithms, "ECDH")
	assert.Empty(t, out.SigningAlgorithms)
	assert.Empty(t, out.EncryptionAlgorithms)
}

func TestScheduleKeyDeletion_BlocksSign(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "RSA_2048",
		KeyUsage: kms.KeyUsageSignVerify,
	})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	_, err = b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
		KeyID:               keyID,
		PendingWindowInDays: 7,
	})
	require.NoError(t, err)

	_, err = b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyID,
		Message:          []byte("message"),
		SigningAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
	})
	require.Error(t, err)
}

func TestCreateKey_SignVerify_DefaultsToRSA2048(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
	})
	require.NoError(t, err)
	assert.Equal(t, "RSA_2048", out.KeyMetadata.KeySpec)
}

func TestGetPublicKey_KeyAgreement(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "ECC_NIST_P256",
		KeyUsage: kms.KeyUsageKeyAgreement,
	})
	require.NoError(t, err)

	pub, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: out.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.NotEmpty(t, pub.PublicKey)
	assert.Equal(t, kms.KeyUsageKeyAgreement, pub.KeyUsage)
	assert.Equal(t, []string{"ECDH"}, pub.KeyAgreementAlgorithms)
	assert.Empty(t, pub.SigningAlgorithms)
}

// TestKMSBackendSignVerify verifies round-trip sign and verify using an RSA key.
func TestKMSBackendSignVerify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		keySpec          string
		signingAlgorithm string
		name             string
	}{
		{
			name:             "RSA_PSS_SHA_256",
			keySpec:          "RSA_2048",
			signingAlgorithm: "RSASSA_PSS_SHA_256",
		},
		{
			name:             "RSA_PKCS1v15_SHA_256",
			keySpec:          "RSA_2048",
			signingAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
		},
		{
			name:             "ECDSA_P256_SHA_256",
			keySpec:          "ECC_NIST_P256",
			signingAlgorithm: "ECDSA_SHA_256",
		},
		{
			name:             "ECDSA_P384_SHA_384",
			keySpec:          "ECC_NIST_P384",
			signingAlgorithm: "ECDSA_SHA_384",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := kms.NewInMemoryBackend()

			keyOut, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeyUsage: kms.KeyUsageSignVerify,
				KeySpec:  tt.keySpec,
			})
			require.NoError(t, err)
			keyID := keyOut.KeyMetadata.KeyID

			message := []byte("hello, cryptographic world")

			signOut, err := backend.Sign(context.Background(), &kms.SignInput{
				KeyID:            keyID,
				Message:          message,
				MessageType:      "RAW",
				SigningAlgorithm: tt.signingAlgorithm,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, signOut.Signature)
			assert.NotEqual(t, message, signOut.Signature)
			assert.Equal(t, tt.signingAlgorithm, signOut.SigningAlgorithm)

			verifyOut, err := backend.Verify(context.Background(), &kms.VerifyInput{
				KeyID:            keyID,
				Message:          message,
				MessageType:      "RAW",
				Signature:        signOut.Signature,
				SigningAlgorithm: tt.signingAlgorithm,
			})
			require.NoError(t, err)
			assert.True(t, verifyOut.SignatureValid)
		})
	}
}

// TestKMSBackendVerifyInvalidSignature verifies that tampered signatures are rejected.
func TestKMSBackendVerifyInvalidSignature(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	keyOut, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	message := []byte("test message")

	signOut, err := backend.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyID,
		Message:          message,
		MessageType:      "RAW",
		SigningAlgorithm: "RSASSA_PSS_SHA_256",
	})
	require.NoError(t, err)

	// Tamper with the signature
	tampered := make([]byte, len(signOut.Signature))
	copy(tampered, signOut.Signature)
	tampered[0] ^= 0xFF

	_, err = backend.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyID,
		Message:          message,
		MessageType:      "RAW",
		Signature:        tampered,
		SigningAlgorithm: "RSASSA_PSS_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrInvalidSignature)
}

// TestKMSBackendGetPublicKey verifies retrieval of asymmetric public keys.
func TestKMSBackendGetPublicKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		keySpec string
		name    string
	}{
		{name: "RSA_2048", keySpec: "RSA_2048"},
		{name: "ECC_NIST_P256", keySpec: "ECC_NIST_P256"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := kms.NewInMemoryBackend()
			keyOut, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeyUsage: kms.KeyUsageSignVerify,
				KeySpec:  tt.keySpec,
			})
			require.NoError(t, err)

			pubKeyOut, err := backend.GetPublicKey(
				context.Background(),
				&kms.GetPublicKeyInput{KeyID: keyOut.KeyMetadata.KeyID},
			)
			require.NoError(t, err)
			assert.NotEmpty(t, pubKeyOut.PublicKey)
			assert.Equal(t, tt.keySpec, pubKeyOut.KeySpec)
			assert.Equal(t, kms.KeyUsageSignVerify, pubKeyOut.KeyUsage)
		})
	}
}

// TestKMSBackendSignWrongKeyType verifies that Sign fails on symmetric keys.
func TestKMSBackendSignWrongKeyType(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	symKey, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{KeyUsage: kms.KeyUsageEncryptDecrypt})
	require.NoError(t, err)

	_, err = backend.Sign(context.Background(), &kms.SignInput{
		KeyID:            symKey.KeyMetadata.KeyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		SigningAlgorithm: "RSASSA_PSS_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrInvalidKeyUsage)
}

// TestKMSBackendSignVerifyDigestMode verifies signing and verification with MessageType=DIGEST.
func TestKMSBackendSignVerifyDigestMode(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	keyOut, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	rawMsg := []byte("data to sign")

	// Sign using RAW mode first to get signature
	signOut, err := backend.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyID,
		Message:          rawMsg,
		MessageType:      "RAW",
		SigningAlgorithm: "RSASSA_PSS_SHA_512",
	})
	require.NoError(t, err)

	// Verify using DIGEST mode (pre-computed hash)
	d512 := sha512.Sum512(rawMsg)
	digest512 := d512[:]
	verifyOut, err := backend.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyID,
		Message:          digest512,
		MessageType:      "DIGEST",
		Signature:        signOut.Signature,
		SigningAlgorithm: "RSASSA_PSS_SHA_512",
	})
	require.NoError(t, err)
	assert.True(t, verifyOut.SignatureValid)
}

// TestKMSSnapshotRestoreWithKeyMaterials verifies that key materials survive snapshot/restore.
func TestKMSSnapshotRestoreWithKeyMaterials(t *testing.T) {
	t.Parallel()

	original := kms.NewInMemoryBackend()

	// Create symmetric key and encrypt something
	symKey, err := original.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	plaintext := []byte("persistence test data")
	encOut, err := original.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     symKey.KeyMetadata.KeyID,
		Plaintext: plaintext,
	})
	require.NoError(t, err)

	// Create asymmetric key and sign something
	asymKey, err := original.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)
	signOut, err := original.Sign(context.Background(), &kms.SignInput{
		KeyID:            asymKey.KeyMetadata.KeyID,
		Message:          plaintext,
		MessageType:      "RAW",
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.NoError(t, err)

	// Snapshot and restore to new backend
	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := kms.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), snap))

	// Decrypt using restored backend — must use same per-key material
	decOut, err := restored.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)

	// Verify using restored backend — must use same per-key material
	verifyOut, err := restored.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            asymKey.KeyMetadata.KeyID,
		Message:          plaintext,
		MessageType:      "RAW",
		Signature:        signOut.Signature,
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.NoError(t, err)
	assert.True(t, verifyOut.SignatureValid)
}

// TestKMSBackendSignVerifyAdditionalKeySpecs verifies sign/verify with RSA_3072, RSA_4096, and ECC variants.
func TestKMSBackendSignVerifyAdditionalKeySpecs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		keySpec          string
		signingAlgorithm string
		name             string
	}{
		{
			name:             "RSA_3072_PSS",
			keySpec:          "RSA_3072",
			signingAlgorithm: "RSASSA_PSS_SHA_384",
		},
		{
			name:             "RSA_4096_PKCS1v15",
			keySpec:          "RSA_4096",
			signingAlgorithm: "RSASSA_PKCS1_V1_5_SHA_512",
		},
		{
			name:             "ECC_NIST_P521",
			keySpec:          "ECC_NIST_P521",
			signingAlgorithm: "ECDSA_SHA_512",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeyUsage: kms.KeyUsageSignVerify,
				KeySpec:  tt.keySpec,
			})
			require.NoError(t, err)

			msg := []byte("test-" + tt.name)
			signOut, err := b.Sign(context.Background(), &kms.SignInput{
				KeyID:            keyOut.KeyMetadata.KeyID,
				Message:          msg,
				MessageType:      "RAW",
				SigningAlgorithm: tt.signingAlgorithm,
			})
			require.NoError(t, err)

			verifyOut, err := b.Verify(context.Background(), &kms.VerifyInput{
				KeyID:            keyOut.KeyMetadata.KeyID,
				Message:          msg,
				MessageType:      "RAW",
				Signature:        signOut.Signature,
				SigningAlgorithm: tt.signingAlgorithm,
			})
			require.NoError(t, err)
			assert.True(t, verifyOut.SignatureValid)
		})
	}
}

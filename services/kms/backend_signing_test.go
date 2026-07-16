package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"

	"crypto/sha512"
	"strings"
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

// TestKMSSnapshotRestoreRSA3072 verifies snapshot/restore preserves RSA-3072 key material.
func TestKMSSnapshotRestoreRSA3072(t *testing.T) {
	t.Parallel()

	original := kms.NewInMemoryBackend()
	keyOut, err := original.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_3072",
	})
	require.NoError(t, err)

	msg := []byte("rsa-3072-persistence-test")
	signOut, err := original.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          msg,
		MessageType:      "RAW",
		SigningAlgorithm: "RSASSA_PSS_SHA_384",
	})
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := kms.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), snap))

	verifyOut, err := restored.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          msg,
		MessageType:      "RAW",
		Signature:        signOut.Signature,
		SigningAlgorithm: "RSASSA_PSS_SHA_384",
	})
	require.NoError(t, err)
	assert.True(t, verifyOut.SignatureValid)
}

// TestKMSBackendGetPublicKeySymmetricFails verifies GetPublicKey on symmetric keys returns an error.
func TestKMSBackendGetPublicKeySymmetricFails(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeyUsage: kms.KeyUsageEncryptDecrypt})
	require.NoError(t, err)

	_, err = b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: keyOut.KeyMetadata.KeyID})
	require.ErrorIs(t, err, kms.ErrInvalidKeyUsage)
}

// TestKMSBackendVerifyDisabledKey verifies that Verify fails on a disabled key.
func TestKMSBackendVerifyDisabledKey(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	signOut, err := b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.NoError(t, err)

	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyID}))

	_, err = b.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		Signature:        signOut.Signature,
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrKeyDisabled)
}

// TestKMSBackendSignDisabledKey verifies that Sign fails on a disabled key.
func TestKMSBackendSignDisabledKey(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)

	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyOut.KeyMetadata.KeyID}))

	_, err = b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		SigningAlgorithm: "RSASSA_PSS_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrKeyDisabled)
}

// TestKMSKeyMetadataSigningAlgorithms verifies that DescribeKey returns signing algorithms
// for asymmetric keys.
func TestKMSKeyMetadataSigningAlgorithms(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)

	descOut, err := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: keyOut.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.NotEmpty(t, descOut.KeyMetadata.SigningAlgorithms)
	assert.Contains(t, descOut.KeyMetadata.SigningAlgorithms, "RSASSA_PSS_SHA_256")
}

// TestKMSBackendSignUnsupportedAlgorithm verifies that unsupported signing algorithms return an error.
func TestKMSBackendSignUnsupportedAlgorithm(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)

	_, err = b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		SigningAlgorithm: "UNSUPPORTED_ALGORITHM",
	})
	require.Error(t, err)
}

// TestKMSBackendVerifyUnsupportedAlgorithm verifies that unsupported algorithms return an error.
func TestKMSBackendVerifyUnsupportedAlgorithm(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)

	_, err = b.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		Signature:        []byte("sig"),
		SigningAlgorithm: "UNSUPPORTED_ALGORITHM",
	})
	require.Error(t, err)
}

// TestKMSBackendVerifyECDSAInvalidASN1 verifies that a non-ASN.1 ECDSA signature is rejected.
func TestKMSBackendVerifyECDSAInvalidASN1(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)

	// Provide a signature that is not valid ASN.1
	invalidSig := []byte("not-asn1-signature-data-at-all-!!!!")
	_, err = b.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          []byte("test message"),
		MessageType:      "RAW",
		Signature:        invalidSig,
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrInvalidSignature)
}

// TestKMSBackendVerifyECDSAWrongSignature verifies that a well-formed but wrong ECDSA signature is rejected.
func TestKMSBackendVerifyECDSAWrongSignature(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	// Sign one message
	signOut, err := b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyID,
		Message:          []byte("message-a"),
		MessageType:      "RAW",
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.NoError(t, err)

	// Verify against a different message — should fail
	_, err = b.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyID,
		Message:          []byte("message-b"),
		MessageType:      "RAW",
		Signature:        signOut.Signature,
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrInvalidSignature)
}

// TestKMSBackendSnapshotRestoreECDSA verifies snapshot/restore preserves ECDSA key material.
func TestKMSBackendSnapshotRestoreECDSA(t *testing.T) {
	t.Parallel()

	orig := kms.NewInMemoryBackend()
	keyOut, err := orig.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P384",
	})
	require.NoError(t, err)

	msg := []byte("ecdsa-snapshot-test")
	signOut, err := orig.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          msg,
		MessageType:      "RAW",
		SigningAlgorithm: "ECDSA_SHA_384",
	})
	require.NoError(t, err)

	snap := orig.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := kms.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), snap))

	verifyOut, err := restored.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          msg,
		MessageType:      "RAW",
		Signature:        signOut.Signature,
		SigningAlgorithm: "ECDSA_SHA_384",
	})
	require.NoError(t, err)
	assert.True(t, verifyOut.SignatureValid)
}

// TestKMSBackendGetPublicKeyDisabledKey verifies GetPublicKey fails on a disabled key.
func TestKMSBackendGetPublicKeyDisabledKey(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)

	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyOut.KeyMetadata.KeyID}))

	_, err = b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: keyOut.KeyMetadata.KeyID})
	require.ErrorIs(t, err, kms.ErrKeyDisabled)
}

// TestKMSBackendGetPublicKeyNotFound verifies GetPublicKey returns ErrKeyNotFound.
func TestKMSBackendGetPublicKeyNotFound(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	_, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: "non-existent-key"})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSCreateKeyIncompatibleSpecUsage verifies that CreateKey fails when KeySpec
// and KeyUsage are incompatible (e.g. RSA key with ENCRYPT_DECRYPT usage).
func TestKMSCreateKeyIncompatibleSpecUsage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keySpec  string
		keyUsage string
	}{
		{
			name:     "ECC_NIST_P256_with_ENCRYPT_DECRYPT",
			keySpec:  "ECC_NIST_P256",
			keyUsage: kms.KeyUsageEncryptDecrypt,
		},
		{
			name:     "SYMMETRIC_DEFAULT_with_SIGN_VERIFY",
			keySpec:  "SYMMETRIC_DEFAULT",
			keyUsage: kms.KeyUsageSignVerify,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			_, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeySpec:  tt.keySpec,
				KeyUsage: tt.keyUsage,
			})
			require.ErrorIs(t, err, kms.ErrInvalidKeyUsage)
		})
	}
}

// TestKMSSignVerifyAlgorithmKeySpecMismatch verifies that Sign/Verify reject algorithms
// that are incompatible with the key spec (e.g. ECDSA_SHA_256 on ECC_NIST_P384).
func TestKMSSignVerifyAlgorithmKeySpecMismatch(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P384",
	})
	require.NoError(t, err)

	msg := []byte("test")

	// ECDSA_SHA_256 is only valid for ECC_NIST_P256, not ECC_NIST_P384
	_, signErr := b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          msg,
		MessageType:      "RAW",
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.Error(t, signErr)

	_, verifyErr := b.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          msg,
		MessageType:      "RAW",
		Signature:        []byte("sig"),
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.Error(t, verifyErr)
}

// TestKMSHashAndAlgorithmInvalidMessageType verifies that an invalid message type is rejected.
func TestKMSHashAndAlgorithmInvalidMessageType(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)

	_, err = b.Sign(context.Background(), &kms.SignInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          []byte("test"),
		MessageType:      "INVALID_TYPE",
		SigningAlgorithm: "RSASSA_PSS_SHA_256",
	})
	require.Error(t, err)
}

// TestKMSKeyMaterialUnavailableAfterManualRestore verifies that Encrypt/Sign/Verify
// return ErrKeyMaterialUnavailable when key material is missing after a snapshot restore
// from an old format that did not persist key materials.
func TestKMSKeyMaterialUnavailableAfterManualRestore(t *testing.T) {
	t.Parallel()

	orig := kms.NewInMemoryBackend()
	symKey, err := orig.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	asymKey, err := orig.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)

	// Build a snapshot without key_materials to simulate an old-format snapshot.
	snap := orig.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// Strip key_materials from the JSON.
	stripped := strings.ReplaceAll(string(snap), `"key_materials":`, `"_key_materials":`)

	restored := kms.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), []byte(stripped)))

	// Encrypt should fail with ErrKeyMaterialUnavailable.
	_, encErr := restored.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     symKey.KeyMetadata.KeyID,
		Plaintext: []byte("test"),
	})
	require.ErrorIs(t, encErr, kms.ErrKeyMaterialUnavailable)

	// Sign should fail with ErrKeyMaterialUnavailable.
	_, signErr := restored.Sign(context.Background(), &kms.SignInput{
		KeyID:            asymKey.KeyMetadata.KeyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.ErrorIs(t, signErr, kms.ErrKeyMaterialUnavailable)
}

// TestKMSBackendGetPublicKeyMaterialUnavailable verifies GetPublicKey returns ErrKeyMaterialUnavailable
// when key material is missing.
func TestKMSBackendGetPublicKeyMaterialUnavailable(t *testing.T) {
	t.Parallel()

	orig := kms.NewInMemoryBackend()
	key, err := orig.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)

	snap := orig.Snapshot(t.Context())
	require.NotEmpty(t, snap)
	stripped := strings.ReplaceAll(string(snap), `"key_materials":`, `"_key_materials":`)

	restored := kms.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), []byte(stripped)))

	_, err = restored.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: key.KeyMetadata.KeyID})
	require.ErrorIs(t, err, kms.ErrKeyMaterialUnavailable)
}

func TestECDSASignVerifyAllAlgorithms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		keySpec   string
		algorithm string
	}{
		{name: "p384_sha384", keySpec: "ECC_NIST_P384", algorithm: "ECDSA_SHA_384"},
		{name: "p521_sha512", keySpec: "ECC_NIST_P521", algorithm: "ECDSA_SHA_512"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(
				context.Background(),
				&kms.CreateKeyInput{KeySpec: tt.keySpec, KeyUsage: "SIGN_VERIFY"},
			)
			require.NoError(t, err)

			signOut, err := b.Sign(context.Background(), &kms.SignInput{
				KeyID:            key.KeyMetadata.KeyID,
				Message:          []byte("message"),
				SigningAlgorithm: tt.algorithm,
			})
			require.NoError(t, err)

			verOut, err := b.Verify(context.Background(), &kms.VerifyInput{
				KeyID:            key.KeyMetadata.KeyID,
				Message:          []byte("message"),
				Signature:        signOut.Signature,
				SigningAlgorithm: tt.algorithm,
			})
			require.NoError(t, err)
			assert.True(t, verOut.SignatureValid)
		})
	}
}

func TestRSAPKCS1SignVerify(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "RSA_2048",
		KeyUsage: "SIGN_VERIFY",
	})
	require.NoError(t, err)

	signOut, err := b.Sign(context.Background(), &kms.SignInput{
		KeyID:            key.KeyMetadata.KeyID,
		Message:          []byte("data"),
		SigningAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
	})
	require.NoError(t, err)

	verOut, err := b.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            key.KeyMetadata.KeyID,
		Message:          []byte("data"),
		Signature:        signOut.Signature,
		SigningAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
	})
	require.NoError(t, err)
	assert.True(t, verOut.SignatureValid)
}

func TestRSAPSSSignVerifyAllSizes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keySpec string
		algo    string
	}{
		{name: "rsa3072_pss_sha384", keySpec: "RSA_3072", algo: "RSASSA_PSS_SHA_384"},
		{name: "rsa4096_pss_sha512", keySpec: "RSA_4096", algo: "RSASSA_PSS_SHA_512"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(
				context.Background(),
				&kms.CreateKeyInput{KeySpec: tt.keySpec, KeyUsage: "SIGN_VERIFY"},
			)
			require.NoError(t, err)

			msg := []byte("test message for " + tt.name)
			signOut, err := b.Sign(context.Background(), &kms.SignInput{
				KeyID:            key.KeyMetadata.KeyID,
				Message:          msg,
				SigningAlgorithm: tt.algo,
			})
			require.NoError(t, err)

			verOut, err := b.Verify(context.Background(), &kms.VerifyInput{
				KeyID:            key.KeyMetadata.KeyID,
				Message:          msg,
				Signature:        signOut.Signature,
				SigningAlgorithm: tt.algo,
			})
			require.NoError(t, err)
			assert.True(t, verOut.SignatureValid)
		})
	}
}

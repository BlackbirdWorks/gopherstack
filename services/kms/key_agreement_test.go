package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestDeriveSharedSecret_ECDH(t *testing.T) {
	t.Parallel()
	b := newBackend(t)

	k1Out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "ECC_NIST_P256",
		KeyUsage: kms.KeyUsageKeyAgreement,
	})
	require.NoError(t, err)

	k2Out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "ECC_NIST_P256",
		KeyUsage: kms.KeyUsageKeyAgreement,
	})
	require.NoError(t, err)

	// Get peer public key.
	pubOut, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: k2Out.KeyMetadata.KeyID})
	require.NoError(t, err)

	out, err := b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 k1Out.KeyMetadata.KeyID,
		KeyAgreementAlgorithm: "ECDH",
		PublicKey:             pubOut.PublicKey,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.SharedSecret)
}

func TestDeriveSharedSecret_P256_Roundtrip(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	key1ID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)
	key2ID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)

	pub1, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: key1ID})
	require.NoError(t, err)
	pub2, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: key2ID})
	require.NoError(t, err)

	// Derive shared secret from key1 with key2's public key
	out1, err := b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 key1ID,
		KeyAgreementAlgorithm: "ECDH",
		PublicKey:             pub2.PublicKey,
	})
	require.NoError(t, err)

	// Derive shared secret from key2 with key1's public key
	out2, err := b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 key2ID,
		KeyAgreementAlgorithm: "ECDH",
		PublicKey:             pub1.PublicKey,
	})
	require.NoError(t, err)

	// ECDH is commutative: both shared secrets must be identical
	assert.Equal(t, out1.SharedSecret, out2.SharedSecret)
}

func TestDeriveSharedSecret_P384_Roundtrip(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	key1ID := b2mustCreateECKey(t, b, "ECC_NIST_P384", kms.KeyUsageKeyAgreement)
	key2ID := b2mustCreateECKey(t, b, "ECC_NIST_P384", kms.KeyUsageKeyAgreement)

	pub2, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: key2ID})
	require.NoError(t, err)

	out, err := b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 key1ID,
		KeyAgreementAlgorithm: "ECDH",
		PublicKey:             pub2.PublicKey,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.SharedSecret)
	assert.Equal(t, "ECDH", out.KeyAgreementAlgorithm)
}

func TestDeriveSharedSecret_P521_Roundtrip(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	key1ID := b2mustCreateECKey(t, b, "ECC_NIST_P521", kms.KeyUsageKeyAgreement)
	key2ID := b2mustCreateECKey(t, b, "ECC_NIST_P521", kms.KeyUsageKeyAgreement)

	pub2, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: key2ID})
	require.NoError(t, err)

	out, err := b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 key1ID,
		KeyAgreementAlgorithm: "ECDH",
		PublicKey:             pub2.PublicKey,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.SharedSecret)
}

func TestDeriveSharedSecret_WrongKeyUsage_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	// ECC SIGN_VERIFY key, not KEY_AGREEMENT
	keyID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageSignVerify)
	peerID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)

	pub, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: peerID})
	require.NoError(t, err)

	_, err = b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 keyID,
		KeyAgreementAlgorithm: "ECDH",
		PublicKey:             pub.PublicKey,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidKeyUsageException")
}

func TestDeriveSharedSecret_DisabledKey_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	keyID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)
	peerID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)

	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyID}))

	pub, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: peerID})
	require.NoError(t, err)

	_, err = b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 keyID,
		KeyAgreementAlgorithm: "ECDH",
		PublicKey:             pub.PublicKey,
	})
	require.Error(t, err)
}

func TestDeriveSharedSecret_InvalidPublicKey_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	keyID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)

	_, err := b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 keyID,
		KeyAgreementAlgorithm: "ECDH",
		PublicKey:             []byte("not a valid DER public key"),
	})
	require.Error(t, err)
}

func TestDeriveSharedSecret_WrongAlgorithm_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	keyID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)
	peerID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)

	pub, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: peerID})
	require.NoError(t, err)

	_, err = b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 keyID,
		KeyAgreementAlgorithm: "RSA_OAEP", // Wrong algorithm
		PublicKey:             pub.PublicKey,
	})
	require.Error(t, err)
}

func TestDeriveSharedSecret_EmptyPublicKey_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	keyID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)

	_, err := b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 keyID,
		KeyAgreementAlgorithm: "ECDH",
		PublicKey:             nil,
	})
	require.Error(t, err)
}

func TestDeriveSharedSecret_InvalidAlgorithm(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "ECC_NIST_P256",
		KeyUsage: kms.KeyUsageKeyAgreement,
	})
	require.NoError(t, err)

	_, err = b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 out.KeyMetadata.KeyID,
		KeyAgreementAlgorithm: "RSA",
		PublicKey:             []byte("fake"),
	})
	require.ErrorIs(t, err, kms.ErrValidation)
}

// TestDeriveSharedSecret verifies ECDH key agreement between two KEY_AGREEMENT ECC keys.
func TestDeriveSharedSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keySpec string
		wantErr bool
	}{
		{name: "ecc_p256", keySpec: "ECC_NIST_P256", wantErr: false},
		{name: "ecc_p384", keySpec: "ECC_NIST_P384", wantErr: false},
		{name: "ecc_p521", keySpec: "ECC_NIST_P521", wantErr: false},
		{name: "wrong_usage_symmetric", keySpec: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()

			if tt.wantErr {
				// Use a symmetric (ENCRYPT_DECRYPT) key – should fail DeriveSharedSecret.
				symKey, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				// We also need a peer ECC key for its public key.
				peerB := kms.NewInMemoryBackend()
				peerOut, err := peerB.CreateKey(context.Background(), &kms.CreateKeyInput{
					KeySpec:  "ECC_NIST_P256",
					KeyUsage: kms.KeyUsageKeyAgreement,
				})
				require.NoError(t, err)

				pubOut, err := peerB.GetPublicKey(
					context.Background(),
					&kms.GetPublicKeyInput{KeyID: peerOut.KeyMetadata.KeyID},
				)
				require.NoError(t, err)

				_, err = b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
					KeyID:                 symKey.KeyMetadata.KeyID,
					KeyAgreementAlgorithm: "ECDH",
					PublicKey:             pubOut.PublicKey,
				})
				require.Error(t, err)

				return
			}

			// Create a KEY_AGREEMENT ECC key in backend A.
			keyA, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeySpec:  tt.keySpec,
				KeyUsage: kms.KeyUsageKeyAgreement,
			})
			require.NoError(t, err)

			// Create a KEY_AGREEMENT ECC key in backend B (peer).
			bPeer := kms.NewInMemoryBackend()
			keyB, err := bPeer.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeySpec:  tt.keySpec,
				KeyUsage: kms.KeyUsageKeyAgreement,
			})
			require.NoError(t, err)

			// Get public keys.
			pubA, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: keyA.KeyMetadata.KeyID})
			require.NoError(t, err)

			pubB, err := bPeer.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: keyB.KeyMetadata.KeyID})
			require.NoError(t, err)

			// Derive shared secret from A's perspective (using B's public key).
			outA, err := b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
				KeyID:                 keyA.KeyMetadata.KeyID,
				KeyAgreementAlgorithm: "ECDH",
				PublicKey:             pubB.PublicKey,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, outA.SharedSecret)
			assert.Equal(t, "ECDH", outA.KeyAgreementAlgorithm)

			// Derive shared secret from B's perspective (using A's public key).
			outB, err := bPeer.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
				KeyID:                 keyB.KeyMetadata.KeyID,
				KeyAgreementAlgorithm: "ECDH",
				PublicKey:             pubA.PublicKey,
			})
			require.NoError(t, err)

			// Both sides should yield the same shared secret (ECDH property).
			assert.Equal(t, outA.SharedSecret, outB.SharedSecret)
		})
	}
}

// TestGetSupportedOperations_NewOps verifies the new operations appear in GetSupportedOperations.
func TestGetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"ConnectCustomKeyStore",
		"CreateCustomKeyStore",
		"DeleteCustomKeyStore",
		"DeriveSharedSecret",
		"DescribeCustomKeyStores",
		"DisconnectCustomKeyStore",
		"GetParametersForImport",
		"GenerateDataKeyPair",
		"GenerateDataKeyPairWithoutPlaintext",
		"GenerateMac",
		"GenerateRandom",
		"ListKeyPolicies",
		"ListKeyRotations",
		"ReplicateKey",
		"RotateKeyOnDemand",
		"UpdateCustomKeyStore",
		"UpdateKeyDescription",
		"UpdatePrimaryRegion",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op, "expected %s in GetSupportedOperations", op)
	}
}

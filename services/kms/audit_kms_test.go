package kms_test

// audit_kms_test.go — Phase-B audit tests for KMS gaps:
// auto-rotation, UpdatePrimaryRegion role-swap, GetParametersForImport real RSA,
// ImportKeyMaterial RSA-OAEP end-to-end, backward compat raw import.

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestAutoRotation_JanitorFiresAfterPeriod verifies that the janitor performs
// automatic key rotation when the rotation period has elapsed.
func TestAutoRotation_JanitorFiresAfterPeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantRotated  bool
		periodDays   int32
		backdateDays int
	}{
		{
			name:         "default 365-day period elapsed",
			periodDays:   0,
			backdateDays: 366,
			wantRotated:  true,
		},
		{
			name:         "custom 90-day period elapsed",
			periodDays:   90,
			backdateDays: 91,
			wantRotated:  true,
		},
		{
			name:         "period not yet elapsed",
			periodDays:   0,
			backdateDays: 100,
			wantRotated:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			ctx := context.Background()

			out, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
			require.NoError(t, err)
			keyID := out.KeyMetadata.KeyID

			// Enable rotation.
			input := &kms.EnableKeyRotationInput{KeyID: keyID}
			if tc.periodDays > 0 {
				p := tc.periodDays
				input.RotationPeriodInDays = &p
			}
			err = b.EnableKeyRotation(ctx, input)
			require.NoError(t, err)

			// Backdate creation so the period appears elapsed.
			past := time.Now().Add(-time.Duration(tc.backdateDays) * 24 * time.Hour)
			b.SetKeyCreationDateForTest(keyID, past)

			j := kms.NewJanitor(b, time.Second)
			j.SweepOnce(ctx)

			rotations, err := b.ListKeyRotations(ctx, &kms.ListKeyRotationsInput{KeyID: keyID})
			require.NoError(t, err)

			if tc.wantRotated {
				assert.NotEmpty(t, rotations.Rotations, "expected at least one auto-rotation record")
			} else {
				assert.Empty(t, rotations.Rotations, "expected no rotation record when period not elapsed")
			}
		})
	}
}

// TestAutoRotation_DisabledKeyNotRotated verifies disabled keys are skipped.
func TestAutoRotation_DisabledKeyNotRotated(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	ctx := context.Background()

	out, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	err = b.EnableKeyRotation(ctx, &kms.EnableKeyRotationInput{KeyID: keyID})
	require.NoError(t, err)

	err = b.DisableKey(ctx, &kms.DisableKeyInput{KeyID: keyID})
	require.NoError(t, err)

	b.SetKeyCreationDateForTest(keyID, time.Now().Add(-400*24*time.Hour))

	j := kms.NewJanitor(b, time.Second)
	j.SweepOnce(ctx)

	rotations, err := b.ListKeyRotations(ctx, &kms.ListKeyRotationsInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Empty(t, rotations.Rotations, "disabled key must not be auto-rotated")
}

// TestUpdatePrimaryRegion_RoleSwap verifies the full topology update: the old
// primary becomes a replica and the old replica becomes the new primary.
func TestUpdatePrimaryRegion_RoleSwap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		replicaRegion string
		newPrimary    string
	}{
		{
			name:          "swap east-to-west",
			replicaRegion: "us-west-2",
			newPrimary:    "us-west-2",
		},
		{
			name:          "swap east-to-eu",
			replicaRegion: "eu-west-1",
			newPrimary:    "eu-west-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			ctx := context.Background()

			// Create primary multi-region key.
			pOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{MultiRegion: true})
			require.NoError(t, err)
			primaryID := pOut.KeyMetadata.KeyID

			// Replicate to target region.
			rOut, err := b.ReplicateKey(ctx, &kms.ReplicateKeyInput{
				KeyID:         primaryID,
				ReplicaRegion: tc.replicaRegion,
			})
			require.NoError(t, err)
			replicaID := rOut.ReplicaKeyMetadata.KeyID

			// UpdatePrimaryRegion takes the CURRENT primary key ID and the target region.
			err = b.UpdatePrimaryRegion(ctx, &kms.UpdatePrimaryRegionInput{
				KeyID:         primaryID,
				PrimaryRegion: tc.newPrimary,
			})
			require.NoError(t, err)

			// New primary (the promoted replica) must report type PRIMARY with old primary in replica list.
			newPrimaryDesc, err := b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: replicaID})
			require.NoError(t, err)
			assert.Equal(t, "PRIMARY", newPrimaryDesc.KeyMetadata.MultiRegionKeyType,
				"promoted key must be PRIMARY")
			require.NotNil(t, newPrimaryDesc.KeyMetadata.MultiRegionConfiguration)
			var foundOldPrimary bool
			for _, rep := range newPrimaryDesc.KeyMetadata.MultiRegionConfiguration.ReplicaKeys {
				if rep.Region == "us-east-1" {
					foundOldPrimary = true
				}
			}
			assert.True(t, foundOldPrimary,
				"new primary ReplicaKeys must include old primary region (us-east-1)")

			// Old primary must now be a replica.
			oldPrimaryDesc, err := b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: primaryID})
			require.NoError(t, err)
			assert.Equal(t, "REPLICA", oldPrimaryDesc.KeyMetadata.MultiRegionKeyType,
				"demoted key must be REPLICA")
		})
	}
}

// TestGetParametersForImport_ReturnsRealRSAPublicKey verifies that the returned
// PublicKey is a parseable DER-encoded SubjectPublicKeyInfo RSA public key.
func TestGetParametersForImport_ReturnsRealRSAPublicKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wrappingSpec string
		wantBits     int
	}{
		{
			name:         "RSA_2048",
			wrappingSpec: "RSA_2048",
			wantBits:     2048,
		},
		{
			name:         "RSA_3072",
			wrappingSpec: "RSA_3072",
			wantBits:     3072,
		},
		{
			name:         "RSA_4096",
			wrappingSpec: "RSA_4096",
			wantBits:     4096,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			ctx := context.Background()

			keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
			require.NoError(t, err)
			keyID := keyOut.KeyMetadata.KeyID

			params, err := b.GetParametersForImport(ctx, &kms.GetParametersForImportInput{
				KeyID:             keyID,
				WrappingAlgorithm: "RSAES_OAEP_SHA_256",
				WrappingKeySpec:   tc.wrappingSpec,
			})
			require.NoError(t, err)
			require.NotEmpty(t, params.PublicKey, "PublicKey must be non-empty")

			// Parse as SubjectPublicKeyInfo.
			pub, err := x509.ParsePKIXPublicKey(params.PublicKey)
			require.NoError(t, err, "PublicKey must be valid DER SubjectPublicKeyInfo")

			rsaPub, ok := pub.(*rsa.PublicKey)
			require.True(t, ok, "PublicKey must be RSA")
			assert.Equal(t, tc.wantBits, rsaPub.N.BitLen(),
				"RSA key bit size must match wrapping spec")
		})
	}
}

// TestImportKeyMaterial_RSAOAEPWrapped verifies full end-to-end: wrap AES material
// with the returned RSA public key, import, confirm key becomes Enabled and usable.
func TestImportKeyMaterial_RSAOAEPWrapped(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	ctx := context.Background()

	keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	params, err := b.GetParametersForImport(ctx, &kms.GetParametersForImportInput{
		KeyID:             keyID,
		WrappingAlgorithm: "RSAES_OAEP_SHA_256",
		WrappingKeySpec:   "RSA_2048",
	})
	require.NoError(t, err)

	// Parse RSA public key.
	pub, err := x509.ParsePKIXPublicKey(params.PublicKey)
	require.NoError(t, err)
	rsaPub := pub.(*rsa.PublicKey)

	// Generate 32-byte AES-256 key material and RSA-OAEP encrypt it.
	rawMaterial := make([]byte, 32)
	_, err = rand.Read(rawMaterial)
	require.NoError(t, err)

	wrapped, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rsaPub, rawMaterial, nil)
	require.NoError(t, err)

	// Import the RSA-OAEP-wrapped material.
	err = b.ImportKeyMaterial(ctx, &kms.ImportKeyMaterialInput{
		KeyID:           keyID,
		KeyMaterial:     wrapped,
		ExpirationModel: "KEY_MATERIAL_DOES_NOT_EXPIRE",
	})
	require.NoError(t, err)

	// Key must be Enabled.
	desc, err := b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyStateEnabled, desc.KeyMetadata.KeyState)

	// Must be able to encrypt and decrypt with the imported key.
	plain := []byte("audit-kms-rsa-wrapped-import-test")
	encOut, err := b.Encrypt(ctx, &kms.EncryptInput{KeyID: keyID, Plaintext: plain})
	require.NoError(t, err)

	decOut, err := b.Decrypt(ctx, &kms.DecryptInput{KeyID: keyID, CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, plain, decOut.Plaintext)
}

// TestImportKeyMaterial_RawBytesBackwardCompat verifies that raw 32-byte key
// material (no RSA wrapping) is still accepted after the RSA-OAEP upgrade.
func TestImportKeyMaterial_RawBytesBackwardCompat(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	ctx := context.Background()

	keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	// GetParametersForImport to satisfy any internal state (import token etc.).
	_, err = b.GetParametersForImport(ctx, &kms.GetParametersForImportInput{
		KeyID:             keyID,
		WrappingAlgorithm: "RSAES_OAEP_SHA_256",
		WrappingKeySpec:   "RSA_2048",
	})
	require.NoError(t, err)

	// Pass raw 32 bytes — backward compat path.
	rawMaterial := make([]byte, 32)
	err = b.ImportKeyMaterial(ctx, &kms.ImportKeyMaterialInput{
		KeyID:           keyID,
		KeyMaterial:     rawMaterial,
		ExpirationModel: "KEY_MATERIAL_DOES_NOT_EXPIRE",
	})
	require.NoError(t, err)

	desc, err := b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyStateEnabled, desc.KeyMetadata.KeyState,
		"raw 32-byte import must still enable the key")
}

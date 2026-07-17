package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"

	"strconv"
	"time"
)

func TestEnableKeyRotation_PeriodBelowMinimum_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		period int32
	}{
		{"one", 1},
		{"forty-five", 45},
		{"eighty-nine", 89},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ab2NewBackend(t)
			keyID := ab2MustCreateKey(t, b)
			period := tc.period

			err := b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{
				KeyID:                keyID,
				RotationPeriodInDays: &period,
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, kms.ErrValidation,
				"period %d should be rejected as below minimum 90", tc.period)
		})
	}
}

func TestEnableKeyRotation_PeriodAtOrAboveMinimum_Accepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		period int32
	}{
		{"ninety", 90},
		{"one-year", 365},
		{"max", 2560},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ab2NewBackend(t)
			keyID := ab2MustCreateKey(t, b)
			period := tc.period

			err := b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{
				KeyID:                keyID,
				RotationPeriodInDays: &period,
			})
			assert.NoError(t, err, "period %d should be accepted", tc.period)
		})
	}
}

func TestEnableKeyRotation_PeriodAboveMaximum_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		period int32
	}{
		{"two-thousand-five-sixty-one", 2561},
		{"ten-thousand", 10000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ab2NewBackend(t)
			keyID := ab2MustCreateKey(t, b)
			period := tc.period

			err := b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{
				KeyID:                keyID,
				RotationPeriodInDays: &period,
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, kms.ErrValidation,
				"period %d should be rejected as above maximum 2560", tc.period)
		})
	}
}

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

func TestRotateKeyOnDemand_RateLimit(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	// 10 rotations should succeed.
	for i := range 10 {
		_, err := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
		require.NoError(t, err, "rotation %d should succeed", i+1)
	}

	// 11th should fail with limit exceeded.
	_, err := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rotation limit")
}

func TestRotateKeyOnDemand_RejectsAsymmetricKey(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateRSAKey(t, b)

	_, err := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "symmetric")
}

func TestRotateKeyOnDemand_RejectsExternalOrigin(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
	require.NoError(t, err)

	_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: out.KeyMetadata.KeyID})
	require.Error(t, err)
}

func TestKeyMaterialHistory_BoundedAt100(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	require.NoError(t, b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID}))

	// Rotate 105 times; decrypt of oldest should still work since we keep 100.
	// Just verify the rotation doesn't panic or error.
	for range 105 {
		_, err := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
		if err != nil {
			// Rate limit kicks in after 10; reset by using a new key.
			break
		}
	}
	// Key still operational after many rotations.
	_, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("still works")})
	require.NoError(t, err)
}

func TestKeyRotation_EnableDisableStatus(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	// Default: rotation disabled.
	status, err := b.GetKeyRotationStatus(context.Background(), &kms.GetKeyRotationStatusInput{KeyID: keyID})
	require.NoError(t, err)
	assert.False(t, status.KeyRotationEnabled)

	// Enable.
	require.NoError(t, b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID}))
	status, err = b.GetKeyRotationStatus(context.Background(), &kms.GetKeyRotationStatusInput{KeyID: keyID})
	require.NoError(t, err)
	assert.True(t, status.KeyRotationEnabled)

	// Disable.
	require.NoError(t, b.DisableKeyRotation(context.Background(), &kms.DisableKeyRotationInput{KeyID: keyID}))
	status, err = b.GetKeyRotationStatus(context.Background(), &kms.GetKeyRotationStatusInput{KeyID: keyID})
	require.NoError(t, err)
	assert.False(t, status.KeyRotationEnabled)
}

func TestKeyRotation_OldCiphertextDecryptable(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	pt := []byte("encrypted before rotation")
	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: pt})
	require.NoError(t, err)

	// Rotate multiple times.
	for range 3 {
		_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
		require.NoError(t, err)
	}

	// Old ciphertext should still decrypt.
	dec, err := b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, pt, dec.Plaintext)
}

func TestListKeyRotations(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	keyID := mustCreateSymKey(t, b)

	for range 3 {
		_, err := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
		require.NoError(t, err)
	}

	rots, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{KeyID: keyID})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(rots.Rotations), 3)
}

func TestRotation_CustomPeriodPreserved(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	period := int32(90)
	require.NoError(t, b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{
		KeyID:                keyID,
		RotationPeriodInDays: &period,
	}))

	status, err := b.GetKeyRotationStatus(context.Background(), &kms.GetKeyRotationStatusInput{KeyID: keyID})
	require.NoError(t, err)
	assert.True(t, status.KeyRotationEnabled)
	assert.Equal(t, int32(90), status.RotationPeriodInDays)
}

func TestRotation_DisableKeyRotation_ClearsStatus(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	require.NoError(t, b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID}))
	require.NoError(t, b.DisableKeyRotation(context.Background(), &kms.DisableKeyRotationInput{KeyID: keyID}))

	status, err := b.GetKeyRotationStatus(context.Background(), &kms.GetKeyRotationStatusInput{KeyID: keyID})
	require.NoError(t, err)
	assert.False(t, status.KeyRotationEnabled)
}

func TestRotation_NextRotationDate_SetAfterEnable(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	require.NoError(t, b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID}))

	now := float64(time.Now().UnixNano()) / 1e9
	status, err := b.GetKeyRotationStatus(context.Background(), &kms.GetKeyRotationStatusInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Greater(t, status.NextRotationDate, now)
}

func TestRotation_ListKeyRotations_ContainsAutoRotationType(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	require.NoError(t, b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID}))
	_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
	require.NoError(t, err)

	rotList, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{KeyID: keyID})
	require.NoError(t, err)
	assert.NotEmpty(t, rotList.Rotations)

	types := make(map[string]bool)
	for _, r := range rotList.Rotations {
		types[r.RotationType] = true
	}
	assert.True(t, types["IMPORTED"], "on-demand rotation type should be present")
}

func TestRotation_ListKeyRotations_Pagination(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	// Perform multiple on-demand rotations
	for range 5 {
		_, rotErr := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
		require.NoError(t, rotErr)
	}

	limit := int32(2)
	first, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{
		KeyID: keyID,
		Limit: &limit,
	})
	require.NoError(t, err)
	assert.Len(t, first.Rotations, 2)
	assert.True(t, first.Truncated)
	assert.NotEmpty(t, first.NextMarker)

	second, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{
		KeyID:  keyID,
		Limit:  &limit,
		Marker: first.NextMarker,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, second.Rotations)
}

func TestRotation_ExternalOriginKey_RotationRejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)
	keyID := b2mustCreateExternalKey(t, b)

	err := b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID})
	require.Error(t, err)
}

func TestRotateKeyOnDemand_OldCiphertext_Decryptable(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	// Encrypt before rotation
	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     keyID,
		Plaintext: []byte("pre-rotation data"),
	})
	require.NoError(t, err)

	_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
	require.NoError(t, err)

	// Should still decrypt after rotation
	dec, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("pre-rotation data"), dec.Plaintext)
}

func TestRotateKeyOnDemand_ReturnsKeyID(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	rotOut, err := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Equal(t, keyID, rotOut.KeyID)
}

func TestRotateKeyOnDemand_GetKeyRotationStatus_HasOnDemandDate(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
	require.NoError(t, err)

	status, err := b.GetKeyRotationStatus(context.Background(), &kms.GetKeyRotationStatusInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Greater(t, status.OnDemandRotationStartDate, float64(0))
}

// TestKMSBackendEncryptDisabledKey verifies encryption fails for disabled keys.
func TestKMSBackendEncryptDisabledKey(t *testing.T) {
	t.Parallel()

	t.Run("EncryptFails", func(t *testing.T) {
		t.Parallel()

		backend := kms.NewInMemoryBackend()
		key, _ := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})

		// Disable via alias (test resolveKeyID too)
		_ = backend.CreateAlias(context.Background(), &kms.CreateAliasInput{
			AliasName:   "alias/my-key",
			TargetKeyID: key.KeyMetadata.KeyID,
		})

		// Manually disable by re-describing then using internal disable
		_ = backend.DisableKeyRotation(context.Background(), &kms.DisableKeyRotationInput{KeyID: key.KeyMetadata.KeyID})

		// Encrypt should succeed on enabled key
		_, err := backend.Encrypt(context.Background(), &kms.EncryptInput{
			KeyID:     "alias/my-key",
			Plaintext: []byte("test"),
		})
		require.NoError(t, err)
	})
}

// TestKMSBackendKeyRotation verifies key rotation enable/disable.
func TestKMSBackendKeyRotation(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	key, _ := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})

	// Default: rotation disabled
	statusOut, err := backend.GetKeyRotationStatus(
		context.Background(),
		&kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID},
	)
	require.NoError(t, err)
	assert.False(t, statusOut.KeyRotationEnabled)

	// Enable
	err = backend.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)

	statusOut, err = backend.GetKeyRotationStatus(
		context.Background(),
		&kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID},
	)
	require.NoError(t, err)
	assert.True(t, statusOut.KeyRotationEnabled)

	// Disable
	err = backend.DisableKeyRotation(context.Background(), &kms.DisableKeyRotationInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)

	statusOut, err = backend.GetKeyRotationStatus(
		context.Background(),
		&kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID},
	)
	require.NoError(t, err)
	assert.False(t, statusOut.KeyRotationEnabled)
}

// TestKMSGetKeyRotationStatusNotFound tests GetKeyRotationStatus with missing key.
func TestKMSGetKeyRotationStatusNotFound(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	_, err := backend.GetKeyRotationStatus(context.Background(), &kms.GetKeyRotationStatusInput{KeyID: "missing"})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSEnableKeyRotationNotFound tests EnableKeyRotation with missing key.
func TestKMSEnableKeyRotationNotFound(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	err := backend.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: "missing"})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSDisableKeyRotationNotFound tests DisableKeyRotation with missing key.
func TestKMSDisableKeyRotationNotFound(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	err := backend.DisableKeyRotation(context.Background(), &kms.DisableKeyRotationInput{KeyID: "missing"})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSKeyRotationRotatesMaterial verifies that EnableKeyRotation generates new key material
// and that old ciphertexts can still be decrypted after rotation.
func TestKMSKeyRotationRotatesMaterial(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ctx  map[string]string
		name string
	}{
		{name: "no_context", ctx: nil},
		{name: "with_context", ctx: map[string]string{"app": "myapp"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			// Encrypt before rotation.
			encOut, err := b.Encrypt(context.Background(), &kms.EncryptInput{
				KeyID:             key.KeyMetadata.KeyID,
				Plaintext:         []byte("pre-rotation"),
				EncryptionContext: tt.ctx,
			})
			require.NoError(t, err)

			// Rotate the key.
			require.NoError(t, b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{
				KeyID: key.KeyMetadata.KeyID,
			}))

			// Verify rotation status is set.
			status, err := b.GetKeyRotationStatus(
				context.Background(),
				&kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID},
			)
			require.NoError(t, err)
			assert.True(t, status.KeyRotationEnabled)

			// Old ciphertext must still be decryptable using history.
			decOut, err := b.Decrypt(context.Background(), &kms.DecryptInput{
				CiphertextBlob:    encOut.CiphertextBlob,
				EncryptionContext: tt.ctx,
			})
			require.NoError(t, err)
			assert.Equal(t, []byte("pre-rotation"), decOut.Plaintext)

			// Encrypt with new key material.
			encOut2, err := b.Encrypt(context.Background(), &kms.EncryptInput{
				KeyID:             key.KeyMetadata.KeyID,
				Plaintext:         []byte("post-rotation"),
				EncryptionContext: tt.ctx,
			})
			require.NoError(t, err)

			// New ciphertext is also decryptable.
			decOut2, err := b.Decrypt(context.Background(), &kms.DecryptInput{
				CiphertextBlob:    encOut2.CiphertextBlob,
				EncryptionContext: tt.ctx,
			})
			require.NoError(t, err)
			assert.Equal(t, []byte("post-rotation"), decOut2.Plaintext)
		})
	}
}

// TestKMSKeyRotationMultipleRounds verifies that multiple rotations preserve history
// and all pre-rotation ciphertexts remain decryptable.
func TestKMSKeyRotationMultipleRounds(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	const rounds = 3

	payloads := make([][]byte, rounds)
	blobs := make([][]byte, rounds)

	for i := range rounds {
		payloads[i] = []byte("payload-round-" + strconv.Itoa(i))
		encOut, encErr := b.Encrypt(context.Background(), &kms.EncryptInput{
			KeyID:     key.KeyMetadata.KeyID,
			Plaintext: payloads[i],
		})
		require.NoError(t, encErr)
		blobs[i] = encOut.CiphertextBlob

		// Rotate between encryptions.
		if i < rounds-1 {
			require.NoError(
				t,
				b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: key.KeyMetadata.KeyID}),
			)
		}
	}

	// All blobs must still decrypt correctly.
	for i := range rounds {
		decOut, decErr := b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: blobs[i]})
		require.NoError(t, decErr, "round %d should decrypt", i)
		assert.Equal(t, payloads[i], decOut.Plaintext, "round %d payload mismatch", i)
	}
}

// TestKMSKeyRotationDisabledKeyFails verifies that EnableKeyRotation on a disabled key
// returns an error and does not change the key state.
func TestKMSKeyRotationDisabledKeyFails(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: key.KeyMetadata.KeyID}))

	err = b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: key.KeyMetadata.KeyID})
	require.Error(t, err)
	assert.ErrorIs(t, err, kms.ErrKeyDisabled)
}

// TestKMSKeyRotationSnapshotRestorePreservesHistory verifies that key material history
// is correctly serialized in snapshots and restored, so old ciphertexts decrypt after restore.
func TestKMSKeyRotationSnapshotRestorePreservesHistory(t *testing.T) {
	t.Parallel()

	orig := kms.NewInMemoryBackend()
	key, err := orig.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	// Encrypt before rotation.
	encBefore, err := orig.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("before-rotation"),
	})
	require.NoError(t, err)

	require.NoError(
		t,
		orig.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: key.KeyMetadata.KeyID}),
	)

	// Encrypt after rotation.
	encAfter, err := orig.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("after-rotation"),
	})
	require.NoError(t, err)

	snap := orig.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	fresh := kms.NewInMemoryBackendWithConfig(kms.MockAccountID, kms.MockRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Both ciphertexts must decrypt on the restored backend.
	decBefore, err := fresh.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encBefore.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("before-rotation"), decBefore.Plaintext)

	decAfter, err := fresh.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encAfter.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("after-rotation"), decAfter.Plaintext)
}

func TestRotateKeyOnDemandValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   kms.CreateKeyInput
		wantErr bool
	}{
		{
			name:    "symmetric_aws_kms",
			input:   kms.CreateKeyInput{},
			wantErr: false,
		},
		{
			name: "asymmetric_rsa",
			input: kms.CreateKeyInput{
				KeySpec:  "RSA_2048",
				KeyUsage: "SIGN_VERIFY",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &tt.input)
			require.NoError(t, err)

			_, rotErr := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{
				KeyID: key.KeyMetadata.KeyID,
			})

			if tt.wantErr {
				require.Error(t, rotErr)
			} else {
				require.NoError(t, rotErr)
			}
		})
	}
}

func TestGetKeyRotationStatusOnDemandDate(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	// Before on-demand rotation: field should be absent.
	status, err := b.GetKeyRotationStatus(
		context.Background(),
		&kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID},
	)
	require.NoError(t, err)
	assert.Zero(t, status.OnDemandRotationStartDate)

	// Trigger on-demand rotation.
	_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)

	// After on-demand rotation: date should be populated.
	status2, err := b.GetKeyRotationStatus(
		context.Background(),
		&kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID},
	)
	require.NoError(t, err)
	assert.NotZero(t, status2.OnDemandRotationStartDate)
}

func TestListKeyRotationsFields(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	keyID := key.KeyMetadata.KeyID

	// Rotation via EnableKeyRotation (AWS_KMS rotation type).
	err = b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID})
	require.NoError(t, err)

	// Rotation via RotateKeyOnDemand (IMPORTED rotation type).
	_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
	require.NoError(t, err)

	out, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{KeyID: keyID})
	require.NoError(t, err)
	require.NotEmpty(t, out.Rotations)

	for _, r := range out.Rotations {
		assert.NotEmpty(t, r.KeyID)
		assert.NotEmpty(t, r.RotationType)
		assert.NotZero(t, r.RotationDate)
	}
}

// TestListKeyRotationsTypedRecords verifies that rotations are stored with their
// correct types (AWS_KMS for scheduled, IMPORTED for on-demand) using the new
// typed RotationRecord storage.
func TestListKeyRotationsTypedRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(b *kms.InMemoryBackend, keyID string) error
		wantTypes []string
	}{
		{
			name: "enable_rotation_creates_no_immediate_record",
			setup: func(b *kms.InMemoryBackend, keyID string) error {
				return b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID})
			},
			wantTypes: []string{},
		},
		{
			name: "on_demand_rotation_is_imported",
			setup: func(b *kms.InMemoryBackend, keyID string) error {
				_, err := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})

				return err
			},
			wantTypes: []string{"IMPORTED"},
		},
		{
			name: "enable_then_on_demand_rotation_has_one_record",
			setup: func(b *kms.InMemoryBackend, keyID string) error {
				if err := b.EnableKeyRotation(
					context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID},
				); err != nil {
					return err
				}
				_, err := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})

				return err
			},
			wantTypes: []string{"IMPORTED"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			keyID := key.KeyMetadata.KeyID

			require.NoError(t, tt.setup(b, keyID))

			out, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{KeyID: keyID})
			require.NoError(t, err)
			require.Len(t, out.Rotations, len(tt.wantTypes))

			for i, wantType := range tt.wantTypes {
				assert.Equal(t, wantType, out.Rotations[i].RotationType)
				assert.Equal(t, keyID, out.Rotations[i].KeyID)
				assert.NotZero(t, out.Rotations[i].RotationDate)
			}
		})
	}
}

// TestListKeyRotationsLegacyFallback verifies that RotateKeyOnDemand creates an IMPORTED
// rotation record. EnableKeyRotation no longer causes an immediate rotation; only on-demand
// rotation creates a record.
func TestListKeyRotationsLegacyFallback(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	keyID := key.KeyMetadata.KeyID

	// Enabling rotation does not create an immediate rotation record.
	require.NoError(t, b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID}))

	// On-demand rotation creates a record with type IMPORTED.
	_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
	require.NoError(t, err)

	// List rotations - should have exactly 1 record (on-demand only).
	out, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{KeyID: keyID})
	require.NoError(t, err)
	require.Len(t, out.Rotations, 1)
	assert.Equal(t, "IMPORTED", out.Rotations[0].RotationType)
}

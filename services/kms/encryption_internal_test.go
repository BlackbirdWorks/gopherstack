package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"

	"strings"
)

// TestKMSBackendCreateKey verifies key creation.
func TestKMSBackendCreateKey(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	out, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{
		Description: "test key",
		KeyUsage:    "ENCRYPT_DECRYPT",
	})

	require.NoError(t, err)
	assert.NotEmpty(t, out.KeyMetadata.KeyID)
	assert.Contains(t, out.KeyMetadata.Arn, "arn:aws:kms:")
	assert.Equal(t, "test key", out.KeyMetadata.Description)
	assert.Equal(t, kms.KeyStateEnabled, out.KeyMetadata.KeyState)
	assert.Equal(t, "ENCRYPT_DECRYPT", out.KeyMetadata.KeyUsage)
}

// TestKMSBackendEncryptDecrypt verifies round-trip encryption and decryption.
func TestKMSBackendEncryptDecrypt(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	key, _ := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})

	plaintext := []byte("hello, world")

	encOut, err := backend.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: plaintext,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, encOut.CiphertextBlob)
	assert.Equal(t, key.KeyMetadata.Arn, encOut.KeyID)

	decOut, err := backend.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)
	assert.Equal(t, key.KeyMetadata.Arn, decOut.KeyID)
}

// TestKMSBackendReEncrypt verifies re-encryption under a different key.
func TestKMSBackendReEncrypt(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	key1, _ := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
	key2, _ := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})

	encOut, _ := backend.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key1.KeyMetadata.KeyID,
		Plaintext: []byte("secret"),
	})

	reEncOut, err := backend.ReEncrypt(context.Background(), &kms.ReEncryptInput{
		CiphertextBlob:   encOut.CiphertextBlob,
		DestinationKeyID: key2.KeyMetadata.KeyID,
	})
	require.NoError(t, err)
	assert.Equal(t, key2.KeyMetadata.Arn, reEncOut.KeyID)
	assert.Equal(t, key1.KeyMetadata.Arn, reEncOut.SourceKeyID)

	// Decrypt re-encrypted blob
	decOut, err := backend.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: reEncOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("secret"), decOut.Plaintext)
}

// TestKMSReEncryptErrors tests error paths in ReEncrypt.
func TestKMSReEncryptErrors(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	// Destination key not found
	key, _ := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
	enc, _ := backend.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("test"),
	})

	_, err := backend.ReEncrypt(context.Background(), &kms.ReEncryptInput{
		CiphertextBlob:   enc.CiphertextBlob,
		DestinationKeyID: "nonexistent-dest",
	})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSDisableEnableKey verifies disabling and re-enabling a key.
func TestKMSDisableEnableKey(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	out, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	require.NoError(t, backend.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyID}))

	desc, err := backend.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyStateDisabled, desc.KeyMetadata.KeyState)

	// Encrypt should fail when key is disabled
	_, err = backend.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("test")})
	require.ErrorIs(t, err, kms.ErrKeyDisabled)

	// Re-enable
	require.NoError(t, backend.EnableKey(context.Background(), &kms.EnableKeyInput{KeyID: keyID}))

	desc, err = backend.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyStateEnabled, desc.KeyMetadata.KeyState)
}

// TestKMSHandlerSnapshotRestore verifies the handler Snapshot and Restore wrapper methods.
func TestKMSHandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create a key and encrypt something to populate state
	keyOut, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{Description: "snapshot-test"})
	require.NoError(t, err)
	encOut, err := backend.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     keyOut.KeyMetadata.KeyID,
		Plaintext: []byte("snap-data"),
	})
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// Restore into a new backend via handler wrapper
	backend2 := kms.NewInMemoryBackend()
	h2 := kms.NewHandler(backend2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Decrypt with restored handler's backend
	decOut, err := backend2.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("snap-data"), decOut.Plaintext)
}

// TestKMSBackendDecryptCiphertextTooShort verifies Decrypt fails with short ciphertext.
func TestKMSBackendDecryptCiphertextTooShort(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	_, err := b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: []byte("short")})
	require.ErrorIs(t, err, kms.ErrCiphertextTooShort)
}

// TestKMSBackendDecryptNonExistentKey verifies Decrypt returns ErrKeyNotFound when the ciphertext
// prefix references a key that does not exist.
func TestKMSBackendDecryptNonExistentKey(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	// Encrypt with a valid key to confirm the normal path works.
	encOut, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("data"),
	})
	require.NoError(t, err)

	// Fabricate a ciphertext blob whose key ID prefix references a non-existent key.
	// Decrypt should fail with ErrKeyNotFound (not a data-corruption error).
	badBlob := make([]byte, 36+28) // keyIDPrefixLen + minimum nonce/ct size
	copy(badBlob[:36], "nonexistent-key-id-000000000000000")
	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: badBlob})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)

	// Confirm successful decrypt recovers the expected plaintext.
	decOut, err := b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), decOut.Plaintext)
}

// TestKMSBackendReEncryptShortBlob verifies ReEncrypt fails with ciphertext too short.
func TestKMSBackendReEncryptShortBlob(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	destKey, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	_, err = b.ReEncrypt(context.Background(), &kms.ReEncryptInput{
		CiphertextBlob:   []byte("short"),
		DestinationKeyID: destKey.KeyMetadata.KeyID,
	})
	require.ErrorIs(t, err, kms.ErrCiphertextTooShort)
}

// TestKMSReEncryptSourceKeyValidation verifies that ReEncrypt validates source key state and usage.
func TestKMSReEncryptSourceKeyValidation(t *testing.T) {
	t.Parallel()

	t.Run("disabled_source_key", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		srcKey, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)
		dstKey, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)

		encOut, err := b.Encrypt(context.Background(), &kms.EncryptInput{
			KeyID:     srcKey.KeyMetadata.KeyID,
			Plaintext: []byte("test"),
		})
		require.NoError(t, err)

		// Disable source key
		require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: srcKey.KeyMetadata.KeyID}))

		_, err = b.ReEncrypt(context.Background(), &kms.ReEncryptInput{
			CiphertextBlob:   encOut.CiphertextBlob,
			DestinationKeyID: dstKey.KeyMetadata.KeyID,
		})
		require.ErrorIs(t, err, kms.ErrKeyDisabled)
	})
}

// TestKMSBackendReEncryptKeyMaterialUnavailable verifies that ReEncrypt returns ErrKeyMaterialUnavailable
// when the source key's material is missing after an old-format snapshot restore.
func TestKMSBackendReEncryptKeyMaterialUnavailable(t *testing.T) {
	t.Parallel()

	orig := kms.NewInMemoryBackend()
	srcKey, err := orig.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	dstKey, err := orig.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	encOut, err := orig.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     srcKey.KeyMetadata.KeyID,
		Plaintext: []byte("test"),
	})
	require.NoError(t, err)

	// Simulate old-format snapshot without key materials.
	snap := orig.Snapshot(t.Context())
	require.NotEmpty(t, snap)
	stripped := strings.ReplaceAll(string(snap), `"key_materials":`, `"_key_materials":`)

	restored := kms.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), []byte(stripped)))

	_, err = restored.ReEncrypt(context.Background(), &kms.ReEncryptInput{
		CiphertextBlob:   encOut.CiphertextBlob,
		DestinationKeyID: dstKey.KeyMetadata.KeyID,
	})
	require.ErrorIs(t, err, kms.ErrKeyMaterialUnavailable)
}

// TestKMSBackendDecryptKeyMaterialUnavailable verifies that Decrypt returns ErrKeyMaterialUnavailable
// when key material is missing after an old-format snapshot restore.
func TestKMSBackendDecryptKeyMaterialUnavailable(t *testing.T) {
	t.Parallel()

	orig := kms.NewInMemoryBackend()
	key, err := orig.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	encOut, err := orig.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("test"),
	})
	require.NoError(t, err)

	snap := orig.Snapshot(t.Context())
	require.NotEmpty(t, snap)
	stripped := strings.ReplaceAll(string(snap), `"key_materials":`, `"_key_materials":`)

	restored := kms.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), []byte(stripped)))

	_, err = restored.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.ErrorIs(t, err, kms.ErrKeyMaterialUnavailable)
}

// TestKMSEncryptionContext verifies that encryption context is incorporated into
// the AES-GCM additional authenticated data (AAD) and must match on decryption.
func TestKMSEncryptionContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		encryptCtx    map[string]string
		decryptCtx    map[string]string
		name          string
		wantDecryptOK bool
	}{
		{
			name:          "no_context_encrypt_no_context_decrypt",
			encryptCtx:    nil,
			decryptCtx:    nil,
			wantDecryptOK: true,
		},
		{
			name:          "with_context_encrypt_same_context_decrypt",
			encryptCtx:    map[string]string{"purpose": "test", "env": "ci"},
			decryptCtx:    map[string]string{"purpose": "test", "env": "ci"},
			wantDecryptOK: true,
		},
		{
			name:          "with_context_encrypt_no_context_decrypt",
			encryptCtx:    map[string]string{"purpose": "test"},
			decryptCtx:    nil,
			wantDecryptOK: false,
		},
		{
			name:          "no_context_encrypt_with_context_decrypt",
			encryptCtx:    nil,
			decryptCtx:    map[string]string{"purpose": "test"},
			wantDecryptOK: false,
		},
		{
			name:          "context_key_order_does_not_matter",
			encryptCtx:    map[string]string{"b": "2", "a": "1"},
			decryptCtx:    map[string]string{"a": "1", "b": "2"},
			wantDecryptOK: true,
		},
		{
			name:          "wrong_context_value_fails",
			encryptCtx:    map[string]string{"purpose": "test"},
			decryptCtx:    map[string]string{"purpose": "wrong"},
			wantDecryptOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			encOut, err := b.Encrypt(context.Background(), &kms.EncryptInput{
				KeyID:             key.KeyMetadata.KeyID,
				Plaintext:         []byte("secret payload"),
				EncryptionContext: tt.encryptCtx,
			})
			require.NoError(t, err)

			decOut, decErr := b.Decrypt(context.Background(), &kms.DecryptInput{
				CiphertextBlob:    encOut.CiphertextBlob,
				EncryptionContext: tt.decryptCtx,
			})

			if tt.wantDecryptOK {
				require.NoError(t, decErr)
				assert.Equal(t, []byte("secret payload"), decOut.Plaintext)
			} else {
				require.Error(t, decErr)
				assert.Nil(t, decOut)
			}
		})
	}
}

// TestKMSReEncryptEncryptionContext verifies that source and destination
// encryption contexts are correctly applied during ReEncrypt.
func TestKMSReEncryptEncryptionContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		encCtx         map[string]string
		srcCtx         map[string]string
		dstCtx         map[string]string
		wantDecryptCtx map[string]string
		name           string
		wantReEncOK    bool
		wantDecryptOK  bool
	}{
		{
			name:           "no_contexts",
			encCtx:         nil,
			srcCtx:         nil,
			dstCtx:         nil,
			wantReEncOK:    true,
			wantDecryptCtx: nil,
			wantDecryptOK:  true,
		},
		{
			name:           "matching_source_context",
			encCtx:         map[string]string{"src": "ctx"},
			srcCtx:         map[string]string{"src": "ctx"},
			dstCtx:         map[string]string{"dst": "ctx"},
			wantReEncOK:    true,
			wantDecryptCtx: map[string]string{"dst": "ctx"},
			wantDecryptOK:  true,
		},
		{
			name:        "wrong_source_context_fails",
			encCtx:      map[string]string{"src": "ctx"},
			srcCtx:      map[string]string{"src": "wrong"},
			dstCtx:      nil,
			wantReEncOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			k1, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)
			k2, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			encOut, err := b.Encrypt(context.Background(), &kms.EncryptInput{
				KeyID:             k1.KeyMetadata.KeyID,
				Plaintext:         []byte("payload"),
				EncryptionContext: tt.encCtx,
			})
			require.NoError(t, err)

			reOut, reErr := b.ReEncrypt(context.Background(), &kms.ReEncryptInput{
				CiphertextBlob:               encOut.CiphertextBlob,
				DestinationKeyID:             k2.KeyMetadata.KeyID,
				SourceEncryptionContext:      tt.srcCtx,
				DestinationEncryptionContext: tt.dstCtx,
			})

			if !tt.wantReEncOK {
				require.Error(t, reErr)

				return
			}

			require.NoError(t, reErr)

			decOut, decErr := b.Decrypt(context.Background(), &kms.DecryptInput{
				CiphertextBlob:    reOut.CiphertextBlob,
				EncryptionContext: tt.wantDecryptCtx,
			})

			if tt.wantDecryptOK {
				require.NoError(t, decErr)
				assert.Equal(t, []byte("payload"), decOut.Plaintext)
			} else {
				require.Error(t, decErr)
			}
		})
	}
}

func TestEncryptionAlgorithmField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keySpec  string
		keyUsage string
		wantAlgo string
	}{
		{
			name:     "symmetric_default",
			keySpec:  "SYMMETRIC_DEFAULT",
			wantAlgo: "SYMMETRIC_DEFAULT",
		},
		{
			name:     "rsa_2048",
			keySpec:  "RSA_2048",
			keyUsage: "ENCRYPT_DECRYPT",
			wantAlgo: "RSAES_OAEP_SHA_256",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeySpec:  tt.keySpec,
				KeyUsage: tt.keyUsage,
			})
			require.NoError(t, err)

			encOut, err := b.Encrypt(context.Background(), &kms.EncryptInput{
				KeyID:     keyOut.KeyMetadata.KeyID,
				Plaintext: []byte("hello"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantAlgo, encOut.EncryptionAlgorithm)

			decOut, err := b.Decrypt(context.Background(), &kms.DecryptInput{
				CiphertextBlob: encOut.CiphertextBlob,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantAlgo, decOut.EncryptionAlgorithm)
		})
	}
}

func TestReEncryptAlgorithmFields(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()

	src, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	dst, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	enc, err := b.Encrypt(
		context.Background(),
		&kms.EncryptInput{KeyID: src.KeyMetadata.KeyID, Plaintext: []byte("data")},
	)
	require.NoError(t, err)

	out, err := b.ReEncrypt(context.Background(), &kms.ReEncryptInput{
		CiphertextBlob:   enc.CiphertextBlob,
		DestinationKeyID: dst.KeyMetadata.KeyID,
	})
	require.NoError(t, err)
	assert.Equal(t, "SYMMETRIC_DEFAULT", out.SourceEncryptionAlgorithm)
	assert.Equal(t, "SYMMETRIC_DEFAULT", out.DestinationEncryptionAlgorithm)
}

func TestDecryptKeyIDHint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hintFn  func(correctID, otherID string) string
		name    string
		wantErr bool
	}{
		{
			name:    "matching_hint",
			hintFn:  func(correctID, _ string) string { return correctID },
			wantErr: false,
		},
		{
			name:    "wrong_hint",
			hintFn:  func(_, otherID string) string { return otherID },
			wantErr: true,
		},
		{
			name:    "no_hint",
			hintFn:  func(_, _ string) string { return "" },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			k1, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)
			k2, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
				KeyID:     k1.KeyMetadata.KeyID,
				Plaintext: []byte("secret"),
			})
			require.NoError(t, err)

			hint := tt.hintFn(k1.KeyMetadata.KeyID, k2.KeyMetadata.KeyID)

			_, decErr := b.Decrypt(context.Background(), &kms.DecryptInput{
				CiphertextBlob: enc.CiphertextBlob,
				KeyID:          hint,
			})

			if tt.wantErr {
				require.Error(t, decErr)
			} else {
				require.NoError(t, decErr)
			}
		})
	}
}

func TestRSAEncryptDecryptRoundTrip(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "RSA_2048",
		KeyUsage: "ENCRYPT_DECRYPT",
	})
	require.NoError(t, err)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("rsa secret"),
	})
	require.NoError(t, err)
	assert.Equal(t, "RSAES_OAEP_SHA_256", enc.EncryptionAlgorithm)

	dec, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("rsa secret"), dec.Plaintext)
	assert.Equal(t, "RSAES_OAEP_SHA_256", dec.EncryptionAlgorithm)
}

func TestDecryptViaARNHint(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("arn decrypt"),
	})
	require.NoError(t, err)

	// Use the ARN as the KeyID hint.
	dec, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
		KeyID:          key.KeyMetadata.Arn,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("arn decrypt"), dec.Plaintext)
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Description: "snap-test"})
	require.NoError(t, err)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("before-snap"),
	})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())

	// Create a new backend and restore into it.
	b2 := kms.NewInMemoryBackend()
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	// Decryption must succeed on the restored backend.
	dec, err := b2.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("before-snap"), dec.Plaintext)
}

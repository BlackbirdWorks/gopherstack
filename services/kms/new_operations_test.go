package kms_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

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

// TestKMSGenerateDataKeyEncryptionContext verifies that encryption context is
// passed through GenerateDataKey and must match when decrypting the data key.
func TestKMSGenerateDataKeyEncryptionContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		genCtx        map[string]string
		decryptCtx    map[string]string
		name          string
		wantDecryptOK bool
	}{
		{
			name:          "matching_context",
			genCtx:        map[string]string{"app": "myapp"},
			decryptCtx:    map[string]string{"app": "myapp"},
			wantDecryptOK: true,
		},
		{
			name:          "no_context",
			genCtx:        nil,
			decryptCtx:    nil,
			wantDecryptOK: true,
		},
		{
			name:          "mismatched_context",
			genCtx:        map[string]string{"app": "myapp"},
			decryptCtx:    nil,
			wantDecryptOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			genOut, err := b.GenerateDataKey(context.Background(), &kms.GenerateDataKeyInput{
				KeyID:             key.KeyMetadata.KeyID,
				EncryptionContext: tt.genCtx,
			})
			require.NoError(t, err)

			decOut, decErr := b.Decrypt(context.Background(), &kms.DecryptInput{
				CiphertextBlob:    genOut.CiphertextBlob,
				EncryptionContext: tt.decryptCtx,
			})

			if tt.wantDecryptOK {
				require.NoError(t, decErr)
				assert.Equal(t, genOut.Plaintext, decOut.Plaintext)
			} else {
				require.Error(t, decErr)
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
			status, err := b.GetKeyRotationStatus(context.Background(), &kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID})
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
			require.NoError(t, b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: key.KeyMetadata.KeyID}))
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

	require.NoError(t, orig.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: key.KeyMetadata.KeyID}))

	// Encrypt after rotation.
	encAfter, err := orig.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("after-rotation"),
	})
	require.NoError(t, err)

	snap := orig.Snapshot()
	require.NotEmpty(t, snap)

	fresh := kms.NewInMemoryBackendWithConfig(kms.MockAccountID, kms.MockRegion)
	require.NoError(t, fresh.Restore(snap))

	// Both ciphertexts must decrypt on the restored backend.
	decBefore, err := fresh.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encBefore.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("before-rotation"), decBefore.Plaintext)

	decAfter, err := fresh.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encAfter.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("after-rotation"), decAfter.Plaintext)
}

// TestKMSJanitorSweepExpiredKeys verifies the janitor purges keys past their deletion date,
// including key material, aliases, and grants.
func TestKMSJanitorSweepExpiredKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		pendingWindowDay int
		advanceTime      bool
		wantKeyDeleted   bool
	}{
		{
			name:             "expired_key_deleted",
			pendingWindowDay: 7,
			advanceTime:      true,
			wantKeyDeleted:   true,
		},
		{
			name:             "not_yet_expired_key_kept",
			pendingWindowDay: 30,
			advanceTime:      false,
			wantKeyDeleted:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			require.NoError(t, b.CreateAlias(context.Background(), &kms.CreateAliasInput{
				AliasName:   "alias/janitor-test",
				TargetKeyID: key.KeyMetadata.KeyID,
			}))

			_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
				KeyID:            key.KeyMetadata.KeyID,
				GranteePrincipal: "arn:aws:iam::000000000000:role/test",
				Operations:       []string{"Decrypt"},
			})
			require.NoError(t, err)

			_, err = b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
				KeyID:               key.KeyMetadata.KeyID,
				PendingWindowInDays: tt.pendingWindowDay,
			})
			require.NoError(t, err)

			if tt.advanceTime {
				// Back-date the deletion date by injecting a past timestamp.
				b.SetDeletionDateForTest(key.KeyMetadata.KeyID, time.Now().Add(-time.Hour))
			}

			janitor := kms.NewJanitor(b, 0)
			janitor.SweepOnce(t.Context())

			if tt.wantKeyDeleted {
				// Key should be gone.
				_, descErr := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: key.KeyMetadata.KeyID})
				require.Error(t, descErr)
				require.ErrorIs(t, descErr, kms.ErrKeyNotFound)

				// Alias should be gone.
				aliases, listErr := b.ListAliases(context.Background(), &kms.ListAliasesInput{})
				require.NoError(t, listErr)
				assert.Empty(t, aliases.Aliases)

				// Grants should be gone.
				grants, grantsErr := b.ListGrants(context.Background(), &kms.ListGrantsInput{KeyID: key.KeyMetadata.KeyID})
				// Key is gone so lookup fails — that's acceptable.
				if grantsErr == nil {
					assert.Empty(t, grants.Grants)
				}
			} else {
				_, descErr := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: key.KeyMetadata.KeyID})
				require.NoError(t, descErr)
			}
		})
	}
}

// TestKMSImportKeyMaterial verifies the ImportKeyMaterial and DeleteImportedKeyMaterial lifecycle.
func TestKMSImportKeyMaterial(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *kms.InMemoryBackend) (string, []byte)
		verify  func(t *testing.T, b *kms.InMemoryBackend, keyID string, mat []byte)
		name    string
		wantErr bool
	}{
		{
			name: "import_enables_key",
			setup: func(b *kms.InMemoryBackend) (string, []byte) {
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
				if err != nil {
					return "", nil
				}

				mat := make([]byte, 32)
				for i := range mat {
					mat[i] = byte(i)
				}

				return key.KeyMetadata.KeyID, mat
			},
			verify: func(t *testing.T, b *kms.InMemoryBackend, keyID string, mat []byte) {
				t.Helper()

				err := b.ImportKeyMaterial(context.Background(), &kms.ImportKeyMaterialInput{
					KeyID:       keyID,
					KeyMaterial: mat,
				})
				require.NoError(t, err)

				// Key should now be Enabled.
				desc, err := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: keyID})
				require.NoError(t, err)
				assert.Equal(t, kms.KeyStateEnabled, desc.KeyMetadata.KeyState)
				assert.Equal(t, kms.KeyOriginExternal, desc.KeyMetadata.Origin)

				// Should be able to encrypt/decrypt.
				encOut, err := b.Encrypt(context.Background(), &kms.EncryptInput{
					KeyID:     keyID,
					Plaintext: []byte("external-key-data"),
				})
				require.NoError(t, err)

				decOut, err := b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
				require.NoError(t, err)
				assert.Equal(t, []byte("external-key-data"), decOut.Plaintext)
			},
		},
		{
			name: "delete_imported_material_returns_to_pending_import",
			setup: func(b *kms.InMemoryBackend) (string, []byte) {
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
				if err != nil {
					return "", nil
				}

				mat := make([]byte, 32)
				for i := range mat {
					mat[i] = byte(i + 1)
				}

				if err = b.ImportKeyMaterial(context.Background(), &kms.ImportKeyMaterialInput{
					KeyID:       key.KeyMetadata.KeyID,
					KeyMaterial: mat,
				}); err != nil {
					return "", nil
				}

				return key.KeyMetadata.KeyID, mat
			},
			verify: func(t *testing.T, b *kms.InMemoryBackend, keyID string, _ []byte) {
				t.Helper()

				err := b.DeleteImportedKeyMaterial(context.Background(), &kms.DeleteImportedKeyMaterialInput{KeyID: keyID})
				require.NoError(t, err)

				desc, err := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: keyID})
				require.NoError(t, err)
				assert.Equal(t, kms.KeyStatePendingImport, desc.KeyMetadata.KeyState)

				// Encrypt should fail now (no key material).
				_, encErr := b.Encrypt(context.Background(), &kms.EncryptInput{
					KeyID:     keyID,
					Plaintext: []byte("should-fail"),
				})
				require.Error(t, encErr)
			},
		},
		{
			name: "import_on_aws_kms_origin_fails",
			setup: func(b *kms.InMemoryBackend) (string, []byte) {
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				if err != nil {
					return "", nil
				}

				return key.KeyMetadata.KeyID, make([]byte, 32)
			},
			verify: func(t *testing.T, b *kms.InMemoryBackend, keyID string, mat []byte) {
				t.Helper()

				err := b.ImportKeyMaterial(context.Background(), &kms.ImportKeyMaterialInput{
					KeyID:       keyID,
					KeyMaterial: mat,
				})
				require.Error(t, err)
				assert.ErrorIs(t, err, kms.ErrUnsupportedOrigin)
			},
		},
		{
			name: "delete_imported_on_aws_kms_origin_fails",
			setup: func(b *kms.InMemoryBackend) (string, []byte) {
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				if err != nil {
					return "", nil
				}

				return key.KeyMetadata.KeyID, nil
			},
			verify: func(t *testing.T, b *kms.InMemoryBackend, keyID string, _ []byte) {
				t.Helper()

				err := b.DeleteImportedKeyMaterial(context.Background(), &kms.DeleteImportedKeyMaterialInput{KeyID: keyID})
				require.Error(t, err)
				assert.ErrorIs(t, err, kms.ErrUnsupportedOrigin)
			},
		},
		{
			name: "import_wrong_size_material_fails",
			setup: func(b *kms.InMemoryBackend) (string, []byte) {
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
				if err != nil {
					return "", nil
				}

				return key.KeyMetadata.KeyID, make([]byte, 16) // too short
			},
			verify: func(t *testing.T, b *kms.InMemoryBackend, keyID string, mat []byte) {
				t.Helper()

				err := b.ImportKeyMaterial(context.Background(), &kms.ImportKeyMaterialInput{
					KeyID:       keyID,
					KeyMaterial: mat,
				})
				require.Error(t, err)
			},
		},
		{
			name: "external_key_starts_as_pending_import",
			setup: func(b *kms.InMemoryBackend) (string, []byte) {
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
				if err != nil {
					return "", nil
				}

				return key.KeyMetadata.KeyID, nil
			},
			verify: func(t *testing.T, b *kms.InMemoryBackend, keyID string, _ []byte) {
				t.Helper()

				desc, err := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: keyID})
				require.NoError(t, err)
				assert.Equal(t, kms.KeyStatePendingImport, desc.KeyMetadata.KeyState)
				assert.Equal(t, kms.KeyOriginExternal, desc.KeyMetadata.Origin)
			},
		},
		{
			name: "import_on_already_enabled_external_key_fails",
			setup: func(b *kms.InMemoryBackend) (string, []byte) {
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
				if err != nil {
					return "", nil
				}

				mat := make([]byte, 32)
				for i := range mat {
					mat[i] = byte(i + 2)
				}

				if err = b.ImportKeyMaterial(context.Background(), &kms.ImportKeyMaterialInput{
					KeyID:       key.KeyMetadata.KeyID,
					KeyMaterial: mat,
				}); err != nil {
					return "", nil
				}

				return key.KeyMetadata.KeyID, mat
			},
			verify: func(t *testing.T, b *kms.InMemoryBackend, keyID string, mat []byte) {
				t.Helper()

				// Attempting to re-import while the key is already Enabled must fail.
				err := b.ImportKeyMaterial(context.Background(), &kms.ImportKeyMaterialInput{
					KeyID:       keyID,
					KeyMaterial: mat,
				})
				require.Error(t, err)
				assert.ErrorIs(t, err, kms.ErrKeyInvalidState)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			keyID, mat := tt.setup(b)
			require.NotEmpty(t, keyID, "setup should return a key ID")
			tt.verify(t, b, keyID, mat)
		})
	}
}

// TestKMSImportKeyMaterialSnapshotRestore verifies that EXTERNAL-origin key state
// is correctly preserved through snapshot/restore.
func TestKMSImportKeyMaterialSnapshotRestore(t *testing.T) {
	t.Parallel()

	orig := kms.NewInMemoryBackend()
	key, err := orig.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyStatePendingImport, key.KeyMetadata.KeyState)

	mat := make([]byte, 32)
	for i := range mat {
		mat[i] = byte(i + 5)
	}

	require.NoError(t, orig.ImportKeyMaterial(context.Background(), &kms.ImportKeyMaterialInput{
		KeyID:       key.KeyMetadata.KeyID,
		KeyMaterial: mat,
	}))

	snap := orig.Snapshot()
	require.NotEmpty(t, snap)

	fresh := kms.NewInMemoryBackendWithConfig(kms.MockAccountID, kms.MockRegion)
	require.NoError(t, fresh.Restore(snap))

	desc, err := fresh.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyStateEnabled, desc.KeyMetadata.KeyState)
	assert.Equal(t, kms.KeyOriginExternal, desc.KeyMetadata.Origin)

	// Encrypt on original, decrypt on restored.
	encOut, err := orig.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("external"),
	})
	require.NoError(t, err)

	decOut, err := fresh.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("external"), decOut.Plaintext)
}

// TestKMSHandlerImportKeyMaterial verifies ImportKeyMaterial and DeleteImportedKeyMaterial
// via the HTTP handler.
func TestKMSHandlerImportKeyMaterial(t *testing.T) {
	t.Parallel()

	h := kms.NewHandler(kms.NewInMemoryBackend())

	// Create EXTERNAL-origin key.
	createBody, err := json.Marshal(kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(createBody))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "TrentService.CreateKey")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	assert.Equal(t, kms.KeyStatePendingImport, createOut.KeyMetadata.KeyState)

	// Import key material.
	mat := make([]byte, 32)
	for i := range mat {
		mat[i] = byte(i + 7)
	}

	importBody, err := json.Marshal(kms.ImportKeyMaterialInput{
		KeyID:       createOut.KeyMetadata.KeyID,
		KeyMaterial: mat,
	})
	require.NoError(t, err)
	importReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(importBody))
	importReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	importReq.Header.Set("X-Amz-Target", "TrentService.ImportKeyMaterial")
	importRec := httptest.NewRecorder()

	c2 := e.NewContext(importReq, importRec)
	require.NoError(t, h.Handler()(c2))
	assert.Equal(t, http.StatusOK, importRec.Code)

	// Describe should show Enabled.
	descBody, err := json.Marshal(kms.DescribeKeyInput{KeyID: createOut.KeyMetadata.KeyID})
	require.NoError(t, err)
	descReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(descBody))
	descReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	descReq.Header.Set("X-Amz-Target", "TrentService.DescribeKey")
	descRec := httptest.NewRecorder()

	c3 := e.NewContext(descReq, descRec)
	require.NoError(t, h.Handler()(c3))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut kms.DescribeKeyOutput
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, kms.KeyStateEnabled, descOut.KeyMetadata.KeyState)

	// DeleteImportedKeyMaterial.
	delBody, err := json.Marshal(kms.DeleteImportedKeyMaterialInput{KeyID: createOut.KeyMetadata.KeyID})
	require.NoError(t, err)
	delReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(delBody))
	delReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	delReq.Header.Set("X-Amz-Target", "TrentService.DeleteImportedKeyMaterial")
	delRec := httptest.NewRecorder()

	c4 := e.NewContext(delReq, delRec)
	require.NoError(t, h.Handler()(c4))
	assert.Equal(t, http.StatusOK, delRec.Code)

	// Key should be back to PendingImport.
	descBody2, err := json.Marshal(kms.DescribeKeyInput{KeyID: createOut.KeyMetadata.KeyID})
	require.NoError(t, err)
	descReq2 := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(descBody2))
	descReq2.Header.Set("Content-Type", "application/x-amz-json-1.1")
	descReq2.Header.Set("X-Amz-Target", "TrentService.DescribeKey")
	descRec2 := httptest.NewRecorder()

	c5 := e.NewContext(descReq2, descRec2)
	require.NoError(t, h.Handler()(c5))
	require.Equal(t, http.StatusOK, descRec2.Code)

	var descOut2 kms.DescribeKeyOutput
	require.NoError(t, json.Unmarshal(descRec2.Body.Bytes(), &descOut2))
	assert.Equal(t, kms.KeyStatePendingImport, descOut2.KeyMetadata.KeyState)
}

// TestKMSJanitorRunContext verifies that the janitor Run loop respects context cancellation.
func TestKMSJanitorRunContext(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	janitor := kms.NewJanitor(b, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})

	go func() {
		janitor.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "janitor did not exit after context cancellation")
	}
}

// TestKMSExternalKeyEncryptionContext verifies encryption context is correctly applied
// when using an EXTERNAL-origin key.
func TestKMSExternalKeyEncryptionContext(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
	require.NoError(t, err)

	mat := make([]byte, 32)
	for i := range mat {
		mat[i] = byte(i + 10)
	}

	require.NoError(t, b.ImportKeyMaterial(context.Background(), &kms.ImportKeyMaterialInput{
		KeyID:       key.KeyMetadata.KeyID,
		KeyMaterial: mat,
	}))

	ctx := map[string]string{"service": "myservice"}

	encOut, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:             key.KeyMetadata.KeyID,
		Plaintext:         []byte("external-ctx-test"),
		EncryptionContext: ctx,
	})
	require.NoError(t, err)

	// Decrypt with correct context.
	decOut, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob:    encOut.CiphertextBlob,
		EncryptionContext: ctx,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("external-ctx-test"), decOut.Plaintext)

	// Decrypt without context must fail.
	_, err = b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.Error(t, err)
}

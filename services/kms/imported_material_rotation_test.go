package kms_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func materialBytes(seed byte) []byte {
	return bytes.Repeat([]byte{seed}, 32)
}

func importMaterial(
	t *testing.T, c *kmssdk.Client, keyID string, mat []byte, importType kmstypes.ImportType, id string,
) (*kmssdk.ImportKeyMaterialOutput, error) {
	t.Helper()

	in := &kmssdk.ImportKeyMaterialInput{
		KeyId:                aws.String(keyID),
		EncryptedKeyMaterial: mat,
		ImportToken:          []byte("t"),
		ExpirationModel:      kmstypes.ExpirationModelTypeKeyMaterialDoesNotExpire,
		ImportType:           importType,
	}
	if id != "" {
		in.KeyMaterialId = aws.String(id)
	}

	return c.ImportKeyMaterial(t.Context(), in)
}

type rotationRow struct {
	id, state string
	imported  kmstypes.ImportState
	rotation  kmstypes.RotationType
}

func listRows(t *testing.T, c *kmssdk.Client, keyID string, include kmstypes.IncludeKeyMaterial) []rotationRow {
	t.Helper()

	out, err := c.ListKeyRotations(t.Context(), &kmssdk.ListKeyRotationsInput{
		KeyId: aws.String(keyID), IncludeKeyMaterial: include,
	})
	require.NoError(t, err)

	rows := make([]rotationRow, 0, len(out.Rotations))
	for _, r := range out.Rotations {
		rows = append(rows, rotationRow{
			id: aws.ToString(r.KeyMaterialId), state: string(r.KeyMaterialState),
			imported: r.ImportState, rotation: r.RotationType,
		})
	}

	return rows
}

func TestImportedKeyMaterialRotationLifecycle(t *testing.T) {
	t.Parallel()

	c := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	created, err := c.CreateKey(ctx, &kmssdk.CreateKeyInput{Origin: kmstypes.OriginTypeExternal})
	require.NoError(t, err)

	keyID := aws.ToString(created.KeyMetadata.KeyId)

	first, err := importMaterial(t, c, keyID, materialBytes(1), "", "")
	require.NoError(t, err)

	id1 := aws.ToString(first.KeyMaterialId)
	require.NotEmpty(t, id1)

	enc, err := c.Encrypt(ctx, &kmssdk.EncryptInput{KeyId: aws.String(keyID), Plaintext: []byte("old")})
	require.NoError(t, err)

	second, err := importMaterial(t, c, keyID, materialBytes(2), kmstypes.ImportTypeNewKeyMaterial, "")
	require.NoError(t, err)

	id2 := aws.ToString(second.KeyMaterialId)
	require.NotEqual(t, id1, id2)

	assert.Equal(t, []rotationRow{
		{id: id1, state: "CURRENT", imported: kmstypes.ImportStateImported},
		{id: id2, state: "PENDING_ROTATION", imported: kmstypes.ImportStateImported},
	}, listRows(t, c, keyID, kmstypes.IncludeKeyMaterialAllKeyMaterial))
	assert.Empty(t, listRows(t, c, keyID, kmstypes.IncludeKeyMaterialRotationsOnly))

	_, err = c.RotateKeyOnDemand(ctx, &kmssdk.RotateKeyOnDemandInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)

	assert.Equal(t, []rotationRow{
		{id: id1, state: "NON_CURRENT", imported: kmstypes.ImportStateImported},
		{id: id2, state: "CURRENT", imported: kmstypes.ImportStateImported, rotation: kmstypes.RotationTypeOnDemand},
	}, listRows(t, c, keyID, kmstypes.IncludeKeyMaterialAllKeyMaterial))
	assert.Equal(t, []rotationRow{
		{id: id2, state: "CURRENT", imported: kmstypes.ImportStateImported, rotation: kmstypes.RotationTypeOnDemand},
	}, listRows(t, c, keyID, kmstypes.IncludeKeyMaterialRotationsOnly))

	dec, err := c.Decrypt(ctx, &kmssdk.DecryptInput{CiphertextBlob: enc.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, "old", string(dec.Plaintext))

	del, err := c.DeleteImportedKeyMaterial(ctx, &kmssdk.DeleteImportedKeyMaterialInput{
		KeyId: aws.String(keyID), KeyMaterialId: aws.String(id1),
	})
	require.NoError(t, err)
	assert.Equal(t, id1, aws.ToString(del.KeyMaterialId))

	_, err = c.Decrypt(ctx, &kmssdk.DecryptInput{CiphertextBlob: enc.CiphertextBlob})
	require.Error(t, err)
	rows := listRows(t, c, keyID, kmstypes.IncludeKeyMaterialAllKeyMaterial)
	assert.Equal(t, kmstypes.ImportStatePendingImport, rows[0].imported)

	_, err = importMaterial(t, c, keyID, materialBytes(1), kmstypes.ImportTypeExistingKeyMaterial, id1)
	require.NoError(t, err)

	dec, err = c.Decrypt(ctx, &kmssdk.DecryptInput{CiphertextBlob: enc.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, "old", string(dec.Plaintext))

	del, err = c.DeleteImportedKeyMaterial(ctx, &kmssdk.DeleteImportedKeyMaterialInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Equal(t, id2, aws.ToString(del.KeyMaterialId))

	desc, err := c.DescribeKey(ctx, &kmssdk.DescribeKeyInput{KeyId: aws.String(keyID)})
	require.NoError(t, err)
	assert.Equal(t, kmstypes.KeyStatePendingImport, desc.KeyMetadata.KeyState)
	assert.Nil(t, desc.KeyMetadata.CurrentKeyMaterialId)
}

func TestImportedKeyMaterialPendingSurvivesSnapshot(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(t0(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
	require.NoError(t, err)

	keyID := key.KeyMetadata.KeyID
	require.NoError(t, b.ImportKeyMaterial(t0(), &kms.ImportKeyMaterialInput{
		KeyID: keyID, KeyMaterial: materialBytes(1),
	}))
	require.NoError(t, b.ImportKeyMaterial(t0(), &kms.ImportKeyMaterialInput{
		KeyID: keyID, KeyMaterial: materialBytes(2), ImportType: "NEW_KEY_MATERIAL",
	}))

	restored := kms.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t0(), b.Snapshot(t0())))

	_, err = restored.RotateKeyOnDemand(t0(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
	require.NoError(t, err)

	got, err := restored.ListKeyRotations(t0(), &kms.ListKeyRotationsInput{
		KeyID: keyID, IncludeKeyMaterial: "ALL_KEY_MATERIAL",
	})
	require.NoError(t, err)
	require.Len(t, got.Rotations, 2)
	assert.Equal(t, "CURRENT", got.Rotations[1].KeyMaterialState)
}

func TestImportedKeyMaterialErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run     func(t *testing.T, b *kms.InMemoryBackend, keyID string) error
		wantErr error
		name    string
	}{
		{
			name: "new_with_material_id", wantErr: kms.ErrValidation,
			run: func(_ *testing.T, b *kms.InMemoryBackend, keyID string) error {
				return b.ImportKeyMaterial(t0(), &kms.ImportKeyMaterialInput{
					KeyID: keyID, KeyMaterial: materialBytes(2), ImportType: "NEW_KEY_MATERIAL", KeyMaterialID: "abc",
				})
			},
		},
		{
			name: "bad_import_type", wantErr: kms.ErrValidation,
			run: func(_ *testing.T, b *kms.InMemoryBackend, keyID string) error {
				return b.ImportKeyMaterial(t0(), &kms.ImportKeyMaterialInput{
					KeyID: keyID, KeyMaterial: materialBytes(2), ImportType: "OTHER",
				})
			},
		},
		{
			name: "existing_unknown_material", wantErr: kms.ErrIncorrectKeyMaterial,
			run: func(_ *testing.T, b *kms.InMemoryBackend, keyID string) error {
				return b.ImportKeyMaterial(t0(), &kms.ImportKeyMaterialInput{
					KeyID: keyID, KeyMaterial: materialBytes(9), ImportType: "EXISTING_KEY_MATERIAL",
				})
			},
		},
		{
			name: "new_duplicate_material", wantErr: kms.ErrIncorrectKeyMaterial,
			run: func(_ *testing.T, b *kms.InMemoryBackend, keyID string) error {
				return b.ImportKeyMaterial(t0(), &kms.ImportKeyMaterialInput{
					KeyID: keyID, KeyMaterial: materialBytes(1), ImportType: "NEW_KEY_MATERIAL",
				})
			},
		},
		{
			name: "second_pending", wantErr: kms.ErrKeyInvalidState,
			run: func(_ *testing.T, b *kms.InMemoryBackend, keyID string) error {
				in := &kms.ImportKeyMaterialInput{
					KeyID: keyID, KeyMaterial: materialBytes(2), ImportType: "NEW_KEY_MATERIAL",
				}
				if err := b.ImportKeyMaterial(t0(), in); err != nil {
					return err
				}

				in.KeyMaterial = materialBytes(3)

				return b.ImportKeyMaterial(t0(), in)
			},
		},
		{
			name: "rotate_without_pending", wantErr: kms.ErrKeyInvalidState,
			run: func(_ *testing.T, b *kms.InMemoryBackend, keyID string) error {
				_, err := b.RotateKeyOnDemand(t0(), &kms.RotateKeyOnDemandInput{KeyID: keyID})

				return err
			},
		},
		{
			name: "delete_unknown_material_id", wantErr: kms.ErrKeyNotFound,
			run: func(_ *testing.T, b *kms.InMemoryBackend, keyID string) error {
				return b.DeleteImportedKeyMaterial(t0(), &kms.DeleteImportedKeyMaterialInput{
					KeyID: keyID, KeyMaterialID: "nope",
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(t0(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
			require.NoError(t, err)

			keyID := key.KeyMetadata.KeyID
			require.NoError(t, b.ImportKeyMaterial(t0(), &kms.ImportKeyMaterialInput{
				KeyID: keyID, KeyMaterial: materialBytes(1),
			}))

			got := tt.run(t, b, keyID)
			require.Error(t, got)
			assert.ErrorIs(t, got, tt.wantErr)
		})
	}
}

func t0() context.Context { return context.Background() }

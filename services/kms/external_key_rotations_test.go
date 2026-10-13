package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestListKeyRotationsExternalKeyMaterial(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantImport string
		wantLen    int
		imported   bool
	}{
		{name: "pending import", imported: false, wantLen: 0},
		{name: "imported", imported: true, wantLen: 1, wantImport: "IMPORTED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
			require.NoError(t, err)

			if tt.imported {
				b2importKeyMaterial(t, b, out.KeyMetadata.KeyID)
			}

			got, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{
				KeyID: out.KeyMetadata.KeyID, IncludeKeyMaterial: "ALL_KEY_MATERIAL",
			})
			require.NoError(t, err)
			require.Len(t, got.Rotations, tt.wantLen)

			if tt.wantLen > 0 {
				assert.Equal(t, tt.wantImport, got.Rotations[0].ImportState)
				assert.Equal(t, "CURRENT", got.Rotations[0].KeyMaterialState)
				assert.Equal(t, "KEY_MATERIAL_DOES_NOT_EXPIRE", got.Rotations[0].ExpirationModel)
			}
		})
	}
}

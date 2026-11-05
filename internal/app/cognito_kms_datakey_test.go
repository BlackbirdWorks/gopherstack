package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
)

func TestCognitoKMSDataKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		decrypt map[string]string
		name    string
		wantErr bool
	}{
		{name: "same context decrypts", decrypt: map[string]string{"userpool-id": "pool-1"}},
		{name: "other context rejected", decrypt: map[string]string{"userpool-id": "pool-2"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			kms := kmsbackend.NewInMemoryBackend()

			created, err := kms.CreateKey(ctx, &kmsbackend.CreateKeyInput{})
			require.NoError(t, err)

			dk, err := cognitoKMSDataKeys{kms: kms}.GenerateDataKey(
				ctx, created.KeyMetadata.KeyID, map[string]string{"userpool-id": "pool-1"},
			)
			require.NoError(t, err)
			assert.Equal(t, created.KeyMetadata.Arn, dk.KeyARN)
			assert.Len(t, dk.Plaintext, 32)

			out, err := kms.Decrypt(ctx, &kmsbackend.DecryptInput{
				CiphertextBlob: dk.Ciphertext, EncryptionContext: tt.decrypt,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, dk.Plaintext, out.Plaintext)
		})
	}
}

package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestGetKeyLastUsage(t *testing.T) {
	t.Parallel()

	newBackendWithKey := func(t *testing.T) (*kms.InMemoryBackend, string) {
		t.Helper()

		b := kms.NewInMemoryBackend()
		out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)

		return b, out.KeyMetadata.KeyID
	}

	tests := []struct { //nolint:govet // readability: name first
		name          string
		wantOperation string
		keyID         func(keyID string) string
		setup         func(t *testing.T, b *kms.InMemoryBackend, keyID string)
		wantErr       bool
		wantNoUsage   bool
	}{
		{
			name:        "key exists but no usage yet",
			setup:       func(_ *testing.T, _ *kms.InMemoryBackend, _ string) {},
			keyID:       func(id string) string { return id },
			wantNoUsage: true,
		},
		{
			name: "records Encrypt usage",
			setup: func(t *testing.T, b *kms.InMemoryBackend, keyID string) {
				t.Helper()

				_, err := b.Encrypt(context.Background(), &kms.EncryptInput{
					KeyID:     keyID,
					Plaintext: []byte("hello"),
				})
				require.NoError(t, err)
			},
			keyID:         func(id string) string { return id },
			wantOperation: "Encrypt",
		},
		{
			name: "records Decrypt usage",
			setup: func(t *testing.T, b *kms.InMemoryBackend, keyID string) {
				t.Helper()

				enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
					KeyID:     keyID,
					Plaintext: []byte("hello"),
				})
				require.NoError(t, err)

				_, err = b.Decrypt(context.Background(), &kms.DecryptInput{
					KeyID:          keyID,
					CiphertextBlob: enc.CiphertextBlob,
				})
				require.NoError(t, err)
			},
			keyID:         func(id string) string { return id },
			wantOperation: "Decrypt",
		},
		{
			name: "records GenerateDataKey usage",
			setup: func(t *testing.T, b *kms.InMemoryBackend, keyID string) {
				t.Helper()

				_, err := b.GenerateDataKey(context.Background(), &kms.GenerateDataKeyInput{
					KeyID:   keyID,
					KeySpec: "AES_256",
				})
				require.NoError(t, err)
			},
			keyID:         func(id string) string { return id },
			wantOperation: "GenerateDataKey",
		},
		{
			name: "records GenerateDataKeyWithoutPlaintext usage",
			setup: func(t *testing.T, b *kms.InMemoryBackend, keyID string) {
				t.Helper()

				_, err := b.GenerateDataKeyWithoutPlaintext(
					context.Background(),
					&kms.GenerateDataKeyWithoutPlaintextInput{
						KeyID:   keyID,
						KeySpec: "AES_256",
					},
				)
				require.NoError(t, err)
			},
			keyID:         func(id string) string { return id },
			wantOperation: "GenerateDataKeyWithoutPlaintext",
		},
		{
			name:    "returns error for unknown key",
			setup:   func(_ *testing.T, _ *kms.InMemoryBackend, _ string) {},
			keyID:   func(_ string) string { return "nonexistent-key-id" },
			wantErr: true,
		},
		{
			name:        "KeyCreationDate and TrackingStartDate are populated",
			setup:       func(_ *testing.T, _ *kms.InMemoryBackend, _ string) {},
			keyID:       func(id string) string { return id },
			wantNoUsage: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, keyID := newBackendWithKey(t)
			tc.setup(t, b, keyID)

			out, err := b.GetKeyLastUsage(context.Background(), &kms.GetKeyLastUsageInput{
				KeyID: tc.keyID(keyID),
			})

			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out)
			assert.NotEmpty(t, out.KeyID)
			assert.Greater(t, out.KeyCreationDate, float64(0))
			assert.Greater(t, out.TrackingStartDate, float64(0))

			if tc.wantNoUsage {
				assert.Nil(t, out.KeyLastUsage)

				return
			}

			require.NotNil(t, out.KeyLastUsage)
			assert.Equal(t, tc.wantOperation, out.KeyLastUsage.Operation)
			assert.Greater(t, out.KeyLastUsage.Timestamp, float64(0))
			assert.NotEmpty(t, out.KeyLastUsage.CloudTrailEventID)
			assert.NotEmpty(t, out.KeyLastUsage.KmsRequestID)
		})
	}
}

func TestGetKeyLastUsage_Via_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "dispatches GetKeyLastUsage through handler",
			wantStatus: 200,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			h := kms.NewHandler(b)

			createOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			keyID := createOut.KeyMetadata.KeyID

			rec := sendKMSOp(t, h, "GetKeyLastUsage", `{"KeyId":"`+keyID+`"}`)
			assert.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}

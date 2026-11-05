package esdk_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp/internal/esdk"
)

const frameLen = 4096

func TestEncryptDecryptRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		encCtx    map[string]string
		name      string
		plaintext []byte
	}{
		{name: "code", plaintext: []byte("123456"), encCtx: map[string]string{"userpool-id": "us-east-1_abc"}},
		{name: "empty plaintext", plaintext: nil, encCtx: map[string]string{"b": "2", "a": "1"}},
		{name: "no encryption context", plaintext: []byte("x"), encCtx: nil},
		{name: "exactly one frame", plaintext: bytes.Repeat([]byte{7}, frameLen)},
		{name: "multi frame", plaintext: bytes.Repeat([]byte{9}, frameLen*2+17)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dataKey := bytes.Repeat([]byte{0x42}, 32)
			wrapped := []byte("kms-ciphertext")
			dk := &esdk.DataKey{
				KeyARN:     "arn:aws:kms:us-east-1:000000000000:key/k1",
				Plaintext:  dataKey,
				Ciphertext: wrapped,
			}

			msg, err := esdk.Encrypt(tt.plaintext, tt.encCtx, dk)
			require.NoError(t, err)

			got, err := esdk.Decrypt(msg, func(edk []byte, ctx map[string]string) ([]byte, error) {
				assert.Equal(t, wrapped, edk)
				assert.Equal(t, len(tt.encCtx) == 0, len(ctx) == 0)

				return dataKey, nil
			})
			require.NoError(t, err)
			assert.Equal(t, "aws-kms", got.ProviderID)
			assert.Equal(t, dk.KeyARN, got.ProviderInfo)
			assert.Len(t, got.Plaintext, len(tt.plaintext))
			assert.True(t, bytes.Equal(tt.plaintext, got.Plaintext))

			if len(tt.encCtx) > 0 {
				assert.Equal(t, tt.encCtx, got.EncryptionContext)
			}

			msg[len(msg)-1] ^= 1
			_, err = esdk.Decrypt(msg, func([]byte, map[string]string) ([]byte, error) { return dataKey, nil })
			require.Error(t, err)
		})
	}
}

func TestEncryptRejectsShortDataKey(t *testing.T) {
	t.Parallel()

	_, err := esdk.Encrypt([]byte("1"), nil, &esdk.DataKey{Plaintext: []byte("short")})
	require.ErrorIs(t, err, esdk.ErrInvalidDataKey)
}

package kms_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestDecryptIncorrectKeyHint verifies that supplying a KeyId on Decrypt that does
// not identify the key that encrypted the ciphertext is rejected with
// IncorrectKeyException, matching real AWS KMS behavior. A matching hint (or no
// hint at all) must still succeed.
func TestDecryptIncorrectKeyHint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		useWrongKey bool
		omitHint    bool
	}{
		{name: "matching_hint_succeeds", useWrongKey: false, omitHint: false, wantErr: nil},
		{name: "no_hint_succeeds", useWrongKey: false, omitHint: true, wantErr: nil},
		{
			name:        "wrong_hint_returns_incorrect_key",
			useWrongKey: true,
			omitHint:    false,
			wantErr:     kms.ErrIncorrectKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := kms.NewInMemoryBackend()

			encKey, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
			require.NoError(t, err)
			otherKey, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
			require.NoError(t, err)

			encOut, err := b.Encrypt(ctx, &kms.EncryptInput{
				KeyID:     encKey.KeyMetadata.KeyID,
				Plaintext: []byte("top-secret"),
			})
			require.NoError(t, err)

			in := &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob}
			switch {
			case tt.omitHint:
				// leave KeyID empty
			case tt.useWrongKey:
				in.KeyID = otherKey.KeyMetadata.KeyID
			default:
				in.KeyID = encKey.KeyMetadata.KeyID
			}

			out, err := b.Decrypt(ctx, in)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, out)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, []byte("top-secret"), out.Plaintext)
		})
	}
}

// TestReEncryptIncorrectSourceKeyHint verifies that supplying a SourceKeyId on
// ReEncrypt that does not identify the key that encrypted the source ciphertext
// is rejected with IncorrectKeyException. A matching SourceKeyId (or none) succeeds.
func TestReEncryptIncorrectSourceKeyHint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		useWrongKey bool
		omitHint    bool
	}{
		{name: "matching_source_hint_succeeds", useWrongKey: false, omitHint: false, wantErr: nil},
		{name: "no_source_hint_succeeds", useWrongKey: false, omitHint: true, wantErr: nil},
		{
			name:        "wrong_source_hint_returns_incorrect_key",
			useWrongKey: true,
			omitHint:    false,
			wantErr:     kms.ErrIncorrectKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := kms.NewInMemoryBackend()

			srcKey, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
			require.NoError(t, err)
			destKey, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
			require.NoError(t, err)
			otherKey, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
			require.NoError(t, err)

			encOut, err := b.Encrypt(ctx, &kms.EncryptInput{
				KeyID:     srcKey.KeyMetadata.KeyID,
				Plaintext: []byte("payload"),
			})
			require.NoError(t, err)

			in := &kms.ReEncryptInput{
				CiphertextBlob:   encOut.CiphertextBlob,
				DestinationKeyID: destKey.KeyMetadata.KeyID,
			}
			switch {
			case tt.omitHint:
				// leave SourceKeyID empty
			case tt.useWrongKey:
				in.SourceKeyID = otherKey.KeyMetadata.KeyID
			default:
				in.SourceKeyID = srcKey.KeyMetadata.KeyID
			}

			out, err := b.ReEncrypt(ctx, in)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, out)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, destKey.KeyMetadata.Arn, out.KeyID)

			// The re-encrypted blob must still decrypt to the original plaintext.
			decOut, decErr := b.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: out.CiphertextBlob})
			require.NoError(t, decErr)
			assert.Equal(t, []byte("payload"), decOut.Plaintext)
		})
	}
}

// TestIncorrectKeyExceptionWireType verifies that IncorrectKeyException surfaces on
// the wire with the AWS-accurate __type of "IncorrectKeyException" and HTTP 400.
func TestIncorrectKeyExceptionWireType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	e := echo.New()
	b := kms.NewInMemoryBackend()

	encKey, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)
	otherKey, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)

	encOut, err := b.Encrypt(ctx, &kms.EncryptInput{
		KeyID:     encKey.KeyMetadata.KeyID,
		Plaintext: []byte("secret"),
	})
	require.NoError(t, err)

	h := kms.NewHandler(b)

	body, err := json.Marshal(map[string]any{
		"CiphertextBlob": encOut.CiphertextBlob,
		"KeyId":          otherKey.KeyMetadata.KeyID,
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(body)))
	req.Header.Set("X-Amz-Target", "TrentService.Decrypt")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "IncorrectKeyException", resp.Type)
}

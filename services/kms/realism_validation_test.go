package kms_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func kmsErr(t *testing.T, body []byte) (string, string) {
	t.Helper()

	var e struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}

	require.NoError(t, json.Unmarshal(body, &e))

	return e.Type, e.Message
}

func TestKMSWire_ErrorRealism(t *testing.T) {
	t.Parallel()

	const missing = "11111111-1111-1111-1111-111111111111"

	tests := []struct {
		name     string
		action   string
		body     func(keyID string) string
		wantType string
		wantMsg  string
	}{
		{
			name: "encrypt empty plaintext", action: "Encrypt",
			body:     func(k string) string { return fmt.Sprintf(`{"KeyId":%q,"Plaintext":""}`, k) },
			wantType: "ValidationException", wantMsg: "Plaintext",
		},
		{
			name: "encrypt missing key", action: "Encrypt",
			body: func(string) string {
				return fmt.Sprintf(
					`{"KeyId":%q,"Plaintext":%q}`,
					missing,
					base64.StdEncoding.EncodeToString([]byte("x")),
				)
			},
			wantType: "NotFoundException", wantMsg: "does not exist",
		},
		{
			name: "decrypt garbage blob", action: "Decrypt",
			body: func(string) string {
				blob := make([]byte, 80)
				copy(blob, "not-a-kms-ciphertext")

				return fmt.Sprintf(`{"CiphertextBlob":%q}`, base64.StdEncoding.EncodeToString(blob))
			},
			wantType: "InvalidCiphertextException",
		},
		{
			name: "list keys bad marker", action: "ListKeys",
			body:     func(string) string { return `{"Marker":"garbage"}` },
			wantType: "InvalidMarkerException",
		},
		{
			name: "alias missing", action: "DeleteAlias",
			body:     func(string) string { return `{"AliasName":"alias/zzz"}` },
			wantType: "NotFoundException", wantMsg: "alias/zzz",
		},
		{
			name: "sign wrong algorithm", action: "Sign",
			body: func(k string) string {
				return fmt.Sprintf(`{"KeyId":%q,"Message":"aGk=","SigningAlgorithm":"ECDSA_SHA_256"}`, k)
			},
			wantType: "InvalidKeyUsageException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ab2NewHandler(t)

			createBody := `{}`
			if tt.action == "Sign" {
				createBody = `{"KeyUsage":"SIGN_VERIFY","KeySpec":"RSA_2048"}`
			}

			var created struct {
				KeyMetadata struct {
					KeyID string `json:"KeyId"`
				} `json:"KeyMetadata"`
			}

			require.NoError(t, json.Unmarshal(doKMSRequest(t, h, "CreateKey", createBody).Body.Bytes(), &created))
			keyID := created.KeyMetadata.KeyID

			rec := doKMSRequest(t, h, tt.action, tt.body(keyID))
			require.GreaterOrEqual(t, rec.Code, http.StatusBadRequest, rec.Body.String())
			require.Less(t, rec.Code, http.StatusInternalServerError, rec.Body.String())

			typ, msg := kmsErr(t, rec.Body.Bytes())
			assert.Equal(t, tt.wantType, typ)
			assert.Contains(t, msg, tt.wantMsg)
			assert.NotEqual(t, typ, msg)
		})
	}
}

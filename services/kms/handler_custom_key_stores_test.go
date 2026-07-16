package kms_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestHandler_XKS_CreateDescribeViaHTTP(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)

	// Create XKS store
	rec := b2postKMSOp(t, h, "CreateCustomKeyStore",
		`{"CustomKeyStoreName":"xks-http","CustomKeyStoreType":"EXTERNAL_KEY_STORE"}`)
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		CustomKeyStoreID string `json:"CustomKeyStoreId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.NotEmpty(t, createResp.CustomKeyStoreID)

	// Describe the XKS store
	descBody := fmt.Sprintf(`{"CustomKeyStoreId":"%s"}`, createResp.CustomKeyStoreID)
	rec2 := b2postKMSOp(t, h, "DescribeCustomKeyStores", descBody)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var descResp struct {
		CustomKeyStores []struct {
			CustomKeyStoreType string `json:"CustomKeyStoreType"`
			ConnectionState    string `json:"ConnectionState"`
		} `json:"CustomKeyStores"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &descResp))
	require.Len(t, descResp.CustomKeyStores, 1)
	assert.Equal(t, "EXTERNAL_KEY_STORE", descResp.CustomKeyStores[0].CustomKeyStoreType)
	assert.Equal(t, kms.ConnectionStateDisconnected, descResp.CustomKeyStores[0].ConnectionState)
}

func TestHandler_DeriveSharedSecret_ViaHTTP(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)

	key1ID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)
	key2ID := b2mustCreateECKey(t, b, "ECC_NIST_P256", kms.KeyUsageKeyAgreement)

	pub2, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: key2ID})
	require.NoError(t, err)

	// Encode public key as base64 for JSON
	pubKeyB64 := encodeBase64(pub2.PublicKey)
	body := fmt.Sprintf(`{"KeyId":"%s","KeyAgreementAlgorithm":"ECDH","PublicKey":"%s"}`, key1ID, pubKeyB64)
	rec := b2postKMSOp(t, h, "DeriveSharedSecret", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestErrCustomKeyStoreNotFound_Mapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
		body string
	}{
		{
			name: "ConnectCustomKeyStore_not_found",
			op:   "ConnectCustomKeyStore",
			body: `{"CustomKeyStoreId":"nonexistent"}`,
		},
		{
			name: "DisconnectCustomKeyStore_not_found",
			op:   "DisconnectCustomKeyStore",
			body: `{"CustomKeyStoreId":"nonexistent"}`,
		},
		{
			name: "DeleteCustomKeyStore_not_found",
			op:   "DeleteCustomKeyStore",
			body: `{"CustomKeyStoreId":"nonexistent"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := kms.NewHandler(b)
			rec := sendKMSOp(t, h, tt.op, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "CustomKeyStoreNotFoundException")
		})
	}
}

// TestNewOpsHandler verifies the HTTP handler dispatches all 10 new operations correctly.
func TestNewOpsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn        func(*testing.T, *kms.InMemoryBackend) string
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		operation      string
		name           string
		body           string
		expectedStatus int
	}{
		{
			name:           "CreateCustomKeyStore",
			operation:      "CreateCustomKeyStore",
			body:           `{"CustomKeyStoreName":"test-store"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.CreateCustomKeyStoreOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.CustomKeyStoreID)
			},
		},
		{
			name:      "DescribeCustomKeyStores_empty",
			operation: "DescribeCustomKeyStores",
			body:      `{}`,
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				_, _ = b.CreateCustomKeyStore(
					context.Background(),
					&kms.CreateCustomKeyStoreInput{CustomKeyStoreName: "s1"},
				)

				return ""
			},
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.DescribeCustomKeyStoresOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.CustomKeyStores, 1)
			},
		},
		{
			name:           "DeleteCustomKeyStore_not_found",
			operation:      "DeleteCustomKeyStore",
			body:           `{"CustomKeyStoreId":"nonexistent"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:      "ConnectCustomKeyStore",
			operation: "ConnectCustomKeyStore",
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
					CustomKeyStoreName: "c-store",
				})
				require.NoError(t, err)

				return out.CustomKeyStoreID
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:      "DisconnectCustomKeyStore",
			operation: "DisconnectCustomKeyStore",
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
					CustomKeyStoreName: "d-store",
				})
				require.NoError(t, err)
				_ = b.ConnectCustomKeyStore(context.Background(), &kms.ConnectCustomKeyStoreInput{
					CustomKeyStoreID: out.CustomKeyStoreID,
				})

				return out.CustomKeyStoreID
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "GenerateRandom",
			operation:      "GenerateRandom",
			body:           `{"NumberOfBytes":32}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.GenerateRandomOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.Plaintext, 32)
			},
		},
		{
			name:           "GenerateRandom_over_limit",
			operation:      "GenerateRandom",
			body:           `{"NumberOfBytes":2048}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "GenerateMac_success",
			operation:      "GenerateMac",
			expectedStatus: http.StatusOK,
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeySpec: "HMAC_256"})
				require.NoError(t, err)

				return key.KeyMetadata.KeyID
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.GenerateMacOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.Mac)
			},
		},
		{
			name:           "GenerateDataKeyPair_success",
			operation:      "GenerateDataKeyPair",
			expectedStatus: http.StatusOK,
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				return key.KeyMetadata.KeyID
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.GenerateDataKeyPairOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.PublicKey)
				assert.NotEmpty(t, out.PrivateKeyPlaintext)
			},
		},
		{
			name:           "GenerateDataKeyPairWithoutPlaintext_success",
			operation:      "GenerateDataKeyPairWithoutPlaintext",
			expectedStatus: http.StatusOK,
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				return key.KeyMetadata.KeyID
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.GenerateDataKeyPairWithoutPlaintextOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.PublicKey)
				assert.NotEmpty(t, out.PrivateKeyCiphertextBlob)
			},
		},
		{
			name:           "DeriveSharedSecret_success",
			operation:      "DeriveSharedSecret",
			expectedStatus: http.StatusOK,
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
					KeySpec:  "ECC_NIST_P256",
					KeyUsage: kms.KeyUsageKeyAgreement,
				})
				require.NoError(t, err)

				return key.KeyMetadata.KeyID
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.DeriveSharedSecretOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.SharedSecret)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := kms.NewInMemoryBackend()
			h := kms.NewHandler(backend)

			var resourceID string
			if tt.setupFn != nil {
				resourceID = tt.setupFn(t, backend)
			}

			body := tt.body
			if body == "" {
				// Build body from resourceID if available.
				body = buildBodyFromResourceID(tt.operation, resourceID)
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "TrentService."+tt.operation)
			c := e.NewContext(req, rec)

			_ = h.Handler()(c)

			assert.Equal(t, tt.expectedStatus, rec.Code, "body: %s", rec.Body.String())
			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// TestNewOpsHandlerReset verifies that custom key stores are cleared after Reset.
func TestNewOpsHandlerReset(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)

	_, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
		CustomKeyStoreName: "to-be-reset",
	})
	require.NoError(t, err)

	h.Reset()

	e := echo.New()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "TrentService.DescribeCustomKeyStores")
	c := e.NewContext(req, rec)

	_ = h.Handler()(c)

	require.Equal(t, http.StatusOK, rec.Code)

	var out kms.DescribeCustomKeyStoresOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.CustomKeyStores)
}

// TestCustomKeyStoreHandler_ViaHTTP verifies the full HTTP round-trip for all custom key store ops.
func TestCustomKeyStoreHandler_ViaHTTP(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)

	// Create via HTTP.
	rec := postKMSOp(t, h, "CreateCustomKeyStore", `{"CustomKeyStoreName":"http-store"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut kms.CreateCustomKeyStoreOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	storeID := createOut.CustomKeyStoreID

	// Connect via HTTP.
	connectBody, _ := json.Marshal(map[string]string{"CustomKeyStoreId": storeID})
	rec = postKMSOp(t, h, "ConnectCustomKeyStore", string(connectBody))
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe via HTTP – should be CONNECTED.
	descBody, _ := json.Marshal(map[string]string{"CustomKeyStoreId": storeID})
	rec = postKMSOp(t, h, "DescribeCustomKeyStores", string(descBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut kms.DescribeCustomKeyStoresOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))
	require.Len(t, descOut.CustomKeyStores, 1)
	assert.Equal(t, kms.ConnectionStateConnected, descOut.CustomKeyStores[0].ConnectionState)

	// Disconnect via HTTP.
	rec = postKMSOp(t, h, "DisconnectCustomKeyStore", string(connectBody))
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete via HTTP.
	deleteBody, _ := json.Marshal(map[string]string{"CustomKeyStoreId": storeID})
	rec = postKMSOp(t, h, "DeleteCustomKeyStore", string(deleteBody))
	require.Equal(t, http.StatusOK, rec.Code)
}

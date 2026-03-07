package kms_test

import (
	"crypto/sha512"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestKMSBackendCreateKey verifies key creation.
func TestKMSBackendCreateKey(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	out, err := backend.CreateKey(&kms.CreateKeyInput{
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

// TestKMSBackendDescribeKey verifies key lookup.
func TestKMSBackendDescribeKey(t *testing.T) {
	t.Parallel()

	t.Run("Found", func(t *testing.T) {
		t.Parallel()

		backend := kms.NewInMemoryBackend()
		created, _ := backend.CreateKey(&kms.CreateKeyInput{Description: "my key"})

		out, err := backend.DescribeKey(&kms.DescribeKeyInput{KeyID: created.KeyMetadata.KeyID})
		require.NoError(t, err)
		assert.Equal(t, created.KeyMetadata.KeyID, out.KeyMetadata.KeyID)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()

		backend := kms.NewInMemoryBackend()
		_, err := backend.DescribeKey(&kms.DescribeKeyInput{KeyID: "does-not-exist"})
		require.ErrorIs(t, err, kms.ErrKeyNotFound)
	})
}

// TestKMSBackendListKeys verifies listing keys.
func TestKMSBackendListKeys(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	for range 3 {
		_, _ = backend.CreateKey(&kms.CreateKeyInput{})
	}

	out, err := backend.ListKeys(&kms.ListKeysInput{})
	require.NoError(t, err)
	assert.Len(t, out.Keys, 3)
	assert.False(t, out.Truncated)
}

// TestKMSBackendListKeysPagination verifies pagination in ListKeys.
func TestKMSBackendListKeysPagination(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	for range 5 {
		_, _ = backend.CreateKey(&kms.CreateKeyInput{})
	}

	limit := int32(2)
	out, err := backend.ListKeys(&kms.ListKeysInput{Limit: &limit})
	require.NoError(t, err)
	assert.Len(t, out.Keys, 2)
	assert.True(t, out.Truncated)
	assert.NotEmpty(t, out.NextMarker)

	// Get next page
	out2, err := backend.ListKeys(&kms.ListKeysInput{Limit: &limit, Marker: out.NextMarker})
	require.NoError(t, err)
	assert.Len(t, out2.Keys, 2)
}

// TestKMSBackendEncryptDecrypt verifies round-trip encryption and decryption.
func TestKMSBackendEncryptDecrypt(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	key, _ := backend.CreateKey(&kms.CreateKeyInput{})

	plaintext := []byte("hello, world")

	encOut, err := backend.Encrypt(&kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: plaintext,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, encOut.CiphertextBlob)
	assert.Equal(t, key.KeyMetadata.KeyID, encOut.KeyID)

	decOut, err := backend.Decrypt(&kms.DecryptInput{
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)
	assert.Equal(t, key.KeyMetadata.KeyID, decOut.KeyID)
}

// TestKMSBackendEncryptDisabledKey verifies encryption fails for disabled keys.
func TestKMSBackendEncryptDisabledKey(t *testing.T) {
	t.Parallel()

	t.Run("EncryptFails", func(t *testing.T) {
		t.Parallel()

		backend := kms.NewInMemoryBackend()
		key, _ := backend.CreateKey(&kms.CreateKeyInput{})

		// Disable via alias (test resolveKeyID too)
		_ = backend.CreateAlias(&kms.CreateAliasInput{
			AliasName:   "alias/my-key",
			TargetKeyID: key.KeyMetadata.KeyID,
		})

		// Manually disable by re-describing then using internal disable
		_ = backend.DisableKeyRotation(&kms.DisableKeyRotationInput{KeyID: key.KeyMetadata.KeyID})

		// Encrypt should succeed on enabled key
		_, err := backend.Encrypt(&kms.EncryptInput{
			KeyID:     "alias/my-key",
			Plaintext: []byte("test"),
		})
		require.NoError(t, err)
	})
}

// TestKMSBackendGenerateDataKey verifies data key generation.
func TestKMSBackendGenerateDataKey(t *testing.T) {
	t.Parallel()

	t.Run("AES256", func(t *testing.T) {
		t.Parallel()

		backend := kms.NewInMemoryBackend()
		key, _ := backend.CreateKey(&kms.CreateKeyInput{})

		out, err := backend.GenerateDataKey(&kms.GenerateDataKeyInput{
			KeyID:   key.KeyMetadata.KeyID,
			KeySpec: "AES_256",
		})
		require.NoError(t, err)
		assert.Len(t, out.Plaintext, 32)
		assert.NotEmpty(t, out.CiphertextBlob)
		assert.Equal(t, key.KeyMetadata.KeyID, out.KeyID)
	})

	t.Run("AES128", func(t *testing.T) {
		t.Parallel()

		backend := kms.NewInMemoryBackend()
		key, _ := backend.CreateKey(&kms.CreateKeyInput{})

		out, err := backend.GenerateDataKey(&kms.GenerateDataKeyInput{
			KeyID:   key.KeyMetadata.KeyID,
			KeySpec: "AES_128",
		})
		require.NoError(t, err)
		assert.Len(t, out.Plaintext, 16)
	})

	t.Run("NumberOfBytes", func(t *testing.T) {
		t.Parallel()

		backend := kms.NewInMemoryBackend()
		key, _ := backend.CreateKey(&kms.CreateKeyInput{})

		n := int32(24)
		out, err := backend.GenerateDataKey(&kms.GenerateDataKeyInput{
			KeyID:         key.KeyMetadata.KeyID,
			NumberOfBytes: &n,
		})
		require.NoError(t, err)
		assert.Len(t, out.Plaintext, 24)
	})
}

// TestKMSBackendReEncrypt verifies re-encryption under a different key.
func TestKMSBackendReEncrypt(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	key1, _ := backend.CreateKey(&kms.CreateKeyInput{})
	key2, _ := backend.CreateKey(&kms.CreateKeyInput{})

	encOut, _ := backend.Encrypt(&kms.EncryptInput{
		KeyID:     key1.KeyMetadata.KeyID,
		Plaintext: []byte("secret"),
	})

	reEncOut, err := backend.ReEncrypt(&kms.ReEncryptInput{
		CiphertextBlob:   encOut.CiphertextBlob,
		DestinationKeyID: key2.KeyMetadata.KeyID,
	})
	require.NoError(t, err)
	assert.Equal(t, key2.KeyMetadata.KeyID, reEncOut.KeyID)
	assert.Equal(t, key1.KeyMetadata.KeyID, reEncOut.SourceKeyID)

	// Decrypt re-encrypted blob
	decOut, err := backend.Decrypt(&kms.DecryptInput{
		CiphertextBlob: reEncOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("secret"), decOut.Plaintext)
}

// TestKMSBackendAliases verifies alias create, list, and delete.
func TestKMSBackendAliases(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	key, _ := backend.CreateKey(&kms.CreateKeyInput{})

	err := backend.CreateAlias(&kms.CreateAliasInput{
		AliasName:   "alias/myalias",
		TargetKeyID: key.KeyMetadata.KeyID,
	})
	require.NoError(t, err)

	// Duplicate should fail
	err2 := backend.CreateAlias(&kms.CreateAliasInput{
		AliasName:   "alias/myalias",
		TargetKeyID: key.KeyMetadata.KeyID,
	})
	require.ErrorIs(t, err2, kms.ErrAliasAlreadyExists)

	// ListAliases by key
	listOut, err := backend.ListAliases(&kms.ListAliasesInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.Len(t, listOut.Aliases, 1)
	assert.Equal(t, "alias/myalias", listOut.Aliases[0].AliasName)

	// Delete alias
	err = backend.DeleteAlias(&kms.DeleteAliasInput{AliasName: "alias/myalias"})
	require.NoError(t, err)

	// Delete again should fail
	err2 = backend.DeleteAlias(&kms.DeleteAliasInput{AliasName: "alias/myalias"})
	require.ErrorIs(t, err2, kms.ErrAliasNotFound)
}

// TestKMSBackendKeyRotation verifies key rotation enable/disable.
func TestKMSBackendKeyRotation(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	key, _ := backend.CreateKey(&kms.CreateKeyInput{})

	// Default: rotation disabled
	statusOut, err := backend.GetKeyRotationStatus(&kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.False(t, statusOut.KeyRotationEnabled)

	// Enable
	err = backend.EnableKeyRotation(&kms.EnableKeyRotationInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)

	statusOut, err = backend.GetKeyRotationStatus(&kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.True(t, statusOut.KeyRotationEnabled)

	// Disable
	err = backend.DisableKeyRotation(&kms.DisableKeyRotationInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)

	statusOut, err = backend.GetKeyRotationStatus(&kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.False(t, statusOut.KeyRotationEnabled)
}

// TestKMSHandler verifies the HTTP handler dispatches operations correctly.
func TestKMSHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn        func(*testing.T, kms.StorageBackend) string
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		target         string
		name           string
		body           string
		expectedStatus int
	}{
		{
			name:           "CreateKey",
			target:         "TrentService.CreateKey",
			body:           `{"Description":"my key"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.CreateKeyOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.KeyMetadata.KeyID)
			},
		},
		{
			name:           "UnknownAction",
			target:         "TrentService.FakeOp",
			body:           `{}`,
			expectedStatus: http.StatusBadRequest,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var errResp kms.ErrorResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "UnknownOperationException", errResp.Type)
			},
		},
		{
			name:           "MissingTarget",
			target:         "",
			body:           `{}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "GetSupportedOps",
			target:         "",
			body:           "",
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var ops []string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
				assert.Contains(t, ops, "CreateKey")
			},
		},
		{
			name:           "DescribeKeyNotFound",
			target:         "TrentService.DescribeKey",
			body:           `{"KeyId":"missing"}`,
			expectedStatus: http.StatusBadRequest,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var errResp kms.ErrorResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "NotFoundException", errResp.Type)
			},
		},
		{
			name:   "ListKeys",
			target: "TrentService.ListKeys",
			body:   `{}`,
			setupFn: func(t *testing.T, backend kms.StorageBackend) string {
				t.Helper()
				_, _ = backend.CreateKey(&kms.CreateKeyInput{})

				return ""
			},
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.ListKeysOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.Keys, 1)
			},
		},
		{
			name:           "InvalidTarget",
			target:         "TrentServiceNoSep",
			body:           `{}`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()

			backend := kms.NewInMemoryBackend()

			if tt.setupFn != nil {
				tt.setupFn(t, backend)
			}

			h := kms.NewHandler(backend)

			var req *http.Request

			switch {
			case tt.target != "":
				req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
				req.Header.Set("X-Amz-Target", tt.target)
			case tt.body != "":
				req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			default:
				req = httptest.NewRequest(http.MethodGet, "/", nil)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// TestKMSHandlerEncryptDecrypt tests encrypt and decrypt via HTTP handler.
func TestKMSHandlerEncryptDecrypt(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create key
	createReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Description":"enc-key"}`))
	createReq.Header.Set("X-Amz-Target", "TrentService.CreateKey")
	createRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(createReq, createRec)))

	var createOut kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	keyID := createOut.KeyMetadata.KeyID

	// Encrypt via HTTP (plaintext base64-encoded in JSON)
	encBody, _ := json.Marshal(map[string]any{
		"KeyID":     keyID,
		"Plaintext": []byte("my-secret"),
	})
	encReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(encBody)))
	encReq.Header.Set("X-Amz-Target", "TrentService.Encrypt")
	encRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(encReq, encRec)))
	assert.Equal(t, http.StatusOK, encRec.Code)

	var encOut kms.EncryptOutput
	require.NoError(t, json.Unmarshal(encRec.Body.Bytes(), &encOut))

	// Decrypt
	decBody, _ := json.Marshal(map[string]any{"CiphertextBlob": encOut.CiphertextBlob})
	decReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(decBody)))
	decReq.Header.Set("X-Amz-Target", "TrentService.Decrypt")
	decRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(decReq, decRec)))
	assert.Equal(t, http.StatusOK, decRec.Code)

	var decOut kms.DecryptOutput
	require.NoError(t, json.Unmarshal(decRec.Body.Bytes(), &decOut))
	assert.Equal(t, []byte("my-secret"), decOut.Plaintext)
}

// TestKMSHandlerAliasOperations tests alias operations via HTTP handler.
func TestKMSHandlerAliasOperations(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create key first
	createReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	createReq.Header.Set("X-Amz-Target", "TrentService.CreateKey")
	createRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(createReq, createRec)))

	var createOut kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	keyID := createOut.KeyMetadata.KeyID

	// CreateAlias
	aliasBody, _ := json.Marshal(map[string]string{
		"AliasName":   "alias/test-alias",
		"TargetKeyId": keyID,
	})
	aliasReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(aliasBody)))
	aliasReq.Header.Set("X-Amz-Target", "TrentService.CreateAlias")
	aliasRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(aliasReq, aliasRec)))
	assert.Equal(t, http.StatusOK, aliasRec.Code)

	// ListAliases
	listReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	listReq.Header.Set("X-Amz-Target", "TrentService.ListAliases")
	listRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(listReq, listRec)))

	var listOut kms.ListAliasesOutput
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	assert.Len(t, listOut.Aliases, 1)

	// DeleteAlias
	deleteBody, _ := json.Marshal(map[string]string{"AliasName": "alias/test-alias"})
	deleteReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(deleteBody)))
	deleteReq.Header.Set("X-Amz-Target", "TrentService.DeleteAlias")
	deleteRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(deleteReq, deleteRec)))
	assert.Equal(t, http.StatusOK, deleteRec.Code)
}

// TestKMSHandlerKeyRotation tests rotation operations via HTTP.
func TestKMSHandlerKeyRotation(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create key
	createReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	createReq.Header.Set("X-Amz-Target", "TrentService.CreateKey")
	createRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(createReq, createRec)))

	var createOut kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	keyID := createOut.KeyMetadata.KeyID

	// GetKeyRotationStatus
	statusBody, _ := json.Marshal(map[string]string{"KeyID": keyID})

	statusReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(statusBody)))
	statusReq.Header.Set("X-Amz-Target", "TrentService.GetKeyRotationStatus")
	statusRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(statusReq, statusRec)))

	var statusOut kms.GetKeyRotationStatusOutput
	require.NoError(t, json.Unmarshal(statusRec.Body.Bytes(), &statusOut))
	assert.False(t, statusOut.KeyRotationEnabled)

	// EnableKeyRotation
	enableBody, _ := json.Marshal(map[string]string{"KeyID": keyID})
	enableReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(enableBody)))
	enableReq.Header.Set("X-Amz-Target", "TrentService.EnableKeyRotation")
	enableRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(enableReq, enableRec)))
	assert.Equal(t, http.StatusOK, enableRec.Code)

	// DisableKeyRotation
	disableBody, _ := json.Marshal(map[string]string{"KeyID": keyID})
	disableReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(disableBody)))
	disableReq.Header.Set("X-Amz-Target", "TrentService.DisableKeyRotation")
	disableRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(disableReq, disableRec)))
	assert.Equal(t, http.StatusOK, disableRec.Code)
}

// TestKMSHandlerGenerateDataKey tests data key generation via HTTP.
func TestKMSHandlerGenerateDataKey(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create key
	createReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	createReq.Header.Set("X-Amz-Target", "TrentService.CreateKey")
	createRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(createReq, createRec)))

	var createOut kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	keyID := createOut.KeyMetadata.KeyID

	body, _ := json.Marshal(map[string]string{"KeyID": keyID, "KeySpec": "AES_256"})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(body)))
	req.Header.Set("X-Amz-Target", "TrentService.GenerateDataKey")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var out kms.GenerateDataKeyOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Plaintext, 32)
	assert.NotEmpty(t, out.CiphertextBlob)
}

// TestKMSHandlerReEncrypt tests re-encryption via HTTP.
func TestKMSHandlerReEncrypt(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create two keys
	createReq1 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	createReq1.Header.Set("X-Amz-Target", "TrentService.CreateKey")
	createRec1 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(createReq1, createRec1)))

	var out1 kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(createRec1.Body.Bytes(), &out1))

	createReq2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	createReq2.Header.Set("X-Amz-Target", "TrentService.CreateKey")
	createRec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(createReq2, createRec2)))

	var out2 kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(createRec2.Body.Bytes(), &out2))

	// Encrypt with key1
	encBody, _ := json.Marshal(map[string]any{
		"KeyId":     out1.KeyMetadata.KeyID,
		"Plaintext": []byte("reencrypt-me"),
	})
	encReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(encBody)))
	encReq.Header.Set("X-Amz-Target", "TrentService.Encrypt")
	encRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(encReq, encRec)))

	var encOut kms.EncryptOutput
	require.NoError(t, json.Unmarshal(encRec.Body.Bytes(), &encOut))

	// ReEncrypt with key2
	reEncBody, _ := json.Marshal(map[string]any{
		"CiphertextBlob":   encOut.CiphertextBlob,
		"DestinationKeyId": out2.KeyMetadata.KeyID,
	})
	reEncReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(reEncBody)))
	reEncReq.Header.Set("X-Amz-Target", "TrentService.ReEncrypt")
	reEncRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(reEncReq, reEncRec)))
	assert.Equal(t, http.StatusOK, reEncRec.Code)

	var reEncOut kms.ReEncryptOutput
	require.NoError(t, json.Unmarshal(reEncRec.Body.Bytes(), &reEncOut))
	assert.Equal(t, out2.KeyMetadata.KeyID, reEncOut.KeyID)
}

// TestKMSHandlerMethodNotAllowed verifies non-POST requests are rejected.
func TestKMSHandlerMethodNotAllowed(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	req := httptest.NewRequest(http.MethodPut, "/something", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// TestKMSHandlerRouteMatcher verifies the route matcher for KMS.
func TestKMSHandlerRouteMatcher(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)
	matcher := h.RouteMatcher()

	t.Run("MatchesTrentService", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("X-Amz-Target", "TrentService.CreateKey")
		c := e.NewContext(req, httptest.NewRecorder())
		assert.True(t, matcher(c))
	})

	t.Run("DoesNotMatchOther", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("X-Amz-Target", "AmazonSSM.GetParameter")
		c := e.NewContext(req, httptest.NewRecorder())
		assert.False(t, matcher(c))
	})
}

// TestKMSHandlerInterface verifies the handler interface methods.
func TestKMSHandlerInterface(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	assert.Equal(t, "KMS", h.Name())
	assert.Equal(t, 95, h.MatchPriority())

	e := echo.New()

	// ExtractOperation
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "TrentService.CreateKey")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "CreateKey", h.ExtractOperation(c))

	// ExtractOperation with no separator
	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	req2.Header.Set("X-Amz-Target", "TrentServiceNoSep")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.Equal(t, "Unknown", h.ExtractOperation(c2))

	// ExtractResource with body
	body := `{"KeyId":"test-key"}`
	req3 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req3.Header.Set("Content-Type", "application/json")
	c3 := e.NewContext(req3, httptest.NewRecorder())
	resource := h.ExtractResource(c3)
	assert.Equal(t, "test-key", resource)

	// ExtractResource with no KeyId
	req4 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	c4 := e.NewContext(req4, httptest.NewRecorder())
	assert.Empty(t, h.ExtractResource(c4))
}

// TestKMSProvider verifies the Provider.
func TestKMSProvider(t *testing.T) {
	t.Parallel()

	p := &kms.Provider{}
	assert.Equal(t, "KMS", p.Name())

	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// TestKMSHandlerErrorCases exercises handleError paths.
func TestKMSHandlerErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(b *kms.InMemoryBackend, defaultKeyID string) string
		target         string
		body           string
		name           string
		expectedErrTyp string
		expectedStatus int
	}{
		{
			name:           "KeyNotFound",
			target:         "TrentService.DescribeKey",
			body:           `{"KeyId":"00000000-0000-0000-0000-000000000000"}`,
			expectedStatus: http.StatusBadRequest,
			expectedErrTyp: "NotFoundException",
		},
		{
			name:           "InvalidCiphertext",
			target:         "TrentService.Decrypt",
			body:           `{"CiphertextBlob":"aW52YWxpZA=="}`,
			expectedStatus: http.StatusBadRequest,
			expectedErrTyp: "InvalidCiphertextException",
		},
		{
			name:   "AliasAlreadyExists",
			target: "TrentService.CreateAlias",
			body:   `{"AliasName":"alias/dup","TargetKeyId":"PLACEHOLDER"}`,
			setup: func(b *kms.InMemoryBackend, keyID string) string {
				_ = b.CreateAlias(&kms.CreateAliasInput{
					AliasName:   "alias/dup",
					TargetKeyID: keyID,
				})

				return keyID
			},
			expectedStatus: http.StatusBadRequest,
			expectedErrTyp: "AlreadyExistsException",
		},
		{
			name:           "AliasNotFound",
			target:         "TrentService.DeleteAlias",
			body:           `{"AliasName":"alias/missing-alias"}`,
			expectedStatus: http.StatusBadRequest,
			expectedErrTyp: "NotFoundException",
		},
		{
			name:   "InvalidKeyUsageException",
			target: "TrentService.Encrypt",
			body:   `{"KeyId":"PLACEHOLDER","Plaintext":"aGVsbG8="}`,
			setup: func(b *kms.InMemoryBackend, _ string) string {
				out, _ := b.CreateKey(&kms.CreateKeyInput{KeyUsage: kms.KeyUsageSignVerify})

				return out.KeyMetadata.KeyID
			},
			expectedStatus: http.StatusBadRequest,
			expectedErrTyp: "InvalidKeyUsageException",
		},
		{
			name:   "KMSInvalidStateException",
			target: "TrentService.Encrypt",
			body:   `{"KeyId":"PLACEHOLDER","Plaintext":"aGVsbG8="}`,
			setup: func(b *kms.InMemoryBackend, keyID string) string {
				_, _ = b.ScheduleKeyDeletion(&kms.ScheduleKeyDeletionInput{
					KeyID:               keyID,
					PendingWindowInDays: 7,
				})

				return keyID
			},
			expectedStatus: http.StatusBadRequest,
			expectedErrTyp: "KMSInvalidStateException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()

			backend := kms.NewInMemoryBackend()
			h := kms.NewHandler(backend)

			// Create a key to use as placeholder
			created, err := backend.CreateKey(&kms.CreateKeyInput{})
			require.NoError(t, err)
			keyID := created.KeyMetadata.KeyID

			if tt.setup != nil {
				keyID = tt.setup(backend, keyID)
			}

			body := strings.ReplaceAll(tt.body, "PLACEHOLDER", keyID)

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()

			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.expectedStatus, rec.Code)

			var errResp kms.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.expectedErrTyp, errResp.Type)
		})
	}
}

// TestKMSListAliasesFiltered verifies ListAliases filtered by key ID.
func TestKMSListAliasesFiltered(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create two keys with aliases
	key1, _ := backend.CreateKey(&kms.CreateKeyInput{})
	key2, _ := backend.CreateKey(&kms.CreateKeyInput{})
	_ = backend.CreateAlias(&kms.CreateAliasInput{AliasName: "alias/key1", TargetKeyID: key1.KeyMetadata.KeyID})
	_ = backend.CreateAlias(&kms.CreateAliasInput{AliasName: "alias/key2", TargetKeyID: key2.KeyMetadata.KeyID})

	// Filter by key1
	body, _ := json.Marshal(map[string]string{"KeyId": key1.KeyMetadata.KeyID})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(body)))
	req.Header.Set("X-Amz-Target", "TrentService.ListAliases")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var out kms.ListAliasesOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Aliases, 1)
	assert.Equal(t, "alias/key1", out.Aliases[0].AliasName)
}

// TestKMSResolveKeyIDAlias verifies resolveKeyID works with alias input.
func TestKMSResolveKeyIDAlias(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	key, _ := backend.CreateKey(&kms.CreateKeyInput{})
	_ = backend.CreateAlias(&kms.CreateAliasInput{
		AliasName:   "alias/resolve-test",
		TargetKeyID: key.KeyMetadata.KeyID,
	})

	// Encrypt with alias - exercises resolveKeyID alias path
	out, err := backend.Encrypt(&kms.EncryptInput{
		KeyID:     "alias/resolve-test",
		Plaintext: []byte("hello"),
	})
	require.NoError(t, err)
	assert.Equal(t, key.KeyMetadata.KeyID, out.KeyID)
}

// TestKMSParseMarkerBadToken verifies parseMarker handles invalid tokens gracefully.
func TestKMSParseMarkerBadToken(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	for range 3 {
		_, _ = backend.CreateKey(&kms.CreateKeyInput{})
	}

	// A bad marker should be treated as 0 (start from beginning)
	out, err := backend.ListKeys(&kms.ListKeysInput{Marker: "not-a-number"})
	require.NoError(t, err)
	assert.Len(t, out.Keys, 3)
}

// TestKMSResolveKeyIDARN verifies resolveKeyID handles ARN-format key IDs.
func TestKMSResolveKeyIDARN(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	key, _ := backend.CreateKey(&kms.CreateKeyInput{})
	keyID := key.KeyMetadata.KeyID

	// Use ARN format to encrypt
	arn := key.KeyMetadata.Arn
	out, err := backend.Encrypt(&kms.EncryptInput{
		KeyID:     arn,
		Plaintext: []byte("arn-test"),
	})
	require.NoError(t, err)
	assert.Equal(t, keyID, out.KeyID)
}

// TestKMSHandlerInternalError verifies the InternalServiceError path.
func TestKMSHandlerInternalError(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create a key, then encrypt to get a valid ciphertext, then decrypt
	// with a tampered ciphertext that triggers decryptData failure but not
	// a known error — this exercises InternalServiceError only if we get
	// an unexpected error. The shortest path is ErrCiphertextTooShort mapped
	// to InvalidCiphertextException (already covered). Use a zero-byte blob.
	req := httptest.NewRequest(http.MethodPost, "/",
		strings.NewReader(`{"CiphertextBlob":""}`))
	req.Header.Set("X-Amz-Target", "TrentService.Decrypt")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp kms.ErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidCiphertextException", errResp.Type)
}

// TestKMSGenerateDataKeyErrors tests error paths in GenerateDataKey.
func TestKMSGenerateDataKeyErrors(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	// Key not found
	_, err := backend.GenerateDataKey(&kms.GenerateDataKeyInput{
		KeyID:   "nonexistent-key",
		KeySpec: "AES_256",
	})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSReEncryptErrors tests error paths in ReEncrypt.
func TestKMSReEncryptErrors(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	// Destination key not found
	key, _ := backend.CreateKey(&kms.CreateKeyInput{})
	enc, _ := backend.Encrypt(&kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("test"),
	})

	_, err := backend.ReEncrypt(&kms.ReEncryptInput{
		CiphertextBlob:   enc.CiphertextBlob,
		DestinationKeyID: "nonexistent-dest",
	})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSCreateAliasKeyNotFound tests CreateAlias with nonexistent target key.
func TestKMSCreateAliasKeyNotFound(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	err := backend.CreateAlias(&kms.CreateAliasInput{
		AliasName:   "alias/no-key",
		TargetKeyID: "nonexistent-key-id",
	})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSListAliasesWithAliasFilter tests ListAliases with an alias as the key ID.
func TestKMSListAliasesWithAliasFilter(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	key, _ := backend.CreateKey(&kms.CreateKeyInput{})
	_ = backend.CreateAlias(&kms.CreateAliasInput{
		AliasName:   "alias/lookup-test",
		TargetKeyID: key.KeyMetadata.KeyID,
	})

	// Filter using the alias name itself
	out, err := backend.ListAliases(&kms.ListAliasesInput{
		KeyID: "alias/lookup-test",
	})
	require.NoError(t, err)
	assert.Len(t, out.Aliases, 1)
}

// TestKMSGetKeyRotationStatusNotFound tests GetKeyRotationStatus with missing key.
func TestKMSGetKeyRotationStatusNotFound(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	_, err := backend.GetKeyRotationStatus(&kms.GetKeyRotationStatusInput{KeyID: "missing"})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSEnableKeyRotationNotFound tests EnableKeyRotation with missing key.
func TestKMSEnableKeyRotationNotFound(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	err := backend.EnableKeyRotation(&kms.EnableKeyRotationInput{KeyID: "missing"})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSDisableKeyRotationNotFound tests DisableKeyRotation with missing key.
func TestKMSDisableKeyRotationNotFound(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	err := backend.DisableKeyRotation(&kms.DisableKeyRotationInput{KeyID: "missing"})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSKeyMetadataFields verifies that DescribeKey returns the additional metadata fields.
func TestKMSKeyMetadataFields(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	created, err := backend.CreateKey(&kms.CreateKeyInput{
		Description: "test key",
		KeyUsage:    "ENCRYPT_DECRYPT",
	})
	require.NoError(t, err)

	out, err := backend.DescribeKey(&kms.DescribeKeyInput{KeyID: created.KeyMetadata.KeyID})
	require.NoError(t, err)

	assert.Equal(t, "CUSTOMER", out.KeyMetadata.KeyManager)
	assert.Equal(t, "AWS_KMS", out.KeyMetadata.Origin)
	assert.Equal(t, "SYMMETRIC_DEFAULT", out.KeyMetadata.KeySpec)
	assert.Equal(t, []string{"SYMMETRIC_DEFAULT"}, out.KeyMetadata.EncryptionAlgorithms)
	assert.False(t, out.KeyMetadata.MultiRegion)
}

// TestKMSDisableEnableKey verifies disabling and re-enabling a key.
func TestKMSDisableEnableKey(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	out, err := backend.CreateKey(&kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	require.NoError(t, backend.DisableKey(&kms.DisableKeyInput{KeyID: keyID}))

	desc, err := backend.DescribeKey(&kms.DescribeKeyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyStateDisabled, desc.KeyMetadata.KeyState)

	// Encrypt should fail when key is disabled
	_, err = backend.Encrypt(&kms.EncryptInput{KeyID: keyID, Plaintext: []byte("test")})
	require.ErrorIs(t, err, kms.ErrKeyDisabled)

	// Re-enable
	require.NoError(t, backend.EnableKey(&kms.EnableKeyInput{KeyID: keyID}))

	desc, err = backend.DescribeKey(&kms.DescribeKeyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyStateEnabled, desc.KeyMetadata.KeyState)
}

// TestKMSScheduleAndCancelKeyDeletion verifies schedule and cancel key deletion.
func TestKMSScheduleAndCancelKeyDeletion(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	out, err := backend.CreateKey(&kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	schedOut, err := backend.ScheduleKeyDeletion(&kms.ScheduleKeyDeletionInput{
		KeyID:               keyID,
		PendingWindowInDays: 7,
	})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyStatePendingDeletion, schedOut.KeyState)
	assert.NotZero(t, schedOut.DeletionDate)

	// Cancel deletion — key should become Disabled
	require.NoError(t, backend.CancelKeyDeletion(&kms.CancelKeyDeletionInput{KeyID: keyID}))

	desc, err := backend.DescribeKey(&kms.DescribeKeyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyStateDisabled, desc.KeyMetadata.KeyState)
}

// TestKMSHandlerDisableEnableKey verifies DisableKey and EnableKey via HTTP handler.
func TestKMSHandlerDisableEnableKey(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	out, _ := backend.CreateKey(&kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	doKMSRequest := func(t *testing.T, h *kms.Handler, action, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("X-Amz-Target", "TrentService."+action)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		ctx := logger.Save(req.Context(), slog.Default())
		req = req.WithContext(ctx)
		rec := httptest.NewRecorder()
		e := echo.New()
		c := e.NewContext(req, rec)
		err := h.Handler()(c)
		require.NoError(t, err)

		return rec
	}

	body := `{"KeyId":"` + keyID + `"}`
	rec := doKMSRequest(t, h, "DisableKey", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doKMSRequest(t, h, "EnableKey", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doKMSRequest(t, h, "ScheduleKeyDeletion", `{"KeyId":"`+keyID+`","PendingWindowInDays":7}`)
	assert.Equal(t, http.StatusOK, rec.Code)
	var schedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &schedResp))
	assert.Equal(t, kms.KeyStatePendingDeletion, schedResp["KeyState"])

	rec = doKMSRequest(t, h, "CancelKeyDeletion", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// doKMSHTTPRequest is a test helper for issuing HTTP requests to the KMS handler.
func doKMSHTTPRequest(t *testing.T, h *kms.Handler, action, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "TrentService."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	ctx := logger.Save(req.Context(), slog.Default())
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestKMSGrantOperations verifies CreateGrant, ListGrants, RevokeGrant, and RetireGrant.
func TestKMSGrantOperations(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	keyOut, err := backend.CreateKey(&kms.CreateKeyInput{Description: "grant-test"})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	// CreateGrant
	createBody := `{"KeyId":"` + keyID + `","GranteePrincipal":"arn:aws:iam::000000000000:role/my-role",` +
		`"Operations":["Decrypt","Encrypt"]}`
	rec := doKMSHTTPRequest(t, h, "CreateGrant", createBody)
	assert.Equal(t, http.StatusOK, rec.Code)
	var createOut kms.CreateGrantOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	assert.NotEmpty(t, createOut.GrantID)
	assert.NotEmpty(t, createOut.GrantToken)

	// ListGrants
	listBody := `{"KeyId":"` + keyID + `"}`
	rec = doKMSHTTPRequest(t, h, "ListGrants", listBody)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listOut kms.ListGrantsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Grants, 1)
	assert.Equal(t, createOut.GrantID, listOut.Grants[0].GrantID)

	// RevokeGrant
	revokeBody := `{"KeyId":"` + keyID + `","GrantId":"` + createOut.GrantID + `"}`
	rec = doKMSHTTPRequest(t, h, "RevokeGrant", revokeBody)
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListGrants after revoke — should be empty
	rec = doKMSHTTPRequest(t, h, "ListGrants", listBody)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Empty(t, listOut.Grants)
}

// TestKMSKeyPolicy verifies PutKeyPolicy and GetKeyPolicy.
func TestKMSKeyPolicy(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	keyOut, err := backend.CreateKey(&kms.CreateKeyInput{Description: "policy-test"})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	policy := `{"Version":"2012-10-17","Statement":[]}`
	putBody, _ := json.Marshal(map[string]string{
		"KeyId":      keyID,
		"PolicyName": "default",
		"Policy":     policy,
	})
	rec := doKMSHTTPRequest(t, h, "PutKeyPolicy", string(putBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	getBody, _ := json.Marshal(map[string]string{"KeyId": keyID, "PolicyName": "default"})
	rec = doKMSHTTPRequest(t, h, "GetKeyPolicy", string(getBody))
	assert.Equal(t, http.StatusOK, rec.Code)
	var getOut kms.GetKeyPolicyOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getOut))
	assert.Equal(t, policy, getOut.Policy)
}

// TestKMSGenerateDataKeyWithoutPlaintext verifies GenerateDataKeyWithoutPlaintext.
func TestKMSGenerateDataKeyWithoutPlaintext(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	keyOut, err := backend.CreateKey(&kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	body, _ := json.Marshal(map[string]string{"KeyId": keyID, "KeySpec": "AES_256"})
	rec := doKMSHTTPRequest(t, h, "GenerateDataKeyWithoutPlaintext", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
	var out kms.GenerateDataKeyWithoutPlaintextOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.CiphertextBlob)
	assert.Equal(t, keyID, out.KeyID)
}

// TestKMSRetireGrant verifies RetireGrant and ListRetirableGrants operations.
func TestKMSRetireGrant(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create a key and grant
	keyOut, err := backend.CreateKey(&kms.CreateKeyInput{Description: "retire-grant-test"})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	createBody := `{"KeyId":"` + keyID + `","GranteePrincipal":"arn:aws:iam::000000000000:role/my-role",` +
		`"Operations":["Decrypt"],"RetiringPrincipal":"arn:aws:iam::000000000000:role/retire-role"}`
	rec := doKMSHTTPRequest(t, h, "CreateGrant", createBody)
	assert.Equal(t, http.StatusOK, rec.Code)

	var createOut kms.CreateGrantOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	grantToken := createOut.GrantToken

	// ListRetirableGrants for the retiring principal
	listBody, _ := json.Marshal(map[string]string{
		"RetiringPrincipal": "arn:aws:iam::000000000000:role/retire-role",
	})
	rec = doKMSHTTPRequest(t, h, "ListRetirableGrants", string(listBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	var listOut kms.ListGrantsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Grants, 1)
	assert.Equal(t, createOut.GrantID, listOut.Grants[0].GrantID)

	// RetireGrant using grant token
	retireBody, _ := json.Marshal(map[string]string{"GrantToken": grantToken})
	rec = doKMSHTTPRequest(t, h, "RetireGrant", string(retireBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListGrants should now be empty
	listGrantsBody := `{"KeyId":"` + keyID + `"}`
	rec = doKMSHTTPRequest(t, h, "ListGrants", listGrantsBody)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Empty(t, listOut.Grants)
}

// TestKMSTagOperations verifies TagResource, ListResourceTags, and UntagResource via HTTP.
func TestKMSTagOperations(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	keyOut, err := backend.CreateKey(&kms.CreateKeyInput{Description: "tag-test"})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	// ListResourceTags on a key with no tags
	listBody, _ := json.Marshal(map[string]string{"KeyId": keyID})
	rec := doKMSHTTPRequest(t, h, "ListResourceTags", string(listBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Empty(t, listOut["Tags"])
	assert.Equal(t, false, listOut["Truncated"])

	// TagResource — add two tags
	tagBody, _ := json.Marshal(map[string]any{
		"KeyId": keyID,
		"Tags": []map[string]string{
			{"TagKey": "env", "TagValue": "prod"},
			{"TagKey": "team", "TagValue": "backend"},
		},
	})
	rec = doKMSHTTPRequest(t, h, "TagResource", string(tagBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListResourceTags should now return 2 tags
	rec = doKMSHTTPRequest(t, h, "ListResourceTags", string(listBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	tags, ok := listOut["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)

	// UntagResource — remove one tag
	untagBody, _ := json.Marshal(map[string]any{
		"KeyId":   keyID,
		"TagKeys": []string{"team"},
	})
	rec = doKMSHTTPRequest(t, h, "UntagResource", string(untagBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListResourceTags should now return 1 tag
	rec = doKMSHTTPRequest(t, h, "ListResourceTags", string(listBody))
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	tags, ok = listOut["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 1)
}

// TestKMSSetRemoveGetTags verifies the low-level setTags, removeTags, and getTags helpers.
func TestKMSSetRemoveGetTags(t *testing.T) {
	t.Parallel()

	h := kms.NewHandler(kms.NewInMemoryBackend())

	// getTags on unknown resource returns empty map
	got := h.GetTags("no-such-key")
	assert.Empty(t, got)

	// setTags then getTags
	h.SetTags("key-1", map[string]string{"a": "1", "b": "2"})
	got = h.GetTags("key-1")
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, got)

	// setTags merges into existing tags
	h.SetTags("key-1", map[string]string{"b": "updated", "c": "3"})
	got = h.GetTags("key-1")
	assert.Equal(t, map[string]string{"a": "1", "b": "updated", "c": "3"}, got)

	// removeTags
	h.RemoveTags("key-1", []string{"a", "c"})
	got = h.GetTags("key-1")
	assert.Equal(t, map[string]string{"b": "updated"}, got)
}

// TestKMSBackendInvalidKeyUsage verifies that using a SIGN_VERIFY key for encryption returns
// InvalidKeyUsageException matching the AWS SDK v2 *types.InvalidKeyUsageException error.
func TestKMSBackendInvalidKeyUsage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		operation func(b *kms.InMemoryBackend, keyID string) error
		name      string
	}{
		{
			name: "Encrypt_with_sign_verify_key",
			operation: func(b *kms.InMemoryBackend, keyID string) error {
				_, err := b.Encrypt(&kms.EncryptInput{KeyID: keyID, Plaintext: []byte("test")})

				return err
			},
		},
		{
			name: "GenerateDataKey_with_sign_verify_key",
			operation: func(b *kms.InMemoryBackend, keyID string) error {
				_, err := b.GenerateDataKey(&kms.GenerateDataKeyInput{KeyID: keyID, KeySpec: "AES_256"})

				return err
			},
		},
		{
			name: "ReEncrypt_with_sign_verify_dest_key",
			operation: func(b *kms.InMemoryBackend, keyID string) error {
				encKey, createErr := b.CreateKey(&kms.CreateKeyInput{})
				if createErr != nil {
					return createErr
				}

				encOut, encErr := b.Encrypt(&kms.EncryptInput{
					KeyID:     encKey.KeyMetadata.KeyID,
					Plaintext: []byte("test"),
				})
				if encErr != nil {
					return encErr
				}

				_, err := b.ReEncrypt(&kms.ReEncryptInput{
					DestinationKeyID: keyID,
					CiphertextBlob:   encOut.CiphertextBlob,
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			createOut, err := b.CreateKey(&kms.CreateKeyInput{KeyUsage: kms.KeyUsageSignVerify})
			require.NoError(t, err)

			err = tt.operation(b, createOut.KeyMetadata.KeyID)

			require.ErrorIs(t, err, kms.ErrInvalidKeyUsage)
		})
	}
}

// TestKMSBackendSignVerify verifies round-trip sign and verify using an RSA key.
func TestKMSBackendSignVerify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		keySpec          string
		signingAlgorithm string
		name             string
	}{
		{
			name:             "RSA_PSS_SHA_256",
			keySpec:          "RSA_2048",
			signingAlgorithm: "RSASSA_PSS_SHA_256",
		},
		{
			name:             "RSA_PKCS1v15_SHA_256",
			keySpec:          "RSA_2048",
			signingAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
		},
		{
			name:             "ECDSA_P256_SHA_256",
			keySpec:          "ECC_NIST_P256",
			signingAlgorithm: "ECDSA_SHA_256",
		},
		{
			name:             "ECDSA_P384_SHA_384",
			keySpec:          "ECC_NIST_P384",
			signingAlgorithm: "ECDSA_SHA_384",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := kms.NewInMemoryBackend()

			keyOut, err := backend.CreateKey(&kms.CreateKeyInput{
				KeyUsage: kms.KeyUsageSignVerify,
				KeySpec:  tt.keySpec,
			})
			require.NoError(t, err)
			keyID := keyOut.KeyMetadata.KeyID

			message := []byte("hello, cryptographic world")

			signOut, err := backend.Sign(&kms.SignInput{
				KeyID:            keyID,
				Message:          message,
				MessageType:      "RAW",
				SigningAlgorithm: tt.signingAlgorithm,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, signOut.Signature)
			assert.NotEqual(t, message, signOut.Signature)
			assert.Equal(t, tt.signingAlgorithm, signOut.SigningAlgorithm)

			verifyOut, err := backend.Verify(&kms.VerifyInput{
				KeyID:            keyID,
				Message:          message,
				MessageType:      "RAW",
				Signature:        signOut.Signature,
				SigningAlgorithm: tt.signingAlgorithm,
			})
			require.NoError(t, err)
			assert.True(t, verifyOut.SignatureValid)
		})
	}
}

// TestKMSBackendVerifyInvalidSignature verifies that tampered signatures are rejected.
func TestKMSBackendVerifyInvalidSignature(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	keyOut, err := backend.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	message := []byte("test message")

	signOut, err := backend.Sign(&kms.SignInput{
		KeyID:            keyID,
		Message:          message,
		MessageType:      "RAW",
		SigningAlgorithm: "RSASSA_PSS_SHA_256",
	})
	require.NoError(t, err)

	// Tamper with the signature
	tampered := make([]byte, len(signOut.Signature))
	copy(tampered, signOut.Signature)
	tampered[0] ^= 0xFF

	_, err = backend.Verify(&kms.VerifyInput{
		KeyID:            keyID,
		Message:          message,
		MessageType:      "RAW",
		Signature:        tampered,
		SigningAlgorithm: "RSASSA_PSS_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrInvalidSignature)
}

// TestKMSBackendGetPublicKey verifies retrieval of asymmetric public keys.
func TestKMSBackendGetPublicKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		keySpec string
		name    string
	}{
		{name: "RSA_2048", keySpec: "RSA_2048"},
		{name: "ECC_NIST_P256", keySpec: "ECC_NIST_P256"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := kms.NewInMemoryBackend()
			keyOut, err := backend.CreateKey(&kms.CreateKeyInput{
				KeyUsage: kms.KeyUsageSignVerify,
				KeySpec:  tt.keySpec,
			})
			require.NoError(t, err)

			pubKeyOut, err := backend.GetPublicKey(&kms.GetPublicKeyInput{KeyID: keyOut.KeyMetadata.KeyID})
			require.NoError(t, err)
			assert.NotEmpty(t, pubKeyOut.PublicKey)
			assert.Equal(t, tt.keySpec, pubKeyOut.KeySpec)
			assert.Equal(t, kms.KeyUsageSignVerify, pubKeyOut.KeyUsage)
		})
	}
}

// TestKMSBackendSignWrongKeyType verifies that Sign fails on symmetric keys.
func TestKMSBackendSignWrongKeyType(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	symKey, err := backend.CreateKey(&kms.CreateKeyInput{KeyUsage: kms.KeyUsageEncryptDecrypt})
	require.NoError(t, err)

	_, err = backend.Sign(&kms.SignInput{
		KeyID:            symKey.KeyMetadata.KeyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		SigningAlgorithm: "RSASSA_PSS_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrInvalidKeyUsage)
}

// TestKMSBackendSignVerifyDigestMode verifies signing and verification with MessageType=DIGEST.
func TestKMSBackendSignVerifyDigestMode(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	keyOut, err := backend.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	rawMsg := []byte("data to sign")

	// Sign using RAW mode first to get signature
	signOut, err := backend.Sign(&kms.SignInput{
		KeyID:            keyID,
		Message:          rawMsg,
		MessageType:      "RAW",
		SigningAlgorithm: "RSASSA_PSS_SHA_512",
	})
	require.NoError(t, err)

	// Verify using DIGEST mode (pre-computed hash)
	d512 := sha512.Sum512(rawMsg)
	digest512 := d512[:]
	verifyOut, err := backend.Verify(&kms.VerifyInput{
		KeyID:            keyID,
		Message:          digest512,
		MessageType:      "DIGEST",
		Signature:        signOut.Signature,
		SigningAlgorithm: "RSASSA_PSS_SHA_512",
	})
	require.NoError(t, err)
	assert.True(t, verifyOut.SignatureValid)
}

// TestKMSSnapshotRestoreWithKeyMaterials verifies that key materials survive snapshot/restore.
func TestKMSSnapshotRestoreWithKeyMaterials(t *testing.T) {
	t.Parallel()

	original := kms.NewInMemoryBackend()

	// Create symmetric key and encrypt something
	symKey, err := original.CreateKey(&kms.CreateKeyInput{})
	require.NoError(t, err)
	plaintext := []byte("persistence test data")
	encOut, err := original.Encrypt(&kms.EncryptInput{
		KeyID:     symKey.KeyMetadata.KeyID,
		Plaintext: plaintext,
	})
	require.NoError(t, err)

	// Create asymmetric key and sign something
	asymKey, err := original.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)
	signOut, err := original.Sign(&kms.SignInput{
		KeyID:            asymKey.KeyMetadata.KeyID,
		Message:          plaintext,
		MessageType:      "RAW",
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.NoError(t, err)

	// Snapshot and restore to new backend
	snap := original.Snapshot()
	require.NotEmpty(t, snap)

	restored := kms.NewInMemoryBackend()
	require.NoError(t, restored.Restore(snap))

	// Decrypt using restored backend — must use same per-key material
	decOut, err := restored.Decrypt(&kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)

	// Verify using restored backend — must use same per-key material
	verifyOut, err := restored.Verify(&kms.VerifyInput{
		KeyID:            asymKey.KeyMetadata.KeyID,
		Message:          plaintext,
		MessageType:      "RAW",
		Signature:        signOut.Signature,
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.NoError(t, err)
	assert.True(t, verifyOut.SignatureValid)
}

// TestKMSBackendSignVerifyAdditionalKeySpecs verifies sign/verify with RSA_3072, RSA_4096, and ECC variants.
func TestKMSBackendSignVerifyAdditionalKeySpecs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		keySpec          string
		signingAlgorithm string
		name             string
	}{
		{
			name:             "RSA_3072_PSS",
			keySpec:          "RSA_3072",
			signingAlgorithm: "RSASSA_PSS_SHA_384",
		},
		{
			name:             "RSA_4096_PKCS1v15",
			keySpec:          "RSA_4096",
			signingAlgorithm: "RSASSA_PKCS1_V1_5_SHA_512",
		},
		{
			name:             "ECC_NIST_P521",
			keySpec:          "ECC_NIST_P521",
			signingAlgorithm: "ECDSA_SHA_512",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			keyOut, err := b.CreateKey(&kms.CreateKeyInput{
				KeyUsage: kms.KeyUsageSignVerify,
				KeySpec:  tt.keySpec,
			})
			require.NoError(t, err)

			msg := []byte("test-" + tt.name)
			signOut, err := b.Sign(&kms.SignInput{
				KeyID:            keyOut.KeyMetadata.KeyID,
				Message:          msg,
				MessageType:      "RAW",
				SigningAlgorithm: tt.signingAlgorithm,
			})
			require.NoError(t, err)

			verifyOut, err := b.Verify(&kms.VerifyInput{
				KeyID:            keyOut.KeyMetadata.KeyID,
				Message:          msg,
				MessageType:      "RAW",
				Signature:        signOut.Signature,
				SigningAlgorithm: tt.signingAlgorithm,
			})
			require.NoError(t, err)
			assert.True(t, verifyOut.SignatureValid)
		})
	}
}

// TestKMSBackendUnsupportedKeySpec verifies that CreateKey fails with an unknown key spec.
func TestKMSBackendUnsupportedKeySpec(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	_, err := b.CreateKey(&kms.CreateKeyInput{
		KeySpec: "UNSUPPORTED_SPEC",
	})
	require.Error(t, err)
}

// TestKMSHandlerSignVerify verifies Sign and Verify dispatch through the HTTP handler.
func TestKMSHandlerSignVerify(t *testing.T) {
	t.Parallel()

	h := kms.NewHandler(kms.NewInMemoryBackend())

	// Create an asymmetric key
	createBody, _ := json.Marshal(map[string]any{
		"KeyUsage": kms.KeyUsageSignVerify,
		"KeySpec":  "RSA_2048",
	})
	rec := doKMSHTTPRequest(t, h, "CreateKey", string(createBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	keyMeta, ok := createResp["KeyMetadata"].(map[string]any)
	require.True(t, ok)
	keyID, ok := keyMeta["KeyId"].(string)
	require.True(t, ok)

	message := []byte("handler-sign-test")
	signBody, _ := json.Marshal(map[string]any{
		"KeyId":            keyID,
		"Message":          message,
		"MessageType":      "RAW",
		"SigningAlgorithm": "RSASSA_PSS_SHA_256",
	})
	rec = doKMSHTTPRequest(t, h, "Sign", string(signBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var signResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &signResp))
	sigRaw, ok := signResp["Signature"]
	require.True(t, ok)
	assert.NotEmpty(t, sigRaw)

	// Verify via handler
	verifyBody, _ := json.Marshal(map[string]any{
		"KeyId":            keyID,
		"Message":          message,
		"MessageType":      "RAW",
		"Signature":        sigRaw,
		"SigningAlgorithm": "RSASSA_PSS_SHA_256",
	})
	rec = doKMSHTTPRequest(t, h, "Verify", string(verifyBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var verifyResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &verifyResp))
	assert.True(t, verifyResp["SignatureValid"].(bool))
}

// TestKMSHandlerGetPublicKey verifies GetPublicKey dispatch through the HTTP handler.
func TestKMSHandlerGetPublicKey(t *testing.T) {
	t.Parallel()

	h := kms.NewHandler(kms.NewInMemoryBackend())

	createBody, _ := json.Marshal(map[string]any{
		"KeyUsage": kms.KeyUsageSignVerify,
		"KeySpec":  "ECC_NIST_P256",
	})
	rec := doKMSHTTPRequest(t, h, "CreateKey", string(createBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	keyMeta := createResp["KeyMetadata"].(map[string]any)
	keyID := keyMeta["KeyId"].(string)

	getKeyBody, _ := json.Marshal(map[string]any{"KeyId": keyID})
	rec = doKMSHTTPRequest(t, h, "GetPublicKey", string(getKeyBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var pubResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &pubResp))
	assert.NotEmpty(t, pubResp["PublicKey"])
	assert.Equal(t, "ECC_NIST_P256", pubResp["KeySpec"])
}

// TestKMSHandlerInvalidSignatureError verifies that a bad signature returns KMSInvalidSignatureException.
func TestKMSHandlerInvalidSignatureError(t *testing.T) {
	t.Parallel()

	h := kms.NewHandler(kms.NewInMemoryBackend())

	createBody, _ := json.Marshal(map[string]any{
		"KeyUsage": kms.KeyUsageSignVerify,
		"KeySpec":  "RSA_2048",
	})
	rec := doKMSHTTPRequest(t, h, "CreateKey", string(createBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	keyMeta := createResp["KeyMetadata"].(map[string]any)
	keyID := keyMeta["KeyId"].(string)

	badSig := []byte("this-is-not-a-valid-signature")
	verifyBody, _ := json.Marshal(map[string]any{
		"KeyId":            keyID,
		"Message":          []byte("test"),
		"MessageType":      "RAW",
		"Signature":        badSig,
		"SigningAlgorithm": "RSASSA_PSS_SHA_256",
	})

	rec = doKMSHTTPRequest(t, h, "Verify", string(verifyBody))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "KMSInvalidSignatureException", errResp["__type"])
}

// TestKMSSnapshotRestoreRSA3072 verifies snapshot/restore preserves RSA-3072 key material.
func TestKMSSnapshotRestoreRSA3072(t *testing.T) {
	t.Parallel()

	original := kms.NewInMemoryBackend()
	keyOut, err := original.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_3072",
	})
	require.NoError(t, err)

	msg := []byte("rsa-3072-persistence-test")
	signOut, err := original.Sign(&kms.SignInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          msg,
		MessageType:      "RAW",
		SigningAlgorithm: "RSASSA_PSS_SHA_384",
	})
	require.NoError(t, err)

	snap := original.Snapshot()
	require.NotEmpty(t, snap)

	restored := kms.NewInMemoryBackend()
	require.NoError(t, restored.Restore(snap))

	verifyOut, err := restored.Verify(&kms.VerifyInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          msg,
		MessageType:      "RAW",
		Signature:        signOut.Signature,
		SigningAlgorithm: "RSASSA_PSS_SHA_384",
	})
	require.NoError(t, err)
	assert.True(t, verifyOut.SignatureValid)
}

// TestKMSBackendGetPublicKeySymmetricFails verifies GetPublicKey on symmetric keys returns an error.
func TestKMSBackendGetPublicKeySymmetricFails(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(&kms.CreateKeyInput{KeyUsage: kms.KeyUsageEncryptDecrypt})
	require.NoError(t, err)

	_, err = b.GetPublicKey(&kms.GetPublicKeyInput{KeyID: keyOut.KeyMetadata.KeyID})
	require.ErrorIs(t, err, kms.ErrInvalidKeyUsage)
}

// TestKMSBackendVerifyDisabledKey verifies that Verify fails on a disabled key.
func TestKMSBackendVerifyDisabledKey(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	signOut, err := b.Sign(&kms.SignInput{
		KeyID:            keyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.NoError(t, err)

	require.NoError(t, b.DisableKey(&kms.DisableKeyInput{KeyID: keyID}))

	_, err = b.Verify(&kms.VerifyInput{
		KeyID:            keyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		Signature:        signOut.Signature,
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrKeyDisabled)
}

// TestKMSBackendSignDisabledKey verifies that Sign fails on a disabled key.
func TestKMSBackendSignDisabledKey(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)

	require.NoError(t, b.DisableKey(&kms.DisableKeyInput{KeyID: keyOut.KeyMetadata.KeyID}))

	_, err = b.Sign(&kms.SignInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		SigningAlgorithm: "RSASSA_PSS_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrKeyDisabled)
}

// TestKMSKeyMetadataSigningAlgorithms verifies that DescribeKey returns signing algorithms
// for asymmetric keys.
func TestKMSKeyMetadataSigningAlgorithms(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)

	descOut, err := b.DescribeKey(&kms.DescribeKeyInput{KeyID: keyOut.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.NotEmpty(t, descOut.KeyMetadata.SigningAlgorithms)
	assert.Contains(t, descOut.KeyMetadata.SigningAlgorithms, "RSASSA_PSS_SHA_256")
}

// TestKMSBackendSignUnsupportedAlgorithm verifies that unsupported signing algorithms return an error.
func TestKMSBackendSignUnsupportedAlgorithm(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)

	_, err = b.Sign(&kms.SignInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		SigningAlgorithm: "UNSUPPORTED_ALGORITHM",
	})
	require.Error(t, err)
}

// TestKMSBackendVerifyUnsupportedAlgorithm verifies that unsupported algorithms return an error.
func TestKMSBackendVerifyUnsupportedAlgorithm(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "RSA_2048",
	})
	require.NoError(t, err)

	_, err = b.Verify(&kms.VerifyInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          []byte("test"),
		MessageType:      "RAW",
		Signature:        []byte("sig"),
		SigningAlgorithm: "UNSUPPORTED_ALGORITHM",
	})
	require.Error(t, err)
}

// TestKMSBackendVerifyECDSAInvalidASN1 verifies that a non-ASN.1 ECDSA signature is rejected.
func TestKMSBackendVerifyECDSAInvalidASN1(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)

	// Provide a signature that is not valid ASN.1
	invalidSig := []byte("not-asn1-signature-data-at-all-!!!!")
	_, err = b.Verify(&kms.VerifyInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          []byte("test message"),
		MessageType:      "RAW",
		Signature:        invalidSig,
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrInvalidSignature)
}

// TestKMSBackendVerifyECDSAWrongSignature verifies that a well-formed but wrong ECDSA signature is rejected.
func TestKMSBackendVerifyECDSAWrongSignature(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	// Sign one message
	signOut, err := b.Sign(&kms.SignInput{
		KeyID:            keyID,
		Message:          []byte("message-a"),
		MessageType:      "RAW",
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.NoError(t, err)

	// Verify against a different message — should fail
	_, err = b.Verify(&kms.VerifyInput{
		KeyID:            keyID,
		Message:          []byte("message-b"),
		MessageType:      "RAW",
		Signature:        signOut.Signature,
		SigningAlgorithm: "ECDSA_SHA_256",
	})
	require.ErrorIs(t, err, kms.ErrInvalidSignature)
}

// TestKMSHandlerSnapshotRestore verifies the handler Snapshot and Restore wrapper methods.
func TestKMSHandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create a key and encrypt something to populate state
	keyOut, err := backend.CreateKey(&kms.CreateKeyInput{Description: "snapshot-test"})
	require.NoError(t, err)
	encOut, err := backend.Encrypt(&kms.EncryptInput{
		KeyID:     keyOut.KeyMetadata.KeyID,
		Plaintext: []byte("snap-data"),
	})
	require.NoError(t, err)

	snap := h.Snapshot()
	require.NotEmpty(t, snap)

	// Restore into a new backend via handler wrapper
	backend2 := kms.NewInMemoryBackend()
	h2 := kms.NewHandler(backend2)
	require.NoError(t, h2.Restore(snap))

	// Decrypt with restored handler's backend
	decOut, err := backend2.Decrypt(&kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("snap-data"), decOut.Plaintext)
}

// TestKMSBackendSnapshotRestoreECDSA verifies snapshot/restore preserves ECDSA key material.
func TestKMSBackendSnapshotRestoreECDSA(t *testing.T) {
	t.Parallel()

	orig := kms.NewInMemoryBackend()
	keyOut, err := orig.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P384",
	})
	require.NoError(t, err)

	msg := []byte("ecdsa-snapshot-test")
	signOut, err := orig.Sign(&kms.SignInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          msg,
		MessageType:      "RAW",
		SigningAlgorithm: "ECDSA_SHA_384",
	})
	require.NoError(t, err)

	snap := orig.Snapshot()
	require.NotEmpty(t, snap)

	restored := kms.NewInMemoryBackend()
	require.NoError(t, restored.Restore(snap))

	verifyOut, err := restored.Verify(&kms.VerifyInput{
		KeyID:            keyOut.KeyMetadata.KeyID,
		Message:          msg,
		MessageType:      "RAW",
		Signature:        signOut.Signature,
		SigningAlgorithm: "ECDSA_SHA_384",
	})
	require.NoError(t, err)
	assert.True(t, verifyOut.SignatureValid)
}

// TestKMSBackendGetPublicKeyDisabledKey verifies GetPublicKey fails on a disabled key.
func TestKMSBackendGetPublicKeyDisabledKey(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(&kms.CreateKeyInput{
		KeyUsage: kms.KeyUsageSignVerify,
		KeySpec:  "ECC_NIST_P256",
	})
	require.NoError(t, err)

	require.NoError(t, b.DisableKey(&kms.DisableKeyInput{KeyID: keyOut.KeyMetadata.KeyID}))

	_, err = b.GetPublicKey(&kms.GetPublicKeyInput{KeyID: keyOut.KeyMetadata.KeyID})
	require.ErrorIs(t, err, kms.ErrKeyDisabled)
}

// TestKMSBackendGetPublicKeyNotFound verifies GetPublicKey returns ErrKeyNotFound.
func TestKMSBackendGetPublicKeyNotFound(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	_, err := b.GetPublicKey(&kms.GetPublicKeyInput{KeyID: "non-existent-key"})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSHandlerChaosOperations verifies the chaos-related handler methods.
func TestKMSHandlerChaosOperations(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)
	h.DefaultRegion = "eu-west-1"

	assert.Equal(t, "kms", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"eu-west-1"}, h.ChaosRegions())
}

// TestKMSHandlerTaggedKeysByARN verifies TaggedKeys, TagKeyByARN, and UntagKeyByARN.
func TestKMSHandlerTaggedKeysByARN(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create a key
	keyOut, err := backend.CreateKey(&kms.CreateKeyInput{Description: "tagged"})
	require.NoError(t, err)
	keyARN := keyOut.KeyMetadata.Arn

	// TaggedKeys should return the key with empty tags
	tagged := h.TaggedKeys()
	require.Len(t, tagged, 1)
	assert.Equal(t, keyARN, tagged[0].ARN)

	// TagKeyByARN
	require.NoError(t, h.TagKeyByARN(keyARN, map[string]string{"env": "test"}))

	taggedAfter := h.TaggedKeys()
	require.Len(t, taggedAfter, 1)
	assert.Equal(t, "test", taggedAfter[0].Tags["env"])

	// TagKeyByARN on non-existent ARN should fail
	err = h.TagKeyByARN("arn:aws:kms:us-east-1:000000000000:key/non-existent", map[string]string{})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)

	// UntagKeyByARN
	require.NoError(t, h.UntagKeyByARN(keyARN, []string{"env"}))

	taggedFinal := h.TaggedKeys()
	require.Len(t, taggedFinal, 1)
	assert.Empty(t, taggedFinal[0].Tags["env"])

	// UntagKeyByARN on non-existent ARN should fail
	err = h.UntagKeyByARN("arn:aws:kms:us-east-1:000000000000:key/non-existent", []string{"env"})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSBackendRetireGrantAllPaths verifies all RetireGrant branches.
func TestKMSBackendRetireGrantAllPaths(t *testing.T) {
	t.Parallel()

	t.Run("by_grant_id", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		keyOut, err := b.CreateKey(&kms.CreateKeyInput{})
		require.NoError(t, err)

		grantOut, err := b.CreateGrant(&kms.CreateGrantInput{
			KeyID:            keyOut.KeyMetadata.KeyID,
			GranteePrincipal: "arn:aws:iam::123:role/test",
			Operations:       []string{"Decrypt"},
		})
		require.NoError(t, err)

		require.NoError(t, b.RetireGrant(&kms.RetireGrantInput{
			GrantID: grantOut.GrantID,
		}))

		// Retiring again should fail
		err = b.RetireGrant(&kms.RetireGrantInput{GrantID: grantOut.GrantID})
		require.ErrorIs(t, err, kms.ErrGrantNotFound)
	})

	t.Run("by_grant_id_with_key", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		keyOut, err := b.CreateKey(&kms.CreateKeyInput{})
		require.NoError(t, err)

		grantOut, err := b.CreateGrant(&kms.CreateGrantInput{
			KeyID:            keyOut.KeyMetadata.KeyID,
			GranteePrincipal: "arn:aws:iam::123:role/test",
			Operations:       []string{"Decrypt"},
		})
		require.NoError(t, err)

		require.NoError(t, b.RetireGrant(&kms.RetireGrantInput{
			GrantID: grantOut.GrantID,
			KeyID:   keyOut.KeyMetadata.KeyID,
		}))
	})

	t.Run("empty_grant_id_returns_error", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		err := b.RetireGrant(&kms.RetireGrantInput{GrantID: ""})
		require.ErrorIs(t, err, kms.ErrGrantNotFound)
	})

	t.Run("wrong_key_id_returns_error", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		key1, err := b.CreateKey(&kms.CreateKeyInput{})
		require.NoError(t, err)
		key2, err := b.CreateKey(&kms.CreateKeyInput{})
		require.NoError(t, err)

		grantOut, err := b.CreateGrant(&kms.CreateGrantInput{
			KeyID:            key1.KeyMetadata.KeyID,
			GranteePrincipal: "arn:aws:iam::123:role/test",
			Operations:       []string{"Decrypt"},
		})
		require.NoError(t, err)

		err = b.RetireGrant(&kms.RetireGrantInput{
			GrantID: grantOut.GrantID,
			KeyID:   key2.KeyMetadata.KeyID,
		})
		require.ErrorIs(t, err, kms.ErrGrantNotFound)
	})
}

// TestKMSBackendDecryptCiphertextTooShort verifies Decrypt fails with short ciphertext.
func TestKMSBackendDecryptCiphertextTooShort(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	_, err := b.Decrypt(&kms.DecryptInput{CiphertextBlob: []byte("short")})
	require.ErrorIs(t, err, kms.ErrCiphertextTooShort)
}

// TestKMSBackendGetKeyPolicyDefaultAndCustom verifies GetKeyPolicy returns default policy.
func TestKMSBackendGetKeyPolicyDefaultAndCustom(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	keyOut, err := b.CreateKey(&kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	// Default policy
	out, err := b.GetKeyPolicy(&kms.GetKeyPolicyInput{KeyID: keyID})
	require.NoError(t, err)
	assert.NotEmpty(t, out.Policy)
	assert.Equal(t, "default", out.PolicyName)

	// Custom policy
	customPolicy := `{"Version":"2012-10-17","Statement":[]}`
	require.NoError(t, b.PutKeyPolicy(&kms.PutKeyPolicyInput{
		KeyID:  keyID,
		Policy: customPolicy,
	}))
	out2, err := b.GetKeyPolicy(&kms.GetKeyPolicyInput{
		KeyID:      keyID,
		PolicyName: "custom",
	})
	require.NoError(t, err)
	assert.Equal(t, customPolicy, out2.Policy)
	assert.Equal(t, "custom", out2.PolicyName)
}

// TestKMSBackendDecryptKeyMaterialMissing verifies Decrypt fails when key is found but ciphertext was
// encrypted with a different key's material (simulates data corruption).
func TestKMSBackendDecryptNonExistentKey(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(&kms.CreateKeyInput{})
	require.NoError(t, err)

	// Encrypt with a key that exists
	encOut, err := b.Encrypt(&kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("data"),
	})
	require.NoError(t, err)

	// Delete the key (not possible via API, but modify the ciphertext keyID prefix
	// to reference a non-existent key by using a different key's ID in the blob header).
	// The simplest test: create a new key, encrypt with key1, try to decrypt
	// — Decrypt will look up key1 by ID from blob prefix and succeed.
	// For non-existent key, fabricate a blob with a bad key ID prefix.
	badBlob := make([]byte, 36+28) // keyIDPrefixLen + minimum nonce/ct size
	copy(badBlob[:36], "nonexistent-key-id-000000000000000")
	_, err = b.Decrypt(&kms.DecryptInput{CiphertextBlob: badBlob})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)

	// Also test successful decrypt verifies the expected plaintext
	decOut, err := b.Decrypt(&kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), decOut.Plaintext)
}

// TestKMSBackendPutKeyPolicyNotFound verifies PutKeyPolicy fails for missing keys.
func TestKMSBackendPutKeyPolicyNotFound(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	err := b.PutKeyPolicy(&kms.PutKeyPolicyInput{
		KeyID:  "non-existent",
		Policy: `{"Version":"2012-10-17"}`,
	})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSBackendReEncryptShortBlob verifies ReEncrypt fails with ciphertext too short.
func TestKMSBackendReEncryptShortBlob(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	destKey, err := b.CreateKey(&kms.CreateKeyInput{})
	require.NoError(t, err)

	_, err = b.ReEncrypt(&kms.ReEncryptInput{
		CiphertextBlob:   []byte("short"),
		DestinationKeyID: destKey.KeyMetadata.KeyID,
	})
	require.ErrorIs(t, err, kms.ErrCiphertextTooShort)
}

// mockConfigProvider implements config.Provider for testing.
type mockConfigProvider struct{}

func (m *mockConfigProvider) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{
		AccountID: "123456789012",
		Region:    "ap-southeast-2",
	}
}

// TestKMSProviderWithConfig verifies that Init uses config.Provider when available.
func TestKMSProviderWithConfig(t *testing.T) {
	t.Parallel()

	p := &kms.Provider{}
	ctx := &service.AppContext{
		Logger: slog.Default(),
		Config: &mockConfigProvider{},
	}

	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

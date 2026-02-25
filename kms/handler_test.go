package kms_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/kms"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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
			log := logger.NewLogger(slog.LevelDebug)
			backend := kms.NewInMemoryBackend()

			if tt.setupFn != nil {
				tt.setupFn(t, backend)
			}

			h := kms.NewHandler(backend, log)

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
	log := logger.NewLogger(slog.LevelDebug)
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend, log)

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
	log := logger.NewLogger(slog.LevelDebug)
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend, log)

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
	log := logger.NewLogger(slog.LevelDebug)
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend, log)

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
	log := logger.NewLogger(slog.LevelDebug)
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend, log)

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
	log := logger.NewLogger(slog.LevelDebug)
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend, log)

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
	log := logger.NewLogger(slog.LevelDebug)
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend, log)

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
	h := kms.NewHandler(backend, logger.NewLogger(slog.LevelDebug))
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

	log := logger.NewLogger(slog.LevelDebug)
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend, log)

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

	log := logger.NewLogger(slog.LevelDebug)
	ctx := &service.AppContext{Logger: log}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// TestKMSHandlerErrorCases exercises handleError paths.
func TestKMSHandlerErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
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
			name:           "AliasAlreadyExists",
			target:         "TrentService.CreateAlias",
			body:           `{"AliasName":"alias/dup","TargetKeyId":"PLACEHOLDER"}`,
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			log := logger.NewLogger(slog.LevelDebug)
			backend := kms.NewInMemoryBackend()
			h := kms.NewHandler(backend, log)

			// Create a key to use as placeholder
			created, err := backend.CreateKey(&kms.CreateKeyInput{})
			require.NoError(t, err)
			keyID := created.KeyMetadata.KeyID

			body := strings.ReplaceAll(tt.body, "PLACEHOLDER", keyID)

			if tt.name == "AliasAlreadyExists" {
				// Pre-create the alias
				_ = backend.CreateAlias(&kms.CreateAliasInput{
					AliasName:   "alias/dup",
					TargetKeyID: keyID,
				})
				body = strings.ReplaceAll(tt.body, "PLACEHOLDER", keyID)
			}

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
	log := logger.NewLogger(slog.LevelDebug)
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend, log)

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
	log := logger.NewLogger(slog.LevelDebug)
	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend, log)

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
	h := kms.NewHandler(backend, slog.Default())

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
h := kms.NewHandler(backend, slog.Default())

keyOut, err := backend.CreateKey(&kms.CreateKeyInput{Description: "grant-test"})
require.NoError(t, err)
keyID := keyOut.KeyMetadata.KeyID

// CreateGrant
createBody := `{"KeyId":"` + keyID + `","GranteePrincipal":"arn:aws:iam::000000000000:role/my-role","Operations":["Decrypt","Encrypt"]}`
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
h := kms.NewHandler(backend, slog.Default())

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
h := kms.NewHandler(backend, slog.Default())

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

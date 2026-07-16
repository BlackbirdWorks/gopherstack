package kms_test

import (
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

	"strings"
)

func TestHandler_ListAliases_FilterByKeyID_ViaHTTP(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	require.NoError(t, b.CreateAlias(context.Background(), &kms.CreateAliasInput{
		AliasName:   "alias/filter-test",
		TargetKeyID: keyID,
	}))

	body := fmt.Sprintf(`{"KeyId":"%s"}`, keyID)
	rec := b2postKMSOp(t, h, "ListAliases", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Aliases []struct {
			AliasName string `json:"AliasName"`
		} `json:"Aliases"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Aliases)
	for _, a := range resp.Aliases {
		assert.Equal(t, "alias/filter-test", a.AliasName)
	}
}

func TestHandler_RotateKeyOnDemand_ViaHTTP(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	body := fmt.Sprintf(`{"KeyId":"%s"}`, keyID)
	rec := b2postKMSOp(t, h, "RotateKeyOnDemand", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		KeyID string `json:"KeyId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, keyID, resp.KeyID)
}

func TestHandler_EnableKeyRotation_ViaHTTP(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	body := fmt.Sprintf(`{"KeyId":"%s","RotationPeriodInDays":90}`, keyID)
	rec := b2postKMSOp(t, h, "EnableKeyRotation", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify via GetKeyRotationStatus
	statusBody := fmt.Sprintf(`{"KeyId":"%s"}`, keyID)
	rec2 := b2postKMSOp(t, h, "GetKeyRotationStatus", statusBody)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var status struct {
		KeyRotationEnabled   bool  `json:"KeyRotationEnabled"`
		RotationPeriodInDays int32 `json:"RotationPeriodInDays"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &status))
	assert.True(t, status.KeyRotationEnabled)
	assert.Equal(t, int32(90), status.RotationPeriodInDays)
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
				_ = b.CreateAlias(context.Background(), &kms.CreateAliasInput{
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
				out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeyUsage: kms.KeyUsageSignVerify})

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
				_, _ = b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
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
			created, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
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
	key1, _ := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
	key2, _ := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
	_ = backend.CreateAlias(
		context.Background(),
		&kms.CreateAliasInput{AliasName: "alias/key1", TargetKeyID: key1.KeyMetadata.KeyID},
	)
	_ = backend.CreateAlias(
		context.Background(),
		&kms.CreateAliasInput{AliasName: "alias/key2", TargetKeyID: key2.KeyMetadata.KeyID},
	)

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

// TestKMSUpdateAlias_Handler verifies UpdateAlias via the HTTP handler.
func TestKMSUpdateAlias_Handler(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)

	k1, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	k2, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	require.NoError(t, b.CreateAlias(context.Background(), &kms.CreateAliasInput{
		AliasName:   "alias/handler-update-test",
		TargetKeyID: k1.KeyMetadata.KeyID,
	}))

	body, err := json.Marshal(kms.UpdateAliasInput{
		AliasName:   "alias/handler-update-test",
		TargetKeyID: k2.KeyMetadata.KeyID,
	})
	require.NoError(t, err)

	rec := doKMSHTTPRequest(t, h, "UpdateAlias", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// Confirm the alias now points to k2.
	aliases, listErr := b.ListAliases(context.Background(), &kms.ListAliasesInput{KeyID: k2.KeyMetadata.KeyID})
	require.NoError(t, listErr)
	require.Len(t, aliases.Aliases, 1)
	assert.Equal(t, "alias/handler-update-test", aliases.Aliases[0].AliasName)
}

func TestHandlerCreateAliasWhitespaceRejected(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	keyRec := sendKMSOp(t, h, "CreateKey", `{}`)
	require.Equal(t, http.StatusOK, keyRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(keyRec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	rec := sendKMSOp(t, h, "CreateAlias", `{"AliasName":"alias/my bad alias","TargetKeyId":"`+keyID+`"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerListKeyRotationsViaHTTP(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	keyRec := sendKMSOp(t, h, "CreateKey", `{}`)
	require.Equal(t, http.StatusOK, keyRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(keyRec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	sendKMSOp(t, h, "RotateKeyOnDemand", `{"KeyId":"`+keyID+`"}`)

	rec := sendKMSOp(t, h, "ListKeyRotations", `{"KeyId":"`+keyID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	rotations := out["Rotations"].([]any)
	require.NotEmpty(t, rotations)

	first := rotations[0].(map[string]any)
	assert.NotEmpty(t, first["RotationType"])
	assert.NotEmpty(t, first["KeyId"])
}

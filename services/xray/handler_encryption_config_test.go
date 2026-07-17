package xray_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_GetEncryptionConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantContain string
		wantCode    int
	}{
		{
			name:        "default_is_none",
			wantCode:    http.StatusOK,
			wantContain: `"NONE"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayGETRequest(t, h)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
		})
	}
}

func TestHandler_PutEncryptionConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantContain string
		wantCode    int
	}{
		{
			name:        "set_kms_type",
			body:        map[string]any{"Type": "KMS", "KeyId": "arn:aws:kms:us-east-1:123:key/abc"},
			wantCode:    http.StatusOK,
			wantContain: "KMS",
		},
		{
			name:        "reset_to_none",
			body:        map[string]any{"Type": "NONE"},
			wantCode:    http.StatusOK,
			wantContain: `"NONE"`,
		},
		{
			name:        "empty_body_defaults_to_none",
			body:        map[string]any{},
			wantCode:    http.StatusOK,
			wantContain: `"NONE"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/PutEncryptionConfig", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
		})
	}
}

func TestEncryption_KeyIdFormatValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		keyID      string
		wantStatus int
	}{
		{
			name:       "alias format accepted",
			keyID:      "alias/my-key",
			wantStatus: http.StatusOK,
		},
		{
			name:       "ARN format accepted",
			keyID:      "arn:aws:kms:us-east-1:123456789012:key/abc-123",
			wantStatus: http.StatusOK,
		},
		{
			name:       "UUID format accepted",
			keyID:      "12345678-1234-1234-1234-123456789abc",
			wantStatus: http.StatusOK,
		},
		{
			name:       "random string rejected",
			keyID:      "not-a-valid-key-id",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty rejected for KMS",
			keyID:      "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"Type": "KMS"}

			if tt.keyID != "" {
				body["KeyId"] = tt.keyID
			}

			rec := doXrayRequest(t, h, "/PutEncryptionConfig", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestEncryption_UpdatingStatusAfterPut(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	putRec := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{
		"Type":  "KMS",
		"KeyId": "alias/my-key",
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))

	enc, ok := putResp["EncryptionConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "UPDATING", enc["Status"], "status should be UPDATING immediately after PUT")

	// GET should advance to ACTIVE.
	getRec := doXrayGETRequest(t, h)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	enc2, ok := getResp["EncryptionConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ACTIVE", enc2["Status"], "status should be ACTIVE after first GET")
}

// TestPutEncryptionConfigHandler verifies handler validates type.
func TestPutEncryptionConfigHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{name: "valid NONE", body: map[string]any{"Type": "NONE"}, wantStatus: http.StatusOK},
		{
			name:       "valid KMS",
			body:       map[string]any{"Type": "KMS", "KeyId": "arn:aws:kms:us-east-1:123:key/abc"},
			wantStatus: http.StatusOK,
		},
		{name: "invalid type", body: map[string]any{"Type": "BOGUS"}, wantStatus: http.StatusBadRequest},
		{name: "KMS without key", body: map[string]any{"Type": "KMS"}, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/PutEncryptionConfig", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestGetEncryptionConfig_DefaultIsNONE verifies fresh backend returns Type=NONE.
func TestGetEncryptionConfig_DefaultIsNONE(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doXrayGETRequest(t, h)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	enc, ok := resp["EncryptionConfig"].(map[string]any)
	require.True(t, ok, "EncryptionConfig must be present in response")
	assert.Equal(t, "NONE", enc["Type"], "default encryption type must be NONE")
	assert.Equal(t, "ACTIVE", enc["Status"], "default encryption status must be ACTIVE")
}

// TestPutEncryptionConfig_InvalidType verifies invalid Type is rejected.
func TestPutEncryptionConfig_InvalidType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{
		"Type": "INVALID_TYPE",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "invalid Type must return 400")
}

func TestHandler_PutEncryptionConfig_TypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "NONE type accepted",
			body:       map[string]any{"Type": "NONE"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "KMS type with KeyId accepted",
			body:       map[string]any{"Type": "KMS", "KeyId": "alias/my-key"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty type defaults to NONE",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid type rejected",
			body:       map[string]any{"Type": "INVALID"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "KMS without KeyId rejected",
			body:       map[string]any{"Type": "KMS"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "NONE with KeyId rejected",
			body:       map[string]any{"Type": "NONE", "KeyId": "alias/my-key"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "none (lowercase) rejected",
			body:       map[string]any{"Type": "none"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doXrayRequest(t, h, "/PutEncryptionConfig", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_EncryptionConfig_KMSRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Set KMS encryption
	rec := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{
		"Type":  "KMS",
		"KeyId": "alias/my-xray-key",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Read it back
	rec2 := doXrayRequest(t, h, "/EncryptionConfig", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	cfg, ok := resp["EncryptionConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "KMS", cfg["Type"])
	assert.Equal(t, "alias/my-xray-key", cfg["KeyId"])

	// Revert to NONE
	rec3 := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{"Type": "NONE"})
	require.Equal(t, http.StatusOK, rec3.Code)

	rec4 := doXrayRequest(t, h, "/EncryptionConfig", nil)
	require.Equal(t, http.StatusOK, rec4.Code)

	var resp4 map[string]any
	require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &resp4))
	cfg4, _ := resp4["EncryptionConfig"].(map[string]any)
	assert.Equal(t, "NONE", cfg4["Type"])
}

func TestEncryptionConfig_KMSKeyIdFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		keyID      string
		wantStatus int
	}{
		{name: "alias format", keyID: "alias/my-key", wantStatus: http.StatusOK},
		{name: "key ARN format", keyID: "arn:aws:kms:us-east-1:123456789012:key/abc-123", wantStatus: http.StatusOK},
		{name: "UUID format", keyID: "12345678-1234-1234-1234-123456789abc", wantStatus: http.StatusOK},
		{name: "random string rejected", keyID: "not-a-valid-key", wantStatus: http.StatusBadRequest},
		{name: "empty rejected", keyID: "", wantStatus: http.StatusBadRequest},
		{name: "partial alias rejected", keyID: "alias", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"Type": "KMS"}
			if tt.keyID != "" {
				body["KeyId"] = tt.keyID
			}

			rec := doXrayRequest(t, h, "/PutEncryptionConfig", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestEncryptionConfig_UpdatingThenActiveStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	putRec := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{
		"Type":  "KMS",
		"KeyId": "alias/my-key",
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))

	enc, ok := putResp["EncryptionConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "UPDATING", enc["Status"], "PUT must return UPDATING for KMS")

	getRec := doXrayGETRequest(t, h)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	enc2, ok := getResp["EncryptionConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ACTIVE", enc2["Status"], "first GET after KMS PUT must return ACTIVE")
}

func TestEncryptionConfig_NoneTypeAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{"Type": "NONE"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	enc, ok := resp["EncryptionConfig"].(map[string]any)
	require.True(t, ok)
	// NONE type should be ACTIVE immediately.
	assert.Equal(t, "ACTIVE", enc["Status"])
}

func TestEncryptionConfig_InvalidTypeRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{"Type": "BOGUS"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

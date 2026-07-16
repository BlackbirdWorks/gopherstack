package iam_test

import (
	"encoding/xml"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAccessKey_Handler tests the UpdateAccessKey and GetAccessKeyLastUsed handlers.
func TestAccessKey_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*iam.InMemoryBackend) string
		params      map[string]string
		name        string
		action      string
		wantContain string
		wantCode    int
	}{
		{
			name:   "UpdateAccessKey_inactive",
			action: "UpdateAccessKey",
			setup: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateUser("alice", "/", "")
				ak, _ := b.CreateAccessKey("alice")

				return ak.AccessKeyID
			},
			params: map[string]string{
				"UserName": "alice",
				"Status":   "Inactive",
			},
			wantCode:    http.StatusOK,
			wantContain: "UpdateAccessKeyResponse",
		},
		{
			name:   "GetAccessKeyLastUsed",
			action: "GetAccessKeyLastUsed",
			setup: func(b *iam.InMemoryBackend) string {
				_, _ = b.CreateUser("alice", "/", "")
				ak, _ := b.CreateAccessKey("alice")

				return ak.AccessKeyID
			},
			wantCode:    http.StatusOK,
			wantContain: "GetAccessKeyLastUsedResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			keyID := tt.setup(b)
			h := iam.NewHandler(b)

			params := make(map[string]string)
			maps.Copy(params, tt.params)

			params["AccessKeyId"] = keyID

			e := echo.New()
			req := iamRequest(tt.action, params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
		})
	}
}

func TestHandler_GetAccessKeyLastUsed_NeverUsed(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("aklu-user", "/", "")

	ak, err := b.CreateAccessKey("aklu-user")
	require.NoError(t, err)

	req := iamRequest("GetAccessKeyLastUsed", map[string]string{
		"AccessKeyId": ak.AccessKeyID,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "GetAccessKeyLastUsedResult")
	assert.Contains(t, body, "aklu-user")
	assert.Contains(t, body, "N/A", "never-used key must show N/A last used")
}

func TestHandler_GetAccessKeyLastUsed_AfterUsage(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("aklu-used-user", "/", "")

	ak, err := b.CreateAccessKey("aklu-used-user")
	require.NoError(t, err)

	b.RecordAccessKeyUsage(ak.AccessKeyID, "us-west-2", "s3")

	req := iamRequest("GetAccessKeyLastUsed", map[string]string{
		"AccessKeyId": ak.AccessKeyID,
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "us-west-2")
	assert.Contains(t, body, "s3")
}

func TestHandler_AccessKey_RotationUpdatesStatus(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("rotation-user", "/", "")

	ak, err := b.CreateAccessKey("rotation-user")
	require.NoError(t, err)

	// Deactivate.
	req := iamRequest("UpdateAccessKey", map[string]string{
		"UserName":    "rotation-user",
		"AccessKeyId": ak.AccessKeyID,
		"Status":      "Inactive",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// List and verify status.
	req2 := iamRequest("ListAccessKeys", map[string]string{"UserName": "rotation-user"})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "Inactive")
}

func TestHandler_AccessKey_RotationCycle(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")

	// Create first key.
	req := iamRequest("CreateAccessKey", map[string]string{"UserName": "alice"})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp iam.CreateAccessKeyResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &createResp))
	keyID := createResp.CreateAccessKeyResult.AccessKey.AccessKeyID
	assert.True(t, strings.HasPrefix(keyID, "AKIA"))
	assert.Len(t, keyID, 20)
	assert.Equal(t, "Active", createResp.CreateAccessKeyResult.AccessKey.Status)

	// Deactivate via UpdateAccessKey.
	req2 := iamRequest("UpdateAccessKey", map[string]string{
		"UserName":    "alice",
		"AccessKeyId": keyID,
		"Status":      "Inactive",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List to confirm Inactive.
	req3 := iamRequest("ListAccessKeys", map[string]string{"UserName": "alice"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Contains(t, rec3.Body.String(), "Inactive")

	// GetAccessKeyLastUsed.
	req4 := iamRequest("GetAccessKeyLastUsed", map[string]string{"AccessKeyId": keyID})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)
	assert.Contains(t, rec4.Body.String(), "alice", "response must include username")

	// Delete.
	req5 := iamRequest("DeleteAccessKey", map[string]string{
		"UserName":    "alice",
		"AccessKeyId": keyID,
	})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	assert.Equal(t, http.StatusOK, rec5.Code)
}

func TestHandler_AccessKey_MaxTwoLimit_Via_Handler(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("alice", "/", "")

	for range 2 {
		req := iamRequest("CreateAccessKey", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(e.NewContext(req, rec)))
		require.Equal(t, http.StatusOK, rec.Code, "first two keys must succeed")
	}

	req := iamRequest("CreateAccessKey", map[string]string{"UserName": "alice"})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	// Real AWS IAM returns 409 for LimitExceeded, not 400.
	assert.Equal(t, http.StatusConflict, rec.Code, "third key must fail")

	var errResp iam.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "LimitExceeded", errResp.Error.Code)
}

func TestHandler_AccessKey_IDFormat(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	_, _ = b.CreateUser("bob", "/", "")

	req := iamRequest("CreateAccessKey", map[string]string{"UserName": "bob"})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp iam.CreateAccessKeyResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	keyID := resp.CreateAccessKeyResult.AccessKey.AccessKeyID

	assert.Len(t, keyID, 20, "access key ID must be 20 chars")
	assert.True(t, strings.HasPrefix(keyID, "AKIA"), "must start with AKIA")
	assert.NotContains(t, keyID, "-", "must not contain dashes")
	for _, ch := range keyID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"char %q must be uppercase alphanumeric", ch)
	}
}

func TestIAMHandler_AccessKeys(t *testing.T) {
	t.Parallel()

	t.Run("CreateAccessKey", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/", "")

		req := iamRequest("CreateAccessKey", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.CreateAccessKeyResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "alice", resp.CreateAccessKeyResult.AccessKey.UserName)
		assert.Equal(t, "Active", resp.CreateAccessKeyResult.AccessKey.Status)
	})

	t.Run("DeleteAccessKey", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/", "")
		ak, _ := b.CreateAccessKey("alice")

		req := iamRequest("DeleteAccessKey", map[string]string{
			"UserName":    "alice",
			"AccessKeyId": ak.AccessKeyID,
		})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListAccessKeys", func(t *testing.T) {
		t.Parallel()
		e := echo.New()
		h, b := newTestHandler(t)
		_, _ = b.CreateUser("alice", "/", "")
		_, _ = b.CreateAccessKey("alice")

		req := iamRequest("ListAccessKeys", map[string]string{"UserName": "alice"})
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)

		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp iam.ListAccessKeysResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.ListAccessKeysResult.AccessKeyMetadata, 1)
	})
}

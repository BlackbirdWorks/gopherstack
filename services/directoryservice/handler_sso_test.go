package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

func TestDirectoryService_SSO(t *testing.T) {
	t.Parallel()

	createDir := func(h *directoryservice.Handler) string {
		rec := doRequest(t, h, "CreateDirectory", map[string]any{
			"Name": "corp.example.com", "Password": "Admin1234!", "Size": "Small",
		})
		var resp map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)

		return resp["DirectoryId"].(string)
	}

	t.Run("enable SSO succeeds", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)
		rec := doRequest(t, h, "EnableSso", map[string]any{"DirectoryId": dirID})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("disable SSO succeeds", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		dirID := createDir(h)
		doRequest(t, h, "EnableSso", map[string]any{"DirectoryId": dirID})
		rec := doRequest(t, h, "DisableSso", map[string]any{"DirectoryId": dirID})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("EnableSso unknown directory returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "EnableSso", map[string]any{"DirectoryId": "d-0000000000"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("DisableSso unknown directory returns 400", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		rec := doRequest(t, h, "DisableSso", map[string]any{"DirectoryId": "d-0000000000"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

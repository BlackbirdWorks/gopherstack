package amplify_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_WebhookCRUD verifies create/get/list/update/delete webhook lifecycle.
func TestHandler_WebhookCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "full_webhook_lifecycle", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "WebhookApp")
			seedMainBranch(t, b, app.AppID)

			// Create.
			rec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/webhooks",
				map[string]any{"branchName": "main", "description": "test hook"})
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			wh := createResp["webhook"].(map[string]any)
			webhookID := wh["webhookId"].(string)
			assert.NotEmpty(t, webhookID)

			// Get.
			rec = doRequest(t, h, http.MethodGet, "/webhooks/"+webhookID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// List.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/webhooks", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp["webhooks"].([]any), 1)

			// Update.
			rec = doRequest(t, h, http.MethodPost, "/webhooks/"+webhookID,
				map[string]any{"branchName": "main", "description": "updated"})
			require.Equal(t, tt.wantStatus, rec.Code)

			var updateResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
			updated := updateResp["webhook"].(map[string]any)
			assert.Equal(t, "updated", updated["description"])

			// Delete.
			rec = doRequest(t, h, http.MethodDelete, "/webhooks/"+webhookID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// Confirm gone.
			rec = doRequest(t, h, http.MethodGet, "/webhooks/"+webhookID, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

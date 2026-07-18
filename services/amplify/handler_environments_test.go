package amplify_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_BackendEnvironmentCRUD verifies backend environment lifecycle via the HTTP handler.
func TestHandler_BackendEnvironmentCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "full_backend_env_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "BEApp")

			// Create.
			rec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/backendenvironments",
				map[string]any{"environmentName": "prod", "stackName": "MyStack"})
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			be := createResp["backendEnvironment"].(map[string]any)
			assert.Equal(t, "prod", be["environmentName"])

			// Get.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/backendenvironments/prod", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// List.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/backendenvironments", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			assert.Len(t, listResp["backendEnvironments"].([]any), 1)

			// Delete.
			rec = doRequest(t, h, http.MethodDelete, "/apps/"+app.AppID+"/backendenvironments/prod", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			// Confirm gone.
			rec = doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/backendenvironments/prod", nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

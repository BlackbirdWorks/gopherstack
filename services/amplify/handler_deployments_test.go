package amplify_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_DeploymentOps verifies CreateDeployment and StartDeployment via the HTTP handler.
func TestHandler_DeploymentOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create_and_start_deployment"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "DeployApp")
			seedMainBranch(t, b, app.AppID)

			basePath := "/apps/" + app.AppID + "/branches/main/deployments"

			// Create deployment.
			rec := doRequest(t, h, http.MethodPost, basePath, nil)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			jobID := createResp["jobId"].(string)
			assert.NotEmpty(t, jobID)
			assert.NotNil(t, createResp["fileUploadUrls"])

			// Start deployment.
			rec = doRequest(t, h, http.MethodPost, basePath+"/start",
				map[string]any{"jobId": jobID})
			require.Equal(t, http.StatusOK, rec.Code)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			assert.NotNil(t, startResp["jobSummary"])
		})
	}
}

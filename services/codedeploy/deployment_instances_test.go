package codedeploy_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// TestDeploymentInstances_BatchGetMissingDeployment verifies that
// BatchGetDeploymentInstances surfaces a missing deployment as a 404
// DeploymentDoesNotExistException.
func TestDeploymentInstances_BatchGetMissingDeployment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "BatchGetDeploymentInstances", map[string]any{
		"deploymentId": "d-NOTEXIST1",
		"instanceIds":  []string{"i-abc"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "DeploymentDoesNotExistException", resp["__type"])
}

func TestHandler_BatchGetDeploymentInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler) string
		input      func(deployID string) map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) string {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
				_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)
				d, _ := createDeploy(h.Backend, "my-app", "my-dg", "", "")

				return d.DeploymentID
			},
			input: func(deployID string) map[string]any {
				return map[string]any{
					"deploymentId": deployID,
					"instanceIds":  []string{"i-abc123", "i-def456"},
				}
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:  "missing_deployment_id",
			setup: func(_ *codedeploy.Handler) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{"instanceIds": []string{"i-abc"}}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			deployID := tt.setup(h)

			rec := doRequest(t, h, "BatchGetDeploymentInstances", tt.input(deployID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				summaries, ok := resp["instancesSummary"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, tt.wantCount)
			}
		})
	}
}

func TestHandler_BatchGetDeploymentTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler) string
		input      func(deployID string) map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) string {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
				_, _ = createDG(h.Backend, "my-app", "my-dg", "", "", nil)
				d, _ := createDeploy(h.Backend, "my-app", "my-dg", "", "")

				return d.DeploymentID
			},
			input: func(deployID string) map[string]any {
				return map[string]any{
					"deploymentId": deployID,
					"targetIds":    []string{"target-1", "target-2"},
				}
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:  "missing_deployment_id",
			setup: func(_ *codedeploy.Handler) string { return "" },
			input: func(_ string) map[string]any {
				return map[string]any{"targetIds": []string{"t-1"}}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:  "deployment_not_found",
			setup: func(_ *codedeploy.Handler) string { return "d-nonexistent" },
			input: func(deployID string) map[string]any {
				return map[string]any{"deploymentId": deployID, "targetIds": []string{"t-1"}}
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			deployID := tt.setup(h)

			rec := doRequest(t, h, "BatchGetDeploymentTargets", tt.input(deployID))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				targets, ok := resp["deploymentTargets"].([]any)
				require.True(t, ok)
				assert.Len(t, targets, tt.wantCount)
			}
		})
	}
}

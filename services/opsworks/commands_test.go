package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestDescribeCommands_CorrectFieldName verifies CommandIds (not CommandIDs) works.
func TestDescribeCommands_CorrectFieldName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "DescribeCommands with CommandIds field filters correctly",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "CreateApp", map[string]any{
					"StackId": stackID,
					"Name":    "app",
					"Type":    "other",
				})
				appID := parseJSON(t, rec.Body.Bytes())["AppId"].(string)

				rec = doTarget(t, h, "CreateDeployment", map[string]any{
					"StackId": stackID,
					"AppId":   appID,
					"Command": map[string]any{"Name": "deploy"},
				})
				deploymentID := parseJSON(t, rec.Body.Bytes())["DeploymentId"].(string)

				// Get commands for the deployment to find a command ID.
				rec = doTarget(t, h, "DescribeCommands", map[string]any{
					"DeploymentId": deploymentID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				commands := parseJSON(t, rec.Body.Bytes())["Commands"].([]any)
				require.Len(t, commands, 1)
				cmdID := commands[0].(map[string]any)["CommandId"].(string)

				// Now filter by CommandIds (correct AWS field name).
				rec = doTarget(t, h, "DescribeCommands", map[string]any{
					"CommandIds": []string{cmdID},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				filtered := parseJSON(t, rec.Body.Bytes())["Commands"].([]any)
				require.Len(t, filtered, 1)
				assert.Equal(t, cmdID, filtered[0].(map[string]any)["CommandId"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}

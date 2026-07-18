package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestDeployment verifies deployment and command operations.
func TestDeployment(t *testing.T) {
	t.Parallel()

	createStackAndApp := func(h *opsworks.Handler) (string, string) {
		rec := doTarget(t, h, "CreateStack", map[string]any{
			"Name":                      "stack",
			"Region":                    "us-east-1",
			"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
			"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		stackID := parseJSON(t, rec.Body.Bytes())["StackId"].(string)

		rec = doTarget(t, h, "CreateApp", map[string]any{
			"StackId": stackID,
			"Name":    "app",
			"Type":    "other",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		appID := parseJSON(t, rec.Body.Bytes())["AppId"].(string)

		return stackID, appID
	}

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler, stackID, appID string)
		name  string
	}{
		{
			name: "CreateDeployment returns DeploymentId",
			check: func(t *testing.T, h *opsworks.Handler, stackID, appID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateDeployment", map[string]any{
					"StackId": stackID,
					"AppId":   appID,
					"Command": map[string]any{"Name": "deploy"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.NotEmpty(t, resp["DeploymentId"])
			},
		},
		{
			name: "DescribeDeployments returns deployment with status and timestamps",
			check: func(t *testing.T, h *opsworks.Handler, stackID, appID string) {
				t.Helper()
				doTarget(t, h, "CreateDeployment", map[string]any{
					"StackId": stackID,
					"AppId":   appID,
					"Command": map[string]any{"Name": "deploy"},
				})
				rec := doTarget(t, h, "DescribeDeployments", map[string]any{"StackId": stackID})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				deployments, ok := resp["Deployments"].([]any)
				require.True(t, ok)
				require.Len(t, deployments, 1)
				d := deployments[0].(map[string]any)
				assert.NotEmpty(t, d["DeploymentId"])
				assert.NotEmpty(t, d["Status"])
				assert.NotEmpty(t, d["CreatedAt"])
				assert.NotEmpty(t, d["CompletedAt"])
				assert.NotEmpty(t, d["Command"])
			},
		},
		{
			name: "DescribeCommands returns commands for deployment",
			check: func(t *testing.T, h *opsworks.Handler, stackID, appID string) {
				t.Helper()
				rec := doTarget(t, h, "CreateDeployment", map[string]any{
					"StackId": stackID,
					"AppId":   appID,
					"Command": map[string]any{"Name": "deploy"},
				})
				deploymentID := parseJSON(t, rec.Body.Bytes())["DeploymentId"].(string)

				rec = doTarget(t, h, "DescribeCommands", map[string]any{
					"DeploymentId": deploymentID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				commands, ok := resp["Commands"].([]any)
				require.True(t, ok)
				require.NotEmpty(t, commands)
				cmd := commands[0].(map[string]any)
				assert.NotEmpty(t, cmd["CommandId"])
				assert.NotEmpty(t, cmd["Status"])
				assert.NotEmpty(t, cmd["CreatedAt"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			stackID, appID := createStackAndApp(h)
			tt.check(t, h, stackID, appID)
		})
	}
}

// TestDeploymentCompletedAt verifies CompletedAt differs from CreatedAt.
func TestDeploymentCompletedAt(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	stackID := createTestStack(t, h)
	rec := doTarget(t, h, "CreateApp", map[string]any{
		"StackId": stackID, "Name": "app", "Type": "other",
	})
	appID := parseJSON(t, rec.Body.Bytes())["AppId"].(string)

	rec = doTarget(t, h, "CreateDeployment", map[string]any{
		"StackId": stackID,
		"AppId":   appID,
		"Command": map[string]any{"Name": "deploy"},
	})
	deploymentID := parseJSON(t, rec.Body.Bytes())["DeploymentId"].(string)

	rec = doTarget(t, h, "DescribeDeployments", map[string]any{
		"DeploymentIds": []string{deploymentID},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	d := parseJSON(t, rec.Body.Bytes())["Deployments"].([]any)[0].(map[string]any)
	assert.NotEmpty(t, d["CompletedAt"])
	assert.NotEmpty(t, d["CreatedAt"])
	// CompletedAt should differ from CreatedAt (not set to creation instant).
	assert.NotEqual(t, d["CreatedAt"], d["CompletedAt"])
}

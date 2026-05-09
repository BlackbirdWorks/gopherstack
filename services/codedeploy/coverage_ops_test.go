package codedeploy_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

func createCDApp(t *testing.T, h *codedeploy.Handler, name string) {
	t.Helper()

	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"applicationName": name,
		"computePlatform": "Server",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestCodeDeploy_UpdateApplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCDApp(t, h, "update-app")

	rec := doRequest(t, h, "UpdateApplication", map[string]any{
		"applicationName": "update-app",
		"newApplicationName": "update-app-v2",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCodeDeploy_DeploymentConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreateDeploymentConfig
	rec := doRequest(t, h, "CreateDeploymentConfig", map[string]any{
		"deploymentConfigName": "my-config",
		"minimumHealthyHosts": map[string]any{
			"type":  "HOST_COUNT",
			"value": 1,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetDeploymentConfig
	rec = doRequest(t, h, "GetDeploymentConfig", map[string]any{
		"deploymentConfigName": "my-config",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListDeploymentConfigs
	rec = doRequest(t, h, "ListDeploymentConfigs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteDeploymentConfig
	rec = doRequest(t, h, "DeleteDeploymentConfig", map[string]any{
		"deploymentConfigName": "my-config",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCodeDeploy_UpdateDeploymentGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCDApp(t, h, "update-dg-app")
	doRequest(t, h, "CreateDeploymentGroup", map[string]any{
		"applicationName":     "update-dg-app",
		"deploymentGroupName": "my-dg",
		"serviceRoleArn":      "arn:aws:iam::123456789012:role/CodeDeployRole",
	})

	rec := doRequest(t, h, "UpdateDeploymentGroup", map[string]any{
		"applicationName":     "update-dg-app",
		"currentDeploymentGroupName": "my-dg",
		"newDeploymentGroupName": "my-dg-v2",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestCodeDeploy_OnPremisesInstances(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// RegisterOnPremisesInstance
	rec := doRequest(t, h, "RegisterOnPremisesInstance", map[string]any{
		"instanceName": "my-instance",
		"iamUserArn":   "arn:aws:iam::123456789012:user/deploy-user",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetOnPremisesInstance
	rec = doRequest(t, h, "GetOnPremisesInstance", map[string]any{
		"instanceName": "my-instance",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListOnPremisesInstances
	rec = doRequest(t, h, "ListOnPremisesInstances", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// RemoveTagsFromOnPremisesInstances
	rec = doRequest(t, h, "RemoveTagsFromOnPremisesInstances", map[string]any{
		"instanceNames": []string{"my-instance"},
		"tags":          []map[string]any{{"Key": "env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeregisterOnPremisesInstance
	rec = doRequest(t, h, "DeregisterOnPremisesInstance", map[string]any{
		"instanceName": "my-instance",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

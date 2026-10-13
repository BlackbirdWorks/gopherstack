package codedeploy_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateDeploymentGroup_InputValidation(t *testing.T) {
	t.Parallel()

	role := "arn:aws:iam::000000000000:role/role"
	tests := []struct {
		body     map[string]any
		name     string
		wantCode string
		wantHTTP int
	}{
		{
			name:     "bad_role",
			body:     map[string]any{"serviceRoleArn": "badarn"},
			wantCode: "InvalidRoleException",
			wantHTTP: 400,
		},
		{
			name:     "unknown_config",
			body:     map[string]any{"serviceRoleArn": role, "deploymentConfigName": "Nope"},
			wantCode: "DeploymentConfigDoesNotExistException", wantHTTP: 404,
		},
		{
			name: "bad_style",
			body: map[string]any{
				"serviceRoleArn":  role,
				"deploymentStyle": map[string]any{"deploymentType": "FOO"},
			},
			wantCode: "InvalidDeploymentStyleException",
			wantHTTP: 400,
		},
		{
			name:     "long_name",
			body:     map[string]any{"serviceRoleArn": role, "deploymentGroupName": strings.Repeat("g", 101)},
			wantCode: "InvalidDeploymentGroupNameException", wantHTTP: 400,
		},
		{name: "valid", body: map[string]any{"serviceRoleArn": role}, wantHTTP: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateApplication", map[string]any{"applicationName": "app", "computePlatform": "Server"})

			tt.body["applicationName"] = "app"
			if _, ok := tt.body["deploymentGroupName"]; !ok {
				tt.body["deploymentGroupName"] = "dg"
			}

			rec := doRequest(t, h, "CreateDeploymentGroup", tt.body)
			require.Equal(t, tt.wantHTTP, rec.Code)

			if tt.wantCode == "" {
				return
			}

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantCode, resp["__type"])
			assert.NotContains(t, resp["message"], "Exception:")
		})
	}
}

func TestDeploymentLifecycle_StopContinueAndCompleted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	waiting := createReadyWaitDeployment(t, h.Backend, "life-app", "life-dg")

	require.NoError(t, h.Backend.ContinueDeployment(waiting))

	rec := doRequest(t, h, "StopDeployment", map[string]any{"deploymentId": waiting})
	require.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeploymentAlreadyCompletedException")

	rec = doRequest(t, h, "GetDeployment", map[string]any{"deploymentId": waiting})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"status":"Succeeded"`)
	assert.Contains(t, rec.Body.String(), `"startTime"`)

	rec = doRequest(t, h, "CreateDeployment", map[string]any{
		"applicationName": "life-app", "deploymentGroupName": "life-dg",
		"revision": map[string]any{"revisionType": "S3"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidRevisionException")
}

package codedeploy_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLifecycleHooks_PutExecutionStatus_DeploymentNotFound verifies that
// PutLifecycleEventHookExecutionStatus validates the deploymentId against the
// backend like its sibling deployment-scoped ops (GetDeploymentInstance,
// ListDeploymentTargets, etc.) instead of unconditionally succeeding for an
// unknown deployment.
func TestLifecycleHooks_PutExecutionStatus_DeploymentNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "PutLifecycleEventHookExecutionStatus", map[string]any{
		"deploymentId":                  "d-NOTFOUND1",
		"lifecycleEventHookExecutionId": "hook-1",
		"status":                        "Succeeded",
	})
	require.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "DeploymentDoesNotExistException", resp["__type"])
}

// TestLifecycleHooks_PutExecutionStatus_DeploymentFound verifies the happy
// path still succeeds once the deployment exists, guarding against an
// over-eager existence check breaking the normal flow.
func TestLifecycleHooks_PutExecutionStatus_DeploymentFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createAppAndDG(t, h, "hook-app", "hook-dg")

	createRec := doRequest(t, h, "CreateDeployment", map[string]any{
		"applicationName":     "hook-app",
		"deploymentGroupName": "hook-dg",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	deployID := createOut["deploymentId"]

	hookRec := doRequest(t, h, "PutLifecycleEventHookExecutionStatus", map[string]any{
		"deploymentId":                  deployID,
		"lifecycleEventHookExecutionId": "hook-1",
		"status":                        "Succeeded",
	})
	require.Equal(t, http.StatusOK, hookRec.Code)

	var hookOut struct {
		LifecycleEventHookExecutionID string `json:"lifecycleEventHookExecutionId"`
	}
	require.NoError(t, json.Unmarshal(hookRec.Body.Bytes(), &hookOut))
	assert.Equal(t, "hook-1", hookOut.LifecycleEventHookExecutionID)
}

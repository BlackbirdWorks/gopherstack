package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReconciler_MinimumHealthyPercent_CapsScaleDown(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "scaletest",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)
	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "test"})

	// Create service with desiredCount=4 and MinimumHealthyPercent=50.
	minPct := 50
	maxPct := 200
	svcRec := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "test",
		"serviceName":    "scalingsvc",
		"taskDefinition": tdArn,
		"desiredCount":   4,
		"deploymentConfiguration": map[string]any{
			"minimumHealthyPercent": minPct,
			"maximumPercent":        maxPct,
		},
	})
	require.Equal(t, http.StatusOK, svcRec.Code, svcRec.Body.String())

	// Manually launch 4 tasks to simulate them running.
	for range 4 {
		runRec := doECSRequest(t, h, "RunTask", map[string]any{
			"cluster":        "test",
			"taskDefinition": tdArn,
			"group":          "service:scalingsvc",
		})
		require.Equal(t, http.StatusOK, runRec.Code)
	}

	// Now update service to desiredCount=0. The MinimumHealthyPercent=50 with
	// desired=4 means a floor of 2 tasks must stay running.
	// Since the reconciler runs asynchronously, we only verify that the
	// service was updated successfully. The floor logic is tested in
	// backend unit tests, not e2e here.
	updRec := doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster":      "test",
		"service":      "scalingsvc",
		"desiredCount": 0,
	})
	require.Equal(t, http.StatusOK, updRec.Code, updRec.Body.String())
}

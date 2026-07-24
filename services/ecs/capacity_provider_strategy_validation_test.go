package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCapacityProviderStrategy_RejectsUnknownProvider proves that every
// operation accepting a capacityProviderStrategy rejects a strategy item
// referencing a capacity provider that was never created (and isn't a
// FARGATE/FARGATE_SPOT builtin) with a 400 ClientException, matching real
// AWS validation. Previously these ops silently accepted any string as a
// capacity provider name.
func TestCapacityProviderStrategy_RejectsUnknownProvider(t *testing.T) {
	t.Parallel()

	badStrategy := []any{
		map[string]any{"capacityProvider": "does-not-exist", "weight": 1},
	}

	t.Run("CreateCluster", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		resp := doECSRequest(t, h, "CreateCluster", map[string]any{
			"clusterName":                     "bad-cps-cluster",
			"defaultCapacityProviderStrategy": badStrategy,
		})
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("UpdateCluster", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "upd-bad-cps-cluster"})

		resp := doECSRequest(t, h, "UpdateCluster", map[string]any{
			"cluster":                         "upd-bad-cps-cluster",
			"defaultCapacityProviderStrategy": badStrategy,
		})
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("PutClusterCapacityProviders", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "pcps-bad-cluster"})

		resp := doECSRequest(t, h, "PutClusterCapacityProviders", map[string]any{
			"cluster":                         "pcps-bad-cluster",
			"defaultCapacityProviderStrategy": badStrategy,
		})
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("CreateService", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "bad-cps-svc-cluster"})
		doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "bad-cps-svc-task",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})

		resp := doECSRequest(t, h, "CreateService", map[string]any{
			"cluster":                  "bad-cps-svc-cluster",
			"serviceName":              "bad-cps-svc",
			"taskDefinition":           "bad-cps-svc-task",
			"desiredCount":             1,
			"capacityProviderStrategy": badStrategy,
		})
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("UpdateService", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "upd-bad-cps-svc-cluster"})
		doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "upd-bad-cps-svc-task",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})
		doECSRequest(t, h, "CreateService", map[string]any{
			"cluster":        "upd-bad-cps-svc-cluster",
			"serviceName":    "upd-bad-cps-svc",
			"taskDefinition": "upd-bad-cps-svc-task",
			"desiredCount":   1,
		})

		resp := doECSRequest(t, h, "UpdateService", map[string]any{
			"cluster":                  "upd-bad-cps-svc-cluster",
			"service":                  "upd-bad-cps-svc",
			"capacityProviderStrategy": badStrategy,
		})
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("RunTask", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "bad-cps-run-cluster"})
		doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "bad-cps-run-task",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})

		resp := doECSRequest(t, h, "RunTask", map[string]any{
			"cluster":                  "bad-cps-run-cluster",
			"taskDefinition":           "bad-cps-run-task",
			"capacityProviderStrategy": badStrategy,
		})
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("CreateTaskSet", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "bad-cps-ts-cluster"})
		doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "bad-cps-ts-task",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})
		doECSRequest(t, h, "CreateService", map[string]any{
			"cluster":        "bad-cps-ts-cluster",
			"serviceName":    "bad-cps-ts-svc",
			"taskDefinition": "bad-cps-ts-task",
			"desiredCount":   1,
			"deploymentController": map[string]any{
				"type": "EXTERNAL",
			},
		})

		resp := doECSRequest(t, h, "CreateTaskSet", map[string]any{
			"cluster":                  "bad-cps-ts-cluster",
			"service":                  "bad-cps-ts-svc",
			"taskDefinition":           "bad-cps-ts-task",
			"capacityProviderStrategy": badStrategy,
		})
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

// TestCapacityProviderStrategy_AcceptsCreatedProvider proves that a
// capacityProviderStrategy referencing a provider created via
// CreateCapacityProvider (not just the FARGATE/FARGATE_SPOT builtins) is
// accepted.
func TestCapacityProviderStrategy_AcceptsCreatedProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	resp := doECSRequest(t, h, "CreateCapacityProvider", map[string]any{
		"name": "real-cp",
		"autoScalingGroupProvider": map[string]any{
			"autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:real-asg",
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	resp = doECSRequest(t, h, "CreateCluster", map[string]any{
		"clusterName": "real-cp-cluster",
		"defaultCapacityProviderStrategy": []any{
			map[string]any{"capacityProvider": "real-cp", "weight": 1},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	cluster := out["cluster"].(map[string]any)
	strategy := cluster["defaultCapacityProviderStrategy"].([]any)
	require.Len(t, strategy, 1)
	assert.Equal(t, "real-cp", strategy[0].(map[string]any)["capacityProvider"])
}

// TestRunTask_CapacityProviderStrategy_SetsCapacityProviderName proves that
// RunTask accepts a capacityProviderStrategy (previously entirely absent
// from RunTaskInput -- there was no way to pass one at all) and that the
// resulting task reports the selected provider via capacityProviderName,
// matching the real ecs.Task wire shape.
func TestRunTask_CapacityProviderStrategy_SetsCapacityProviderName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "run-cps-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "run-cps-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":        "run-cps-cluster",
		"taskDefinition": "run-cps-task",
		"capacityProviderStrategy": []any{
			map[string]any{"capacityProvider": "FARGATE_SPOT", "weight": 1},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	tasks := out["tasks"].([]any)
	require.Len(t, tasks, 1)
	task := tasks[0].(map[string]any)
	assert.Equal(t, "FARGATE_SPOT", task["capacityProviderName"])
}

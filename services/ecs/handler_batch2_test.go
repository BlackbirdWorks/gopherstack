package ecs_test

// handler_batch2_test.go — ECS AWS-accuracy audit batch-2
// Covers: TaskSet blue/green (EXTERNAL controller), CapacityProvider ASG-backed,
// CapacityProviderStrategy on service, circuit breaker + rollback, ServiceConnect,
// ServiceDiscovery/CloudMap, account settings edge cases, attribute CRUD edge cases,
// ClusterSetting containerInsights, ECSExec edge cases, container instance DRAINING,
// TaskDefinition revisions + deregister, PrimaryTaskSet, tags, concurrent safety.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// ---- helpers ----

func newBatch2Handler(t *testing.T) *ecs.Handler {
	t.Helper()
	backend := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())

	return ecs.NewHandler(backend)
}

// ================================================================
// TaskSet (blue/green via EXTERNAL deployment controller)
// ================================================================

func TestBatch2_TaskSet_ExternalController_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ts-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ts-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	// Create service with EXTERNAL deployment controller (required for task sets)
	svcResp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "ts-cluster",
		"serviceName":          "ts-svc",
		"taskDefinition":       "ts-task",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "EXTERNAL"},
	})
	require.Equal(t, http.StatusOK, svcResp.Code)

	// Create a task set
	createResp := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "ts-cluster",
		"service":        "ts-svc",
		"taskDefinition": "ts-task",
		"scale":          map[string]any{"unit": "PERCENT", "value": 100},
	})
	require.Equal(t, http.StatusOK, createResp.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createOut))
	taskSet := createOut["taskSet"].(map[string]any)
	taskSetArn := taskSet["taskSetArn"].(string)
	require.NotEmpty(t, taskSetArn)
	assert.Contains(t, taskSet["clusterArn"], "ts-cluster")
	assert.Equal(t, "ACTIVE", taskSet["status"])

	// Describe task sets
	describeResp := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
		"cluster":  "ts-cluster",
		"service":  "ts-svc",
		"taskSets": []string{taskSetArn},
	})
	require.Equal(t, http.StatusOK, describeResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(describeResp.Body.Bytes(), &descOut))
	taskSets := descOut["taskSets"].([]any)
	require.Len(t, taskSets, 1)

	// Delete task set
	deleteResp := doECSRequest(t, h, "DeleteTaskSet", map[string]any{
		"cluster": "ts-cluster",
		"service": "ts-svc",
		"taskSet": taskSetArn,
	})
	require.Equal(t, http.StatusOK, deleteResp.Code)
}

func TestBatch2_TaskSet_UpdateScale(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ts-scale-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ts-scale-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "ts-scale-cluster",
		"serviceName":          "ts-scale-svc",
		"taskDefinition":       "ts-scale-task",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "EXTERNAL"},
	})

	createResp := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "ts-scale-cluster",
		"service":        "ts-scale-svc",
		"taskDefinition": "ts-scale-task",
		"scale":          map[string]any{"unit": "PERCENT", "value": 50},
	})
	require.Equal(t, http.StatusOK, createResp.Code)
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createOut))
	taskSetArn := createOut["taskSet"].(map[string]any)["taskSetArn"].(string)

	// Update scale to 100%
	updateResp := doECSRequest(t, h, "UpdateTaskSet", map[string]any{
		"cluster": "ts-scale-cluster",
		"service": "ts-scale-svc",
		"taskSet": taskSetArn,
		"scale":   map[string]any{"unit": "PERCENT", "value": 100},
	})
	require.Equal(t, http.StatusOK, updateResp.Code)
	var updateOut map[string]any
	require.NoError(t, json.Unmarshal(updateResp.Body.Bytes(), &updateOut))
	updatedScale := updateOut["taskSet"].(map[string]any)["scale"].(map[string]any)
	assert.InDelta(t, float64(100), updatedScale["value"], 0.001)
}

func TestBatch2_TaskSet_UpdateServicePrimaryTaskSet(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "primary-ts-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "primary-ts-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "primary-ts-cluster",
		"serviceName":          "primary-ts-svc",
		"taskDefinition":       "primary-ts-task",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "EXTERNAL"},
	})

	// Create two task sets
	resp1 := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "primary-ts-cluster",
		"service":        "primary-ts-svc",
		"taskDefinition": "primary-ts-task",
		"scale":          map[string]any{"unit": "PERCENT", "value": 50},
	})
	require.Equal(t, http.StatusOK, resp1.Code)
	var out1 map[string]any
	require.NoError(t, json.Unmarshal(resp1.Body.Bytes(), &out1))
	taskSet1Arn := out1["taskSet"].(map[string]any)["taskSetArn"].(string)

	resp2 := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "primary-ts-cluster",
		"service":        "primary-ts-svc",
		"taskDefinition": "primary-ts-task",
		"scale":          map[string]any{"unit": "PERCENT", "value": 50},
	})
	require.Equal(t, http.StatusOK, resp2.Code)
	var out2 map[string]any
	require.NoError(t, json.Unmarshal(resp2.Body.Bytes(), &out2))
	taskSet2Arn := out2["taskSet"].(map[string]any)["taskSetArn"].(string)

	// Update primary task set to ts1
	primaryResp := doECSRequest(t, h, "UpdateServicePrimaryTaskSet", map[string]any{
		"cluster":        "primary-ts-cluster",
		"service":        "primary-ts-svc",
		"primaryTaskSet": taskSet1Arn,
	})
	require.Equal(t, http.StatusOK, primaryResp.Code)

	// Verify ts1 is primary
	descResp := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
		"cluster":  "primary-ts-cluster",
		"service":  "primary-ts-svc",
		"taskSets": []string{taskSet1Arn},
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	ts := descOut["taskSets"].([]any)[0].(map[string]any)
	assert.Equal(t, "PRIMARY", ts["status"])

	// Switch primary to ts2
	primaryResp2 := doECSRequest(t, h, "UpdateServicePrimaryTaskSet", map[string]any{
		"cluster":        "primary-ts-cluster",
		"service":        "primary-ts-svc",
		"primaryTaskSet": taskSet2Arn,
	})
	require.Equal(t, http.StatusOK, primaryResp2.Code)

	// Now ts1 should revert to ACTIVE
	descResp2 := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
		"cluster":  "primary-ts-cluster",
		"service":  "primary-ts-svc",
		"taskSets": []string{taskSet1Arn},
	})
	require.Equal(t, http.StatusOK, descResp2.Code)
	var descOut2 map[string]any
	require.NoError(t, json.Unmarshal(descResp2.Body.Bytes(), &descOut2))
	ts1After := descOut2["taskSets"].([]any)[0].(map[string]any)
	assert.NotEqual(t, "PRIMARY", ts1After["status"])
}

func TestBatch2_TaskSet_DescribeAll_NoFilter(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ts-all-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ts-all-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "ts-all-cluster",
		"serviceName":          "ts-all-svc",
		"taskDefinition":       "ts-all-task",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "EXTERNAL"},
	})

	for range 3 {
		doECSRequest(t, h, "CreateTaskSet", map[string]any{
			"cluster":        "ts-all-cluster",
			"service":        "ts-all-svc",
			"taskDefinition": "ts-all-task",
			"scale":          map[string]any{"unit": "PERCENT", "value": 33},
		})
	}

	// Describe without filter returns all
	resp := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
		"cluster":  "ts-all-cluster",
		"service":  "ts-all-svc",
		"taskSets": []string{},
	})
	require.Equal(t, http.StatusOK, resp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	assert.Len(t, out["taskSets"].([]any), 3)
}

func TestBatch2_DeleteCluster_CleansTaskSets(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "del-ts-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "del-ts-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "del-ts-cluster",
		"serviceName":          "del-ts-svc",
		"taskDefinition":       "del-ts-task",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "EXTERNAL"},
	})
	doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "del-ts-cluster",
		"service":        "del-ts-svc",
		"taskDefinition": "del-ts-task",
		"scale":          map[string]any{"unit": "PERCENT", "value": 100},
	})

	doECSRequest(t, h, "DeleteService", map[string]any{
		"cluster": "del-ts-cluster",
		"service": "del-ts-svc",
		"force":   true,
	})
	deleteResp := doECSRequest(t, h, "DeleteCluster", map[string]any{
		"cluster": "del-ts-cluster",
	})
	require.Equal(t, http.StatusOK, deleteResp.Code)
}

// ================================================================
// CapacityProvider: ASG-backed (FARGATE/FARGATE_SPOT/ASG)
// ================================================================

func TestBatch2_CapacityProvider_ASGBacked_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	resp := doECSRequest(t, h, "CreateCapacityProvider", map[string]any{
		"name": "asg-cp",
		"autoScalingGroupProvider": map[string]any{
			"autoScalingGroupArn":          "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:asg-1",
			"managedTerminationProtection": "ENABLED",
			"managedDraining":              "ENABLED",
			"managedScaling": map[string]any{
				"status":                 "ENABLED",
				"targetCapacityPercent":  100,
				"minimumScalingStepSize": 1,
				"maximumScalingStepSize": 10,
				"instanceWarmupPeriod":   300,
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	cp := out["capacityProvider"].(map[string]any)
	require.Equal(t, "asg-cp", cp["name"])

	asg := cp["autoScalingGroupProvider"].(map[string]any)
	assert.Equal(
		t,
		"arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:asg-1",
		asg["autoScalingGroupArn"],
	)
	assert.Equal(t, "ENABLED", asg["managedTerminationProtection"])
	assert.Equal(t, "ENABLED", asg["managedDraining"])

	ms := asg["managedScaling"].(map[string]any)
	assert.Equal(t, "ENABLED", ms["status"])
	assert.InDelta(t, float64(100), ms["targetCapacityPercent"], 0.001)
	assert.InDelta(t, float64(1), ms["minimumScalingStepSize"], 0.001)
	assert.InDelta(t, float64(10), ms["maximumScalingStepSize"], 0.001)
	assert.InDelta(t, float64(300), ms["instanceWarmupPeriod"], 0.001)
}

func TestBatch2_CapacityProvider_ASGBacked_NoManagedScaling(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	resp := doECSRequest(t, h, "CreateCapacityProvider", map[string]any{
		"name": "asg-cp-minimal",
		"autoScalingGroupProvider": map[string]any{
			"autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:asg-2",
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	cp := out["capacityProvider"].(map[string]any)
	asg := cp["autoScalingGroupProvider"].(map[string]any)
	assert.Nil(t, asg["managedScaling"])
}

func TestBatch2_CapacityProvider_FARGATE_Types(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	// Create FARGATE-named and FARGATE_SPOT-named capacity providers
	for _, name := range []string{"FARGATE", "FARGATE_SPOT"} {
		resp := doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": name})
		require.Equal(t, http.StatusOK, resp.Code)
	}

	// Describe them by explicit name
	resp := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{
		"capacityProviders": []string{"FARGATE", "FARGATE_SPOT"},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	providers := out["capacityProviders"].([]any)
	require.Len(t, providers, 2)

	names := make([]string, 0, 2)
	for _, p := range providers {
		names = append(names, p.(map[string]any)["name"].(string))
	}
	assert.Contains(t, names, "FARGATE")
	assert.Contains(t, names, "FARGATE_SPOT")
}

func TestBatch2_CapacityProvider_Update_ManagedScaling(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCapacityProvider", map[string]any{
		"name": "asg-update-cp",
		"autoScalingGroupProvider": map[string]any{
			"autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:asg-3",
			"managedScaling": map[string]any{
				"status":                "ENABLED",
				"targetCapacityPercent": 75,
			},
		},
	})

	resp := doECSRequest(t, h, "UpdateCapacityProvider", map[string]any{
		"name": "asg-update-cp",
		"autoScalingGroupProvider": map[string]any{
			"autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:asg-3",
			"managedScaling": map[string]any{
				"status":                "ENABLED",
				"targetCapacityPercent": 90,
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	cp := out["capacityProvider"].(map[string]any)
	asg := cp["autoScalingGroupProvider"].(map[string]any)
	ms := asg["managedScaling"].(map[string]any)
	assert.InDelta(t, float64(90), ms["targetCapacityPercent"], 0.001)
}

func TestBatch2_CapacityProvider_DeleteByARN(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	createResp := doECSRequest(t, h, "CreateCapacityProvider", map[string]any{
		"name": "del-by-arn-cp",
	})
	require.Equal(t, http.StatusOK, createResp.Code)
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createOut))
	cpArn := createOut["capacityProvider"].(map[string]any)["capacityProviderArn"].(string)
	require.NotEmpty(t, cpArn)

	// Delete by ARN
	deleteResp := doECSRequest(t, h, "DeleteCapacityProvider", map[string]any{
		"capacityProvider": cpArn,
	})
	require.Equal(t, http.StatusOK, deleteResp.Code)
}

func TestBatch2_CapacityProvider_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "dup-cp"})
	dupResp := doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "dup-cp"})
	require.Equal(t, http.StatusBadRequest, dupResp.Code)
}

// ================================================================
// CapacityProviderStrategy on Service
// ================================================================

func TestBatch2_Service_CapacityProviderStrategy_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "cps-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "cps-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	// Create service with capacity provider strategy
	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "cps-cluster",
		"serviceName":    "cps-svc",
		"taskDefinition": "cps-task",
		"desiredCount":   1,
		"capacityProviderStrategy": []any{
			map[string]any{"capacityProvider": "FARGATE", "weight": 1, "base": 1},
			map[string]any{"capacityProvider": "FARGATE_SPOT", "weight": 4, "base": 0},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	strategy := svc["capacityProviderStrategy"].([]any)
	require.Len(t, strategy, 2)

	stratMap := make(map[string]any)
	for _, item := range strategy {
		s := item.(map[string]any)
		stratMap[s["capacityProvider"].(string)] = s
	}

	fargate := stratMap["FARGATE"].(map[string]any)
	assert.InDelta(t, float64(1), fargate["weight"], 0.001)
	assert.InDelta(t, float64(1), fargate["base"], 0.001)

	fargateSpot := stratMap["FARGATE_SPOT"].(map[string]any)
	assert.InDelta(t, float64(4), fargateSpot["weight"], 0.001)
}

func TestBatch2_Service_CapacityProviderStrategy_UpdateService(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "cps-upd-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "cps-upd-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "cps-upd-cluster",
		"serviceName":    "cps-upd-svc",
		"taskDefinition": "cps-upd-task",
		"desiredCount":   1,
		"capacityProviderStrategy": []any{
			map[string]any{"capacityProvider": "FARGATE", "weight": 1},
		},
	})

	// Update strategy to add FARGATE_SPOT
	updateResp := doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster": "cps-upd-cluster",
		"service": "cps-upd-svc",
		"capacityProviderStrategy": []any{
			map[string]any{"capacityProvider": "FARGATE", "weight": 1},
			map[string]any{"capacityProvider": "FARGATE_SPOT", "weight": 3},
		},
	})
	require.Equal(t, http.StatusOK, updateResp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(updateResp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	strategy := svc["capacityProviderStrategy"].([]any)
	assert.Len(t, strategy, 2)
}

func TestBatch2_Cluster_PutCapacityProviders_WithStrategy(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "pcps-cluster"})

	resp := doECSRequest(t, h, "PutClusterCapacityProviders", map[string]any{
		"cluster":           "pcps-cluster",
		"capacityProviders": []string{"FARGATE", "FARGATE_SPOT"},
		"defaultCapacityProviderStrategy": []any{
			map[string]any{"capacityProvider": "FARGATE", "weight": 1, "base": 1},
			map[string]any{"capacityProvider": "FARGATE_SPOT", "weight": 4},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	cluster := out["cluster"].(map[string]any)
	providers := cluster["capacityProviders"].([]any)
	assert.Len(t, providers, 2)

	strategy := cluster["defaultCapacityProviderStrategy"].([]any)
	assert.Len(t, strategy, 2)
}

// ================================================================
// Service Deployment Circuit Breaker + Rollback
// ================================================================

func TestBatch2_Service_CircuitBreaker_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "cb-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "cb-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "cb-cluster",
		"serviceName":    "cb-svc",
		"taskDefinition": "cb-task",
		"desiredCount":   1,
		"deploymentConfiguration": map[string]any{
			"deploymentCircuitBreaker": map[string]any{
				"enable":   true,
				"rollback": true,
			},
			"minimumHealthyPercent": 50,
			"maximumPercent":        200,
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentConfiguration"].(map[string]any)

	cb := dc["deploymentCircuitBreaker"].(map[string]any)
	assert.Equal(t, true, cb["enable"])
	assert.Equal(t, true, cb["rollback"])
	assert.InDelta(t, float64(50), dc["minimumHealthyPercent"], 0.001)
	assert.InDelta(t, float64(200), dc["maximumPercent"], 0.001)
}

func TestBatch2_Service_CircuitBreaker_EnabledNoRollback(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "cb2-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "cb2-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "cb2-cluster",
		"serviceName":    "cb2-svc",
		"taskDefinition": "cb2-task",
		"desiredCount":   1,
		"deploymentConfiguration": map[string]any{
			"deploymentCircuitBreaker": map[string]any{
				"enable":   true,
				"rollback": false,
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentConfiguration"].(map[string]any)
	cb := dc["deploymentCircuitBreaker"].(map[string]any)
	assert.Equal(t, true, cb["enable"])
	assert.Equal(t, false, cb["rollback"])
}

func TestBatch2_Service_CircuitBreaker_UpdatePreserved(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "cb3-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "cb3-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "cb3-cluster",
		"serviceName":    "cb3-svc",
		"taskDefinition": "cb3-task",
		"desiredCount":   1,
		"deploymentConfiguration": map[string]any{
			"deploymentCircuitBreaker": map[string]any{
				"enable":   true,
				"rollback": true,
			},
		},
	})

	// Update desired count — circuit breaker should be preserved
	updateResp := doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster":      "cb3-cluster",
		"service":      "cb3-svc",
		"desiredCount": 2,
	})
	require.Equal(t, http.StatusOK, updateResp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(updateResp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentConfiguration"].(map[string]any)
	cb := dc["deploymentCircuitBreaker"].(map[string]any)
	assert.Equal(t, true, cb["enable"])
	assert.Equal(t, true, cb["rollback"])
}

func TestBatch2_Service_DeploymentConfig_Defaults(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dc-default-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "dc-default-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "dc-default-cluster",
		"serviceName":    "dc-default-svc",
		"taskDefinition": "dc-default-task",
		"desiredCount":   1,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentConfiguration"].(map[string]any)
	// AWS defaults: minimumHealthyPercent=100, maximumPercent=200
	assert.InDelta(t, float64(100), dc["minimumHealthyPercent"], 0.001)
	assert.InDelta(t, float64(200), dc["maximumPercent"], 0.001)
}

// ================================================================
// ServiceConnect
// ================================================================

func TestBatch2_ServiceConnect_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "sc-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "sc-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "sc-cluster",
		"serviceName":    "sc-svc",
		"taskDefinition": "sc-task",
		"desiredCount":   1,
		"serviceConnectConfiguration": map[string]any{
			"enabled":   true,
			"namespace": "my-namespace",
			"services": []any{
				map[string]any{
					"portName":      "http",
					"discoveryName": "my-service",
					"clientAliases": []any{
						map[string]any{"dnsName": "my-service.local", "port": 80},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	sc := svc["serviceConnectConfiguration"].(map[string]any)
	assert.Equal(t, true, sc["enabled"])
	assert.Equal(t, "my-namespace", sc["namespace"])

	services := sc["services"].([]any)
	require.Len(t, services, 1)
	service := services[0].(map[string]any)
	assert.Equal(t, "http", service["portName"])
	assert.Equal(t, "my-service", service["discoveryName"])

	aliases := service["clientAliases"].([]any)
	require.Len(t, aliases, 1)
	alias := aliases[0].(map[string]any)
	assert.Equal(t, "my-service.local", alias["dnsName"])
	assert.InDelta(t, float64(80), alias["port"], 0.001)
}

func TestBatch2_ServiceConnect_Disabled(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "sc-dis-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "sc-dis-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "sc-dis-cluster",
		"serviceName":    "sc-dis-svc",
		"taskDefinition": "sc-dis-task",
		"desiredCount":   1,
		"serviceConnectConfiguration": map[string]any{
			"enabled": false,
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	sc := svc["serviceConnectConfiguration"].(map[string]any)
	assert.Equal(t, false, sc["enabled"])
}

func TestBatch2_ServiceConnect_UpdateService(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "sc-upd-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "sc-upd-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "sc-upd-cluster",
		"serviceName":    "sc-upd-svc",
		"taskDefinition": "sc-upd-task",
		"desiredCount":   1,
	})

	updateResp := doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster": "sc-upd-cluster",
		"service": "sc-upd-svc",
		"serviceConnectConfiguration": map[string]any{
			"enabled":   true,
			"namespace": "updated-ns",
		},
	})
	require.Equal(t, http.StatusOK, updateResp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(updateResp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	sc := svc["serviceConnectConfiguration"].(map[string]any)
	assert.Equal(t, true, sc["enabled"])
	assert.Equal(t, "updated-ns", sc["namespace"])
}

// ================================================================
// ServiceDiscovery via CloudMap (ServiceRegistries)
// ================================================================

func TestBatch2_ServiceDiscovery_CloudMap_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "sd-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "sd-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "sd-cluster",
		"serviceName":    "sd-svc",
		"taskDefinition": "sd-task",
		"desiredCount":   1,
		"serviceRegistries": []any{
			map[string]any{
				"registryArn":   "arn:aws:servicediscovery:us-east-1:000000000000:service/srv-xxxx",
				"containerName": "app",
				"containerPort": 8080,
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	registries := svc["serviceRegistries"].([]any)
	require.Len(t, registries, 1)

	reg := registries[0].(map[string]any)
	assert.Equal(
		t,
		"arn:aws:servicediscovery:us-east-1:000000000000:service/srv-xxxx",
		reg["registryArn"],
	)
	assert.Equal(t, "app", reg["containerName"])
	assert.InDelta(t, float64(8080), reg["containerPort"], 0.001)
}

func TestBatch2_ServiceDiscovery_MultipleRegistries(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "sd2-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "sd2-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "sd2-cluster",
		"serviceName":    "sd2-svc",
		"taskDefinition": "sd2-task",
		"desiredCount":   1,
		"serviceRegistries": []any{
			map[string]any{
				"registryArn": "arn:aws:servicediscovery:us-east-1:000000000000:service/srv-aaaa",
				"port":        80,
			},
			map[string]any{
				"registryArn": "arn:aws:servicediscovery:us-east-1:000000000000:service/srv-bbbb",
				"port":        443,
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	registries := svc["serviceRegistries"].([]any)
	assert.Len(t, registries, 2)
}

func TestBatch2_ServiceDiscovery_DescribeServices_Preserved(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "sd3-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "sd3-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "sd3-cluster",
		"serviceName":    "sd3-svc",
		"taskDefinition": "sd3-task",
		"desiredCount":   1,
		"serviceRegistries": []any{
			map[string]any{
				"registryArn": "arn:aws:servicediscovery:us-east-1:000000000000:service/srv-cccc",
			},
		},
	})

	descResp := doECSRequest(t, h, "DescribeServices", map[string]any{
		"cluster":  "sd3-cluster",
		"services": []string{"sd3-svc"},
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &out))
	services := out["services"].([]any)
	require.Len(t, services, 1)
	svc := services[0].(map[string]any)
	registries := svc["serviceRegistries"].([]any)
	assert.Len(t, registries, 1)
	assert.Equal(t, "arn:aws:servicediscovery:us-east-1:000000000000:service/srv-cccc",
		registries[0].(map[string]any)["registryArn"])
}

// ================================================================
// Account Settings edge cases
// ================================================================

func TestBatch2_AccountSettings_AllValidNames(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	validNames := []string{
		"containerInstanceLongArnFormat",
		"serviceLongArnFormat",
		"taskLongArnFormat",
		"awsvpcTrunking",
		"containerInsights",
		"dualStackIPv6",
		"fargateTaskRetirementWaitPeriod",
		"tagResourceAuthorization",
		"guardDutyActivate",
	}

	for _, name := range validNames {
		resp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
			"name":  name,
			"value": "enabled",
		})
		assert.Equal(t, http.StatusOK, resp.Code, "PutAccountSetting %s", name)
	}

	listResp := doECSRequest(t, h, "ListAccountSettings", map[string]any{})
	require.Equal(t, http.StatusOK, listResp.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	settings := listOut["settings"].([]any)
	assert.GreaterOrEqual(t, len(settings), len(validNames))
}

func TestBatch2_AccountSettings_PutDefault_vs_Put(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	// PutAccountSettingDefault sets for all principals
	defResp := doECSRequest(t, h, "PutAccountSettingDefault", map[string]any{
		"name":  "containerInsights",
		"value": "enabled",
	})
	require.Equal(t, http.StatusOK, defResp.Code)

	// PutAccountSetting for a specific principal
	putResp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":         "containerInsights",
		"value":        "disabled",
		"principalArn": "arn:aws:iam::000000000000:user/testuser",
	})
	require.Equal(t, http.StatusOK, putResp.Code)

	// ListAccountSettings with name filter
	listResp := doECSRequest(t, h, "ListAccountSettings", map[string]any{
		"name": "containerInsights",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	settings := listOut["settings"].([]any)
	assert.GreaterOrEqual(t, len(settings), 1)
}

func TestBatch2_AccountSettings_Delete(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":         "awsvpcTrunking",
		"value":        "enabled",
		"principalArn": "arn:aws:iam::000000000000:user/deluser",
	})

	delResp := doECSRequest(t, h, "DeleteAccountSetting", map[string]any{
		"name":         "awsvpcTrunking",
		"principalArn": "arn:aws:iam::000000000000:user/deluser",
	})
	require.Equal(t, http.StatusOK, delResp.Code)

	listResp := doECSRequest(t, h, "ListAccountSettings", map[string]any{
		"name":         "awsvpcTrunking",
		"principalArn": "arn:aws:iam::000000000000:user/deluser",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	settings := listOut["settings"].([]any)
	assert.Empty(t, settings)
}

func TestBatch2_AccountSettings_OverwriteExisting(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "tagResourceAuthorization",
		"value": "enabled",
	})
	doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "tagResourceAuthorization",
		"value": "disabled",
	})

	listResp := doECSRequest(t, h, "ListAccountSettings", map[string]any{
		"name": "tagResourceAuthorization",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	settings := listOut["settings"].([]any)
	require.NotEmpty(t, settings)
	// Last write wins
	found := false
	for _, s := range settings {
		setting := s.(map[string]any)
		if setting["name"] == "tagResourceAuthorization" {
			assert.Equal(t, "disabled", setting["value"])
			found = true
		}
	}
	assert.True(t, found)
}

// ================================================================
// Attribute CRUD edge cases
// ================================================================

func TestBatch2_Attributes_PutListDelete_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "attr-cluster"})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":       "attr-cluster",
		"ec2InstanceId": "i-attr-1234",
	})
	require.Equal(t, http.StatusOK, ciResp.Code)
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	// Put attributes
	putResp := doECSRequest(t, h, "PutAttributes", map[string]any{
		"cluster": "attr-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "com.example.gpu",
				"value":      "nvidia",
				"targetType": "container-instance",
				"targetId":   ciArn,
			},
			map[string]any{
				"name":       "com.example.zone",
				"value":      "us-east-1a",
				"targetType": "container-instance",
				"targetId":   ciArn,
			},
		},
	})
	require.Equal(t, http.StatusOK, putResp.Code)

	// List attributes
	listResp := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":    "attr-cluster",
		"targetType": "container-instance",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	attrs := listOut["attributes"].([]any)
	assert.Len(t, attrs, 2)

	// Delete one attribute
	delResp := doECSRequest(t, h, "DeleteAttributes", map[string]any{
		"cluster": "attr-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "com.example.gpu",
				"targetType": "container-instance",
				"targetId":   ciArn,
			},
		},
	})
	require.Equal(t, http.StatusOK, delResp.Code)

	// Verify one remains
	listResp2 := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":    "attr-cluster",
		"targetType": "container-instance",
	})
	require.Equal(t, http.StatusOK, listResp2.Code)
	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(listResp2.Body.Bytes(), &listOut2))
	attrs2 := listOut2["attributes"].([]any)
	assert.Len(t, attrs2, 1)
	assert.Equal(t, "com.example.zone", attrs2[0].(map[string]any)["name"])
}

func TestBatch2_Attributes_FilterByName(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "attr-filter-cluster"})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":       "attr-filter-cluster",
		"ec2InstanceId": "i-filter-5678",
	})
	require.Equal(t, http.StatusOK, ciResp.Code)
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	doECSRequest(t, h, "PutAttributes", map[string]any{
		"cluster": "attr-filter-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "ecs.gpu",
				"value":      "1",
				"targetType": "container-instance",
				"targetId":   ciArn,
			},
			map[string]any{
				"name":       "ecs.cpu",
				"value":      "16",
				"targetType": "container-instance",
				"targetId":   ciArn,
			},
		},
	})

	// Filter by attributeName
	listResp := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":       "attr-filter-cluster",
		"targetType":    "container-instance",
		"attributeName": "ecs.gpu",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	attrs := listOut["attributes"].([]any)
	assert.Len(t, attrs, 1)
	assert.Equal(t, "ecs.gpu", attrs[0].(map[string]any)["name"])
}

// ================================================================
// ClusterSetting: containerInsights
// ================================================================

func TestBatch2_ClusterSetting_ContainerInsights_Enable(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "insights-cluster"})

	resp := doECSRequest(t, h, "UpdateClusterSettings", map[string]any{
		"cluster": "insights-cluster",
		"settings": []any{
			map[string]any{"name": "containerInsights", "value": "enabled"},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	cluster := out["cluster"].(map[string]any)
	settings := cluster["settings"].([]any)
	require.Len(t, settings, 1)
	assert.Equal(t, "containerInsights", settings[0].(map[string]any)["name"])
	assert.Equal(t, "enabled", settings[0].(map[string]any)["value"])
}

func TestBatch2_ClusterSetting_ContainerInsights_Disable(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{
		"clusterName": "insights-dis-cluster",
		"settings": []any{
			map[string]any{"name": "containerInsights", "value": "enabled"},
		},
	})

	resp := doECSRequest(t, h, "UpdateClusterSettings", map[string]any{
		"cluster": "insights-dis-cluster",
		"settings": []any{
			map[string]any{"name": "containerInsights", "value": "disabled"},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	cluster := out["cluster"].(map[string]any)
	settings := cluster["settings"].([]any)
	assert.Equal(t, "disabled", settings[0].(map[string]any)["value"])
}

func TestBatch2_CreateCluster_WithSettings(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	resp := doECSRequest(t, h, "CreateCluster", map[string]any{
		"clusterName": "with-settings-cluster",
		"settings": []any{
			map[string]any{"name": "containerInsights", "value": "enabled"},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	cluster := out["cluster"].(map[string]any)
	rawSettings := cluster["settings"]
	require.NotNil(t, rawSettings, "settings should be present in response")
	settings := rawSettings.([]any)
	require.Len(t, settings, 1)
	assert.Equal(t, "containerInsights", settings[0].(map[string]any)["name"])
	assert.Equal(t, "enabled", settings[0].(map[string]any)["value"])
}

// ================================================================
// ECSExec / ExecuteCommand
// ================================================================

func TestBatch2_ECSExec_Basic(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "exec-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "exec-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	runResp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":        "exec-cluster",
		"taskDefinition": "exec-task",
		"count":          1,
	})
	require.Equal(t, http.StatusOK, runResp.Code)
	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runResp.Body.Bytes(), &runOut))
	tasks := runOut["tasks"].([]any)
	require.Len(t, tasks, 1)
	taskArn := tasks[0].(map[string]any)["taskArn"].(string)

	execResp := doECSRequest(t, h, "ExecuteCommand", map[string]any{
		"cluster":     "exec-cluster",
		"task":        taskArn,
		"container":   "app",
		"command":     "/bin/bash",
		"interactive": true,
	})
	require.Equal(t, http.StatusOK, execResp.Code)

	var execOut map[string]any
	require.NoError(t, json.Unmarshal(execResp.Body.Bytes(), &execOut))
	session := execOut["session"].(map[string]any)
	assert.NotEmpty(t, session["sessionId"])
	assert.Equal(t, true, execOut["interactive"])
}

func TestBatch2_ECSExec_NonInteractive(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "exec2-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "exec2-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	runResp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":        "exec2-cluster",
		"taskDefinition": "exec2-task",
		"count":          1,
	})
	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runResp.Body.Bytes(), &runOut))
	taskArn := runOut["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

	execResp := doECSRequest(t, h, "ExecuteCommand", map[string]any{
		"cluster":     "exec2-cluster",
		"task":        taskArn,
		"container":   "app",
		"command":     "ls /tmp",
		"interactive": false,
	})
	require.Equal(t, http.StatusOK, execResp.Code)

	var execOut map[string]any
	require.NoError(t, json.Unmarshal(execResp.Body.Bytes(), &execOut))
	assert.Equal(t, false, execOut["interactive"])
}

// ================================================================
// ContainerInstance lifecycle: DRAINING/REGISTERING
// ================================================================

func TestBatch2_ContainerInstance_DRAINING_State(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "draining-cluster"})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":       "draining-cluster",
		"ec2InstanceId": "i-drain-1234",
	})
	require.Equal(t, http.StatusOK, ciResp.Code)
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)
	assert.Equal(t, "ACTIVE", ciOut["containerInstance"].(map[string]any)["status"])

	// Transition to DRAINING
	drainResp := doECSRequest(t, h, "UpdateContainerInstancesState", map[string]any{
		"cluster":            "draining-cluster",
		"containerInstances": []string{ciArn},
		"status":             "DRAINING",
	})
	require.Equal(t, http.StatusOK, drainResp.Code)

	var drainOut map[string]any
	require.NoError(t, json.Unmarshal(drainResp.Body.Bytes(), &drainOut))
	instances := drainOut["containerInstances"].([]any)
	require.Len(t, instances, 1)
	assert.Equal(t, "DRAINING", instances[0].(map[string]any)["status"])

	// Describe and verify DRAINING state persisted
	descResp := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
		"cluster":            "draining-cluster",
		"containerInstances": []string{ciArn},
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	described := descOut["containerInstances"].([]any)
	require.Len(t, described, 1)
	assert.Equal(t, "DRAINING", described[0].(map[string]any)["status"])
}

func TestBatch2_ContainerInstance_DRAINING_to_ACTIVE(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "reactivate-cluster"})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":       "reactivate-cluster",
		"ec2InstanceId": "i-react-5678",
	})
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	// Drain then re-activate
	doECSRequest(t, h, "UpdateContainerInstancesState", map[string]any{
		"cluster":            "reactivate-cluster",
		"containerInstances": []string{ciArn},
		"status":             "DRAINING",
	})
	activeResp := doECSRequest(t, h, "UpdateContainerInstancesState", map[string]any{
		"cluster":            "reactivate-cluster",
		"containerInstances": []string{ciArn},
		"status":             "ACTIVE",
	})
	require.Equal(t, http.StatusOK, activeResp.Code)

	var activeOut map[string]any
	require.NoError(t, json.Unmarshal(activeResp.Body.Bytes(), &activeOut))
	instances := activeOut["containerInstances"].([]any)
	assert.Equal(t, "ACTIVE", instances[0].(map[string]any)["status"])
}

func TestBatch2_ContainerInstance_UpdateAgent(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "update-agent-cluster"})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":       "update-agent-cluster",
		"ec2InstanceId": "i-agent-9999",
	})
	require.Equal(t, http.StatusOK, ciResp.Code)
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	resp := doECSRequest(t, h, "UpdateContainerAgent", map[string]any{
		"cluster":           "update-agent-cluster",
		"containerInstance": ciArn,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	ci := out["containerInstance"].(map[string]any)
	assert.NotEmpty(t, ci["containerInstanceArn"])
}

func TestBatch2_ContainerInstance_Deregister_Force(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "deregforce-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "deregforce-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":       "deregforce-cluster",
		"ec2InstanceId": "i-force-1234",
	})
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	// Start a task on this container instance
	doECSRequest(t, h, "StartTask", map[string]any{
		"cluster":            "deregforce-cluster",
		"taskDefinition":     "deregforce-task",
		"containerInstances": []string{ciArn},
	})

	// Force deregister even with running tasks
	deregResp := doECSRequest(t, h, "DeregisterContainerInstance", map[string]any{
		"cluster":           "deregforce-cluster",
		"containerInstance": ciArn,
		"force":             true,
	})
	require.Equal(t, http.StatusOK, deregResp.Code)

	// Verify it's gone
	listResp := doECSRequest(t, h, "ListContainerInstances", map[string]any{
		"cluster": "deregforce-cluster",
	})
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	arns := listOut["containerInstanceArns"].([]any)
	assert.Empty(t, arns)
}

// ================================================================
// TaskDefinition revisions + deregister
// ================================================================

func TestBatch2_TaskDefinition_MultipleRevisions_SameFamily(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	// Register 3 revisions of same family
	for range 3 {
		resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "rev-family",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})
		require.Equal(t, http.StatusOK, resp.Code)
	}

	// List task definitions for this family
	listResp := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{
		"familyPrefix": "rev-family",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	arns := listOut["taskDefinitionArns"].([]any)
	assert.Len(t, arns, 3)

	// ListTaskDefinitionFamilies should return the family once
	famResp := doECSRequest(t, h, "ListTaskDefinitionFamilies", map[string]any{
		"familyPrefix": "rev-family",
	})
	require.Equal(t, http.StatusOK, famResp.Code)
	var famOut map[string]any
	require.NoError(t, json.Unmarshal(famResp.Body.Bytes(), &famOut))
	families := famOut["families"].([]any)
	assert.Len(t, families, 1)
	assert.Equal(t, "rev-family", families[0])
}

func TestBatch2_TaskDefinition_Deregister_Specific_Revision(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	for range 3 {
		doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "dereg-rev-family",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})
	}

	// Describe revision 1 to get its ARN
	descResp := doECSRequest(t, h, "DescribeTaskDefinition", map[string]any{
		"taskDefinition": "dereg-rev-family:1",
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	td := descOut["taskDefinition"].(map[string]any)
	rev1Arn := td["taskDefinitionArn"].(string)
	require.NotEmpty(t, rev1Arn)

	// Deregister revision 1
	deregResp := doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{
		"taskDefinition": rev1Arn,
	})
	require.Equal(t, http.StatusOK, deregResp.Code)
	var deregOut map[string]any
	require.NoError(t, json.Unmarshal(deregResp.Body.Bytes(), &deregOut))
	deregTd := deregOut["taskDefinition"].(map[string]any)
	assert.Equal(t, "INACTIVE", deregTd["status"])

	// Revisions 2 and 3 should still be active
	listResp := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{
		"familyPrefix": "dereg-rev-family",
		"status":       "ACTIVE",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	arns := listOut["taskDefinitionArns"].([]any)
	assert.Len(t, arns, 2)
}

func TestBatch2_TaskDefinition_DeleteDefinitions_Batch(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	arns := make([]string, 3)
	for i := range 3 {
		resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "batch-del-family",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})
		var out map[string]any
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
		// Deregister first (required before delete)
		td := out["taskDefinition"].(map[string]any)
		arn := td["taskDefinitionArn"].(string)
		doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{"taskDefinition": arn})
		arns[i] = arn
	}

	deleteResp := doECSRequest(t, h, "DeleteTaskDefinitions", map[string]any{
		"taskDefinitions": arns,
	})
	require.Equal(t, http.StatusOK, deleteResp.Code)

	var deleteOut map[string]any
	require.NoError(t, json.Unmarshal(deleteResp.Body.Bytes(), &deleteOut))
	deleted := deleteOut["taskDefinitions"].([]any)
	assert.Len(t, deleted, 3)
}

func TestBatch2_TaskDefinition_ListByStatus_Active_Inactive(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	// Register 2 revisions
	for range 2 {
		doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "status-filter-family",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})
	}

	// Deregister revision 1
	doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{
		"taskDefinition": "status-filter-family:1",
	})

	// List ACTIVE
	activeResp := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{
		"familyPrefix": "status-filter-family",
		"status":       "ACTIVE",
	})
	var activeOut map[string]any
	require.NoError(t, json.Unmarshal(activeResp.Body.Bytes(), &activeOut))
	assert.Len(t, activeOut["taskDefinitionArns"].([]any), 1)

	// List INACTIVE
	inactiveResp := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{
		"familyPrefix": "status-filter-family",
		"status":       "INACTIVE",
	})
	var inactiveOut map[string]any
	require.NoError(t, json.Unmarshal(inactiveResp.Body.Bytes(), &inactiveOut))
	assert.Len(t, inactiveOut["taskDefinitionArns"].([]any), 1)
}

func TestBatch2_TaskDefinitionFamilies_MultipleStatus(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "fam-a",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "fam-b",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	// Deregister fam-b:1
	doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{"taskDefinition": "fam-b:1"})

	// List ACTIVE families
	resp := doECSRequest(t, h, "ListTaskDefinitionFamilies", map[string]any{
		"status": "ACTIVE",
	})
	require.Equal(t, http.StatusOK, resp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	families := out["families"].([]any)
	famNames := make([]string, 0, len(families))
	for _, f := range families {
		famNames = append(famNames, f.(string))
	}
	assert.Contains(t, famNames, "fam-a")
	assert.NotContains(t, famNames, "fam-b")
}

// ================================================================
// Tags (comprehensive)
// ================================================================

func TestBatch2_TagResource_Service(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "tag-svc-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "tag-svc-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	svcResp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "tag-svc-cluster",
		"serviceName":    "tag-svc",
		"taskDefinition": "tag-svc-task",
		"desiredCount":   1,
	})
	var svcOut map[string]any
	require.NoError(t, json.Unmarshal(svcResp.Body.Bytes(), &svcOut))
	svcArn := svcOut["service"].(map[string]any)["serviceArn"].(string)

	// Tag
	tagResp := doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": svcArn,
		"tags": []any{
			map[string]any{"key": "env", "value": "prod"},
			map[string]any{"key": "team", "value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, tagResp.Code)

	// List tags
	listResp := doECSRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": svcArn,
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	tags := listOut["tags"].([]any)
	assert.Len(t, tags, 2)
}

func TestBatch2_TagResource_Overwrite(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	fakeArn := "arn:aws:ecs:us-east-1:000000000000:cluster/overwrite-cluster"

	doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": fakeArn,
		"tags":        []any{map[string]any{"key": "env", "value": "dev"}},
	})
	doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": fakeArn,
		"tags":        []any{map[string]any{"key": "env", "value": "prod"}},
	})

	listResp := doECSRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": fakeArn,
	})
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	tags := listOut["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "prod", tags[0].(map[string]any)["value"])
}

func TestBatch2_UntagResource(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	fakeArn := "arn:aws:ecs:us-east-1:000000000000:cluster/untag-cluster"

	doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": fakeArn,
		"tags": []any{
			map[string]any{"key": "env", "value": "prod"},
			map[string]any{"key": "team", "value": "platform"},
			map[string]any{"key": "version", "value": "1.0"},
		},
	})

	doECSRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": fakeArn,
		"tagKeys":     []string{"env", "version"},
	})

	listResp := doECSRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": fakeArn,
	})
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	tags := listOut["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "team", tags[0].(map[string]any)["key"])
}

func TestBatch2_TagResource_Cluster(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "tagged-cluster"})

	clusterArn := "arn:aws:ecs:us-east-1:000000000000:cluster/tagged-cluster"
	doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": clusterArn,
		"tags":        []any{map[string]any{"key": "owner", "value": "ops-team"}},
	})

	listResp := doECSRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": clusterArn,
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &out))
	tags := out["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "owner", tags[0].(map[string]any)["key"])
}

func TestBatch2_CreateService_Tags_PropagateToResource(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "tag-prop-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "tag-prop-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	svcResp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "tag-prop-cluster",
		"serviceName":    "tag-prop-svc",
		"taskDefinition": "tag-prop-task",
		"desiredCount":   1,
		"tags": []any{
			map[string]any{"key": "app", "value": "myapp"},
			map[string]any{"key": "env", "value": "staging"},
		},
	})
	require.Equal(t, http.StatusOK, svcResp.Code)
	var svcOut map[string]any
	require.NoError(t, json.Unmarshal(svcResp.Body.Bytes(), &svcOut))
	tags := svcOut["service"].(map[string]any)["tags"].([]any)
	assert.Len(t, tags, 2)
}

// ================================================================
// TaskProtection
// ================================================================

func TestBatch2_TaskProtection_Enable_Disable(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "protect-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "protect-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	runResp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":        "protect-cluster",
		"taskDefinition": "protect-task",
		"count":          1,
	})
	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runResp.Body.Bytes(), &runOut))
	taskArn := runOut["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

	// Enable protection
	expiresIn := 60
	protResp := doECSRequest(t, h, "UpdateTaskProtection", map[string]any{
		"cluster":           "protect-cluster",
		"tasks":             []string{taskArn},
		"protectionEnabled": true,
		"expiresInMinutes":  expiresIn,
	})
	require.Equal(t, http.StatusOK, protResp.Code)

	var protOut map[string]any
	require.NoError(t, json.Unmarshal(protResp.Body.Bytes(), &protOut))
	prot := protOut["protectedTasks"].([]any)[0].(map[string]any)
	assert.Equal(t, true, prot["protectionEnabled"])

	// Get protection
	getResp := doECSRequest(t, h, "GetTaskProtection", map[string]any{
		"cluster": "protect-cluster",
		"tasks":   []string{taskArn},
	})
	require.Equal(t, http.StatusOK, getResp.Code)
	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &getOut))
	protTasks := getOut["protectedTasks"].([]any)
	assert.Len(t, protTasks, 1)
	assert.Equal(t, true, protTasks[0].(map[string]any)["protectionEnabled"])

	// Disable protection
	disableResp := doECSRequest(t, h, "UpdateTaskProtection", map[string]any{
		"cluster":           "protect-cluster",
		"tasks":             []string{taskArn},
		"protectionEnabled": false,
	})
	require.Equal(t, http.StatusOK, disableResp.Code)
}

// ================================================================
// ServiceDeployments
// ================================================================

func TestBatch2_ServiceDeployment_DescribeList_Roundtrip(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())
	h := ecs.NewHandler(backend)

	depArn := "arn:aws:ecs:us-east-1:000000000000:service-deployment/sd-dep-cluster/sd-dep-svc/dep-1"
	backend.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
		ServiceDeploymentArn: depArn,
		ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/sd-dep-cluster",
		ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/sd-dep-cluster/sd-dep-svc",
		Status:               "IN_PROGRESS",
	})

	listResp := doECSRequest(t, h, "ListServiceDeployments", map[string]any{
		"cluster": "sd-dep-cluster",
		"service": "sd-dep-svc",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	arns := listOut["serviceDeploymentArns"].([]any)
	require.NotEmpty(t, arns)

	descResp := doECSRequest(t, h, "DescribeServiceDeployments", map[string]any{
		"serviceDeploymentArns": []string{arns[0].(string)},
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	deps := descOut["serviceDeployments"].([]any)
	assert.Len(t, deps, 1)
	dep := deps[0].(map[string]any)
	assert.NotEmpty(t, dep["serviceDeploymentArn"])
}

func TestBatch2_StopServiceDeployment(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())
	h := ecs.NewHandler(backend)

	depArn := "arn:aws:ecs:us-east-1:000000000000:service-deployment/stop-dep-cluster/stop-dep-svc/dep-2"
	backend.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
		ServiceDeploymentArn: depArn,
		ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/stop-dep-cluster",
		ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/stop-dep-cluster/stop-dep-svc",
		Status:               "IN_PROGRESS",
	})

	stopResp := doECSRequest(t, h, "StopServiceDeployment", map[string]any{
		"serviceDeploymentArn": depArn,
	})
	require.Equal(t, http.StatusOK, stopResp.Code)
	var stopOut map[string]any
	require.NoError(t, json.Unmarshal(stopResp.Body.Bytes(), &stopOut))
	dep := stopOut["serviceDeployment"].(map[string]any)
	assert.Equal(t, "STOPPED", dep["status"])

	// Stop again should fail
	stopAgain := doECSRequest(t, h, "StopServiceDeployment", map[string]any{
		"serviceDeploymentArn": depArn,
	})
	assert.Equal(t, http.StatusBadRequest, stopAgain.Code)
}

// ================================================================
// Concurrent access safety
// ================================================================

func TestBatch2_Concurrent_CreateServices(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "concurrent-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "concurrent-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	var wg sync.WaitGroup
	results := make([]int, 10)

	for i := range 10 {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			body, _ := json.Marshal(map[string]any{
				"cluster":        "concurrent-cluster",
				"serviceName":    "concurrent-svc-" + string(rune('a'+idx)),
				"taskDefinition": "concurrent-task",
				"desiredCount":   1,
			})
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "AmazonEC2ContainerServiceV20141113.CreateService")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			_ = h.Handler()(c)
			results[idx] = rec.Code
		}(i)
	}

	wg.Wait()

	for i, code := range results {
		assert.Equal(t, http.StatusOK, code, "service %d", i)
	}
}

func TestBatch2_Concurrent_RegisterContainerInstances(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "concurrent-ci-cluster"})

	var wg sync.WaitGroup
	results := make([]int, 20)

	for i := range 20 {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()

			body, _ := json.Marshal(map[string]any{
				"cluster":       "concurrent-ci-cluster",
				"ec2InstanceId": "i-concurrent-" + string(rune('a'+idx)),
			})
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set(
				"X-Amz-Target",
				"AmazonEC2ContainerServiceV20141113.RegisterContainerInstance",
			)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			_ = h.Handler()(c)
			results[idx] = rec.Code
		}(i)
	}

	wg.Wait()

	for i, code := range results {
		assert.Equal(t, http.StatusOK, code, "container instance %d", i)
	}

	listResp := doECSRequest(t, h, "ListContainerInstances", map[string]any{
		"cluster": "concurrent-ci-cluster",
	})
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	arns := listOut["containerInstanceArns"].([]any)
	assert.Len(t, arns, 20)
}

// ================================================================
// UpdateCluster
// ================================================================

func TestBatch2_UpdateCluster_CapacityProviders(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "upd-cp-cluster"})
	doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "custom-cp"})

	resp := doECSRequest(t, h, "UpdateCluster", map[string]any{
		"cluster":           "upd-cp-cluster",
		"capacityProviders": []string{"FARGATE", "custom-cp"},
		"defaultCapacityProviderStrategy": []any{
			map[string]any{"capacityProvider": "FARGATE", "weight": 1},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	cluster := out["cluster"].(map[string]any)
	providers := cluster["capacityProviders"].([]any)
	assert.Len(t, providers, 2)
}

// ================================================================
// ListServices / DescribeServices
// ================================================================

func TestBatch2_ListServices_LaunchTypeFilter(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "lt-filter-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "lt-filter-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	for i := range 3 {
		doECSRequest(t, h, "CreateService", map[string]any{
			"cluster":        "lt-filter-cluster",
			"serviceName":    "lt-filter-svc-" + string(rune('a'+i)),
			"taskDefinition": "lt-filter-task",
			"desiredCount":   1,
			"launchType":     "FARGATE",
		})
	}

	listResp := doECSRequest(t, h, "ListServices", map[string]any{
		"cluster":    "lt-filter-cluster",
		"launchType": "FARGATE",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	arns := listOut["serviceArns"].([]any)
	assert.Len(t, arns, 3)
}

func TestBatch2_DescribeServices_MultipleServices(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "multi-svc-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "multi-svc-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	for i := range 4 {
		doECSRequest(t, h, "CreateService", map[string]any{
			"cluster":        "multi-svc-cluster",
			"serviceName":    "multi-svc-" + string(rune('a'+i)),
			"taskDefinition": "multi-svc-task",
			"desiredCount":   1,
		})
	}

	descResp := doECSRequest(t, h, "DescribeServices", map[string]any{
		"cluster":  "multi-svc-cluster",
		"services": []string{"multi-svc-a", "multi-svc-b", "multi-svc-c", "multi-svc-d"},
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	services := descOut["services"].([]any)
	assert.Len(t, services, 4)
}

// ================================================================
// Service operations: delete force, run task edge cases
// ================================================================

func TestBatch2_DeleteService_Force_WithRunningTasks(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "del-force-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "del-force-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "del-force-cluster",
		"serviceName":    "del-force-svc",
		"taskDefinition": "del-force-task",
		"desiredCount":   2,
	})

	deleteResp := doECSRequest(t, h, "DeleteService", map[string]any{
		"cluster": "del-force-cluster",
		"service": "del-force-svc",
		"force":   true,
	})
	require.Equal(t, http.StatusOK, deleteResp.Code)
}

func TestBatch2_RunTask_WithOverrides(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "override-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "override-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":        "override-cluster",
		"taskDefinition": "override-task",
		"count":          1,
		"overrides": map[string]any{
			"containerOverrides": []any{
				map[string]any{
					"name":    "app",
					"command": []string{"node", "server.js"},
					"environment": []any{
						map[string]any{"name": "PORT", "value": "8080"},
					},
				},
			},
			"taskRoleArn": "arn:aws:iam::000000000000:role/task-role",
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	tasks := out["tasks"].([]any)
	require.Len(t, tasks, 1)
}

func TestBatch2_RunTask_Count_Multiple(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "multi-run-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "multi-run-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":        "multi-run-cluster",
		"taskDefinition": "multi-run-task",
		"count":          5,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	tasks := out["tasks"].([]any)
	assert.Len(t, tasks, 5)

	// Each task has unique ARN
	arns := make(map[string]bool)
	for _, task := range tasks {
		arn := task.(map[string]any)["taskArn"].(string)
		arns[arn] = true
	}
	assert.Len(t, arns, 5)
}

// ================================================================
// Cluster operations: list, describe, stats
// ================================================================

func TestBatch2_DescribeClusters_MultipleClusters(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	for _, name := range []string{"cluster-x", "cluster-y", "cluster-z"} {
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": name})
	}

	descResp := doECSRequest(t, h, "DescribeClusters", map[string]any{
		"clusters": []string{"cluster-x", "cluster-y", "cluster-z"},
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &out))
	clusters := out["clusters"].([]any)
	assert.Len(t, clusters, 3)
}

func TestBatch2_ListClusters_Pagination(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	for i := range 5 {
		doECSRequest(t, h, "CreateCluster", map[string]any{
			"clusterName": "page-cluster-" + string(rune('a'+i)),
		})
	}

	resp := doECSRequest(t, h, "ListClusters", map[string]any{
		"maxResults": 3,
	})
	require.Equal(t, http.StatusOK, resp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	arns := out["clusterArns"].([]any)
	assert.Len(t, arns, 3)
	assert.NotEmpty(t, out["nextToken"])
}

// ================================================================
// StartTask edge cases
// ================================================================

func TestBatch2_StartTask_MultipleInstances(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "start-multi-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "start-multi-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	// Register 3 container instances
	ciArns := make([]string, 3)
	for i := range 3 {
		ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
			"cluster":       "start-multi-cluster",
			"ec2InstanceId": "i-multi-" + string(rune('a'+i)),
		})
		var ciOut map[string]any
		require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
		ciArns[i] = ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)
	}

	startResp := doECSRequest(t, h, "StartTask", map[string]any{
		"cluster":            "start-multi-cluster",
		"taskDefinition":     "start-multi-task",
		"containerInstances": ciArns,
	})
	require.Equal(t, http.StatusOK, startResp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(startResp.Body.Bytes(), &out))
	tasks := out["tasks"].([]any)
	assert.Len(t, tasks, 3)
}

// ================================================================
// ListServicesByNamespace
// ================================================================

func TestBatch2_ListServicesByNamespace_Filter(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ns-filter-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ns-filter-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	// Create services with different namespace prefixes
	for _, svcName := range []string{"payments-api", "payments-worker", "inventory-api"} {
		doECSRequest(t, h, "CreateService", map[string]any{
			"cluster":        "ns-filter-cluster",
			"serviceName":    svcName,
			"taskDefinition": "ns-filter-task",
			"desiredCount":   1,
		})
	}

	listResp := doECSRequest(t, h, "ListServicesByNamespace", map[string]any{
		"cluster":   "ns-filter-cluster",
		"namespace": "payments",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &out))
	arns := out["serviceArns"].([]any)
	assert.Len(t, arns, 2)
}

// ================================================================
// Handler Reset
// ================================================================

func TestBatch2_Handler_Reset_ClearsAll(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "reset-cluster"})
	doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "reset-cp"})

	h.Reset()

	// Default cluster should still exist
	listResp := doECSRequest(t, h, "ListClusters", map[string]any{})
	require.Equal(t, http.StatusOK, listResp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &out))
	// No custom clusters after reset
	arns := out["clusterArns"].([]any)
	for _, arn := range arns {
		assert.NotContains(t, arn.(string), "reset-cluster")
	}
}

package ecs_test

// handler_accuracy_ops2_test.go — ECS batch-2 ops AWS-accuracy audit (go-xmsm)
// Covers AWS-accurate failure semantics for Describe* batch operations:
// DescribeClusters, DescribeServices, DescribeTasks, DescribeContainerInstances
// all return per-item failures for unknown resources instead of a top-level error,
// matching real AWS ECS behaviour.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ================================================================
// DescribeClusters — unknown names land in failures, not 4xx
// ================================================================

func TestAccOps2_DescribeClusters_UnknownReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "DescribeClusters", map[string]any{
		"clusters": []string{"does-not-exist"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clusters, _ := resp["clusters"].([]any)
	assert.Empty(t, clusters)

	failures, _ := resp["failures"].([]any)
	require.Len(t, failures, 1)

	f := failures[0].(map[string]any)
	assert.Equal(t, "does-not-exist", f["arn"])
	assert.Equal(t, "MISSING", f["reason"])
}

func TestAccOps2_DescribeClusters_MixFoundAndMissing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "real-cluster"})

	rec := doECSRequest(t, h, "DescribeClusters", map[string]any{
		"clusters": []string{"real-cluster", "ghost-cluster"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clusters, _ := resp["clusters"].([]any)
	assert.Len(t, clusters, 1)

	failures, _ := resp["failures"].([]any)
	assert.Len(t, failures, 1)

	f := failures[0].(map[string]any)
	assert.Equal(t, "ghost-cluster", f["arn"])
}

func TestAccOps2_DescribeClusters_AllMissing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "DescribeClusters", map[string]any{
		"clusters": []string{"x", "y", "z"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clusters, _ := resp["clusters"].([]any)
	assert.Empty(t, clusters)

	failures, _ := resp["failures"].([]any)
	assert.Len(t, failures, 3)
}

func TestAccOps2_DescribeClusters_EmptyReturnsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "c1"})
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "c2"})

	rec := doECSRequest(t, h, "DescribeClusters", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clusters, _ := resp["clusters"].([]any)
	assert.Len(t, clusters, 2)

	failures, _ := resp["failures"].([]any)
	assert.Empty(t, failures)
}

// ================================================================
// DescribeServices — unknown names land in failures, not 4xx
// ================================================================

func TestAccOps2_DescribeServices_UnknownReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "DescribeServices", map[string]any{
		"services": []string{"ghost-service"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, _ := resp["services"].([]any)
	assert.Empty(t, services)

	failures, _ := resp["failures"].([]any)
	require.Len(t, failures, 1)

	f := failures[0].(map[string]any)
	assert.Equal(t, "ghost-service", f["arn"])
	assert.Equal(t, "MISSING", f["reason"])
}

func TestAccOps2_DescribeServices_MixFoundAndMissing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "acc-ops2-svc-task")
	doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "real-svc",
		"taskDefinition": tdArn,
		"desiredCount":   0,
	})

	rec := doECSRequest(t, h, "DescribeServices", map[string]any{
		"services": []string{"real-svc", "ghost-svc"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, _ := resp["services"].([]any)
	assert.Len(t, services, 1)

	svc := services[0].(map[string]any)
	assert.Equal(t, "real-svc", svc["serviceName"])

	failures, _ := resp["failures"].([]any)
	require.Len(t, failures, 1)

	f := failures[0].(map[string]any)
	assert.Equal(t, "ghost-svc", f["arn"])
}

func TestAccOps2_DescribeServices_ClusterNotFoundStillErrors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "DescribeServices", map[string]any{
		"cluster":  "nonexistent-cluster",
		"services": []string{"any-svc"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccOps2_DescribeServices_EmptyListReturnsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "acc-ops2-list-task")
	doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "svc-a",
		"taskDefinition": tdArn,
		"desiredCount":   0,
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "svc-b",
		"taskDefinition": tdArn,
		"desiredCount":   0,
	})

	rec := doECSRequest(t, h, "DescribeServices", map[string]any{"services": []string{}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, _ := resp["services"].([]any)
	assert.Len(t, services, 2)

	failures, _ := resp["failures"].([]any)
	assert.Empty(t, failures)
}

// ================================================================
// DescribeTasks — unknown ARNs land in failures, not 4xx
// ================================================================

func TestAccOps2_DescribeTasks_UnknownReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dt-cluster"})

	rec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"cluster": "dt-cluster",
		"tasks":   []string{"arn:aws:ecs:us-east-1:000000000000:task/dt-cluster/ghost"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tasks, _ := resp["tasks"].([]any)
	assert.Empty(t, tasks)

	failures, _ := resp["failures"].([]any)
	require.Len(t, failures, 1)

	f := failures[0].(map[string]any)
	assert.Contains(t, f["arn"], "ghost")
	assert.Equal(t, "MISSING", f["reason"])
}

func TestAccOps2_DescribeTasks_MixFoundAndMissing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "dt-mix-task")
	runResp := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          1,
	})
	require.Equal(t, http.StatusOK, runResp.Code)

	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runResp.Body.Bytes(), &runOut))
	tasksRaw, _ := runOut["tasks"].([]any)
	require.Len(t, tasksRaw, 1)
	taskArn := tasksRaw[0].(map[string]any)["taskArn"].(string)

	rec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"tasks": []string{taskArn, "arn:aws:ecs:us-east-1:000000000000:task/default/ghost"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tasks, _ := resp["tasks"].([]any)
	assert.Len(t, tasks, 1)

	failures, _ := resp["failures"].([]any)
	assert.Len(t, failures, 1)
}

func TestAccOps2_DescribeTasks_ClusterNotFoundStillErrors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"cluster": "nonexistent-cluster",
		"tasks":   []string{"some-task-arn"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccOps2_DescribeTasks_MultipleUnknown(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dt-multi"})

	rec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"cluster": "dt-multi",
		"tasks": []string{
			"arn:aws:ecs:us-east-1:000000000000:task/dt-multi/ghost1",
			"arn:aws:ecs:us-east-1:000000000000:task/dt-multi/ghost2",
			"arn:aws:ecs:us-east-1:000000000000:task/dt-multi/ghost3",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tasks, _ := resp["tasks"].([]any)
	assert.Empty(t, tasks)

	failures, _ := resp["failures"].([]any)
	assert.Len(t, failures, 3)
}

// ================================================================
// DescribeContainerInstances — unknown refs land in failures, not 4xx
// ================================================================

func TestAccOps2_DescribeContainerInstances_UnknownReturnsFailure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dci-cluster"})

	rec := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
		"cluster":            "dci-cluster",
		"containerInstances": []string{"arn:aws:ecs:us-east-1:000000000000:container-instance/dci-cluster/ghost"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	instances, _ := resp["containerInstances"].([]any)
	assert.Empty(t, instances)

	failures, _ := resp["failures"].([]any)
	require.Len(t, failures, 1)

	f := failures[0].(map[string]any)
	assert.Contains(t, f["arn"], "ghost")
	assert.Equal(t, "MISSING", f["reason"])
}

func TestAccOps2_DescribeContainerInstances_MixFoundAndMissing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dci-mix"})
	regResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster": "dci-mix",
	})
	require.Equal(t, http.StatusOK, regResp.Code)

	var regOut map[string]any
	require.NoError(t, json.Unmarshal(regResp.Body.Bytes(), &regOut))
	ciArn := regOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	rec := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
		"cluster":            "dci-mix",
		"containerInstances": []string{ciArn, "arn:aws:ecs:us-east-1:000000000000:container-instance/dci-mix/ghost"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	instances, _ := resp["containerInstances"].([]any)
	assert.Len(t, instances, 1)

	failures, _ := resp["failures"].([]any)
	assert.Len(t, failures, 1)
}

func TestAccOps2_DescribeContainerInstances_ClusterNotFoundStillErrors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
		"cluster":            "nonexistent-cluster",
		"containerInstances": []string{"any-instance"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccOps2_DescribeContainerInstances_EmptyListReturnsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dci-all"})
	doECSRequest(t, h, "RegisterContainerInstance", map[string]any{"cluster": "dci-all"})
	doECSRequest(t, h, "RegisterContainerInstance", map[string]any{"cluster": "dci-all"})

	rec := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
		"cluster":            "dci-all",
		"containerInstances": []string{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	instances, _ := resp["containerInstances"].([]any)
	assert.Len(t, instances, 2)

	failures, _ := resp["failures"].([]any)
	assert.Empty(t, failures)
}

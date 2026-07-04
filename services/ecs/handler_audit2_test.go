package ecs_test

// handler_audit2_test.go — ECS batch-2 ops AWS-accuracy audit (go-g091)
//
// Fixes three AWS-accuracy null-array bugs:
//
//  1. Cluster.CapacityProviders + DefaultCapacityProviderStrategy: absent when
//     empty. AWS always returns "capacityProviders":[] and
//     "defaultCapacityProviderStrategy":[] in cluster responses.
//
//  2. Service.LoadBalancers + ServiceRegistries: absent when empty. AWS always
//     returns "loadBalancers":[] and "serviceRegistries":[] in service responses.
//
//  3. TaskDefinition.Volumes: absent when empty. AWS always returns "volumes":[]
//     in RegisterTaskDefinition and DescribeTaskDefinition responses.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ================================================================
// Bug 1: Cluster.CapacityProviders + DefaultCapacityProviderStrategy null bugs
// ================================================================

func TestAudit2_Cluster_CapacityProviders_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "CreateCluster", map[string]any{
		"clusterName": "bare-cluster",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cluster, ok := resp["cluster"].(map[string]any)
	require.True(t, ok, "cluster key must be present")

	// AWS always returns capacityProviders as an array, never absent.
	raw, exists := cluster["capacityProviders"]
	assert.True(t, exists, "capacityProviders must be present in cluster response")
	assert.NotNil(t, raw, "capacityProviders must not be null")

	providers, ok := raw.([]any)
	assert.True(t, ok, "capacityProviders must be an array")
	assert.Empty(t, providers, "capacityProviders must be [] when none set")
}

func TestAudit2_Cluster_DefaultCapacityProviderStrategy_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "CreateCluster", map[string]any{
		"clusterName": "bare-cluster2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cluster := resp["cluster"].(map[string]any)

	// AWS always returns defaultCapacityProviderStrategy as an array.
	raw, exists := cluster["defaultCapacityProviderStrategy"]
	assert.True(t, exists, "defaultCapacityProviderStrategy must be present")
	assert.NotNil(t, raw, "defaultCapacityProviderStrategy must not be null")

	strategy, ok := raw.([]any)
	assert.True(t, ok, "defaultCapacityProviderStrategy must be an array")
	assert.Empty(t, strategy, "must be [] when no default strategy set")
}

func TestAudit2_DescribeCluster_CapacityProviders_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dc-cluster"})

	rec := doECSRequest(t, h, "DescribeClusters", map[string]any{
		"clusters": []string{"dc-cluster"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clusters, _ := resp["clusters"].([]any)
	require.Len(t, clusters, 1)

	cluster := clusters[0].(map[string]any)

	raw, exists := cluster["capacityProviders"]
	assert.True(t, exists, "capacityProviders present in DescribeClusters response")
	providers, _ := raw.([]any)
	assert.NotNil(t, providers)
	assert.Empty(t, providers)

	rawDcps, exists2 := cluster["defaultCapacityProviderStrategy"]
	assert.True(t, exists2, "defaultCapacityProviderStrategy present in DescribeClusters response")
	dcps, _ := rawDcps.([]any)
	assert.NotNil(t, dcps)
	assert.Empty(t, dcps)
}

// ================================================================
// Bug 2: Service.LoadBalancers + ServiceRegistries null bugs
// ================================================================

func TestAudit2_Service_LoadBalancers_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "svc-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "svc-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "svc-cluster",
		"serviceName":    "svc-no-lb",
		"taskDefinition": "svc-task",
		"desiredCount":   0,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	svc := resp["service"].(map[string]any)

	// AWS always returns loadBalancers as an array, never absent.
	raw, exists := svc["loadBalancers"]
	assert.True(t, exists, "loadBalancers must be present in service response")
	assert.NotNil(t, raw, "loadBalancers must not be null")

	lbs, ok := raw.([]any)
	assert.True(t, ok, "loadBalancers must be an array")
	assert.Empty(t, lbs, "must be [] when no load balancers configured")
}

func TestAudit2_Service_ServiceRegistries_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "svc-cluster2"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "svc-task2",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "svc-cluster2",
		"serviceName":    "svc-no-reg",
		"taskDefinition": "svc-task2",
		"desiredCount":   0,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	svc := resp["service"].(map[string]any)

	// AWS always returns serviceRegistries as an array.
	raw, exists := svc["serviceRegistries"]
	assert.True(t, exists, "serviceRegistries must be present in service response")
	assert.NotNil(t, raw, "serviceRegistries must not be null")

	regs, ok := raw.([]any)
	assert.True(t, ok, "serviceRegistries must be an array")
	assert.Empty(t, regs, "must be [] when no service registries configured")
}

func TestAudit2_DescribeServices_LoadBalancers_ServiceRegistries_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ds-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ds-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "ds-cluster",
		"serviceName":    "ds-svc",
		"taskDefinition": "ds-task",
		"desiredCount":   0,
	})

	rec := doECSRequest(t, h, "DescribeServices", map[string]any{
		"cluster":  "ds-cluster",
		"services": []string{"ds-svc"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, _ := resp["services"].([]any)
	require.Len(t, services, 1)

	svc := services[0].(map[string]any)

	rawLBs, existsLB := svc["loadBalancers"]
	assert.True(t, existsLB, "loadBalancers present in DescribeServices response")
	lbs, _ := rawLBs.([]any)
	assert.NotNil(t, lbs)
	assert.Empty(t, lbs)

	rawRegs, existsReg := svc["serviceRegistries"]
	assert.True(t, existsReg, "serviceRegistries present in DescribeServices response")
	regs, _ := rawRegs.([]any)
	assert.NotNil(t, regs)
	assert.Empty(t, regs)
}

// ================================================================
// Bug 3: TaskDefinition.Volumes null bug
// ================================================================

func TestAudit2_TaskDefinition_Volumes_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "vol-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		// no volumes field
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	td := resp["taskDefinition"].(map[string]any)

	// AWS always returns volumes as an array, never absent.
	raw, exists := td["volumes"]
	assert.True(t, exists, "volumes must be present in RegisterTaskDefinition response")
	assert.NotNil(t, raw, "volumes must not be null")

	vols, ok := raw.([]any)
	assert.True(t, ok, "volumes must be an array")
	assert.Empty(t, vols, "must be [] when no volumes defined")
}

func TestAudit2_DescribeTaskDefinition_Volumes_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "dtd-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "DescribeTaskDefinition", map[string]any{
		"taskDefinition": "dtd-task",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	td := resp["taskDefinition"].(map[string]any)

	raw, exists := td["volumes"]
	assert.True(t, exists, "volumes present in DescribeTaskDefinition response")
	assert.NotNil(t, raw, "volumes not null")

	vols, ok := raw.([]any)
	assert.True(t, ok, "volumes is array")
	assert.Empty(t, vols)
}

func TestAudit2_TaskDefinition_Volumes_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "vol-rt-task",
		"containerDefinitions": []any{
			map[string]any{
				"name":  "app",
				"image": "nginx",
				"mountPoints": []any{
					map[string]any{"sourceVolume": "data", "containerPath": "/data"},
				},
			},
		},
		"volumes": []any{
			map[string]any{"name": "data"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	td := resp["taskDefinition"].(map[string]any)
	vols, _ := td["volumes"].([]any)
	require.Len(t, vols, 1)

	vol := vols[0].(map[string]any)
	assert.Equal(t, "data", vol["name"])
}

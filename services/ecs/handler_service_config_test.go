package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_CapacityProviderStrategy_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestService_CapacityProviderStrategy_UpdateService(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestService_CircuitBreaker_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestService_CircuitBreaker_EnabledNoRollback(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestService_CircuitBreaker_UpdatePreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestService_DeploymentConfig_Defaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestServiceConnect_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestServiceConnect_Disabled(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestServiceConnect_UpdateService(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestServiceDiscovery_CloudMap_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestServiceDiscovery_MultipleRegistries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestServiceDiscovery_DescribeServices_Preserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

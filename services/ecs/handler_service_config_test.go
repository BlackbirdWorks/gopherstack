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

func TestService_EnableExecuteCommand_Create(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "exec-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "exec-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "exec-cluster",
		"serviceName":          "exec-svc",
		"taskDefinition":       "exec-task",
		"desiredCount":         1,
		"enableExecuteCommand": true,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	assert.Equal(t, true, svc["enableExecuteCommand"])
}

func TestService_EnableExecuteCommand_Default_False(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "no-exec-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "no-exec-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "no-exec-cluster",
		"serviceName":    "no-exec-svc",
		"taskDefinition": "no-exec-task",
		"desiredCount":   0,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	eec := svc["enableExecuteCommand"]
	assert.NotEqual(t, true, eec)
}

func TestService_EnableExecuteCommand_UpdateService(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "upd-exec-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "upd-exec-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "upd-exec-cluster",
		"serviceName":    "upd-exec-svc",
		"taskDefinition": "upd-exec-task",
		"desiredCount":   0,
	})

	// Enable exec on existing service
	updResp := doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster":              "upd-exec-cluster",
		"service":              "upd-exec-svc",
		"enableExecuteCommand": true,
	})
	require.Equal(t, http.StatusOK, updResp.Code)

	var updOut map[string]any
	require.NoError(t, json.Unmarshal(updResp.Body.Bytes(), &updOut))
	svc := updOut["service"].(map[string]any)
	assert.Equal(t, true, svc["enableExecuteCommand"])

	// Disable exec
	disResp := doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster":              "upd-exec-cluster",
		"service":              "upd-exec-svc",
		"enableExecuteCommand": false,
	})
	require.Equal(t, http.StatusOK, disResp.Code)

	var disOut map[string]any
	require.NoError(t, json.Unmarshal(disResp.Body.Bytes(), &disOut))
	disSvc := disOut["service"].(map[string]any)
	eec := disSvc["enableExecuteCommand"]
	assert.NotEqual(t, true, eec)
}

func TestService_EnableExecuteCommand_DescribeServices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "desc-exec-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "desc-exec-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "desc-exec-cluster",
		"serviceName":          "desc-exec-svc",
		"taskDefinition":       "desc-exec-task",
		"desiredCount":         0,
		"enableExecuteCommand": true,
	})

	descResp := doECSRequest(t, h, "DescribeServices", map[string]any{
		"cluster":  "desc-exec-cluster",
		"services": []any{"desc-exec-svc"},
	})
	require.Equal(t, http.StatusOK, descResp.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	svcs := descOut["services"].([]any)
	require.Len(t, svcs, 1)
	assert.Equal(t, true, svcs[0].(map[string]any)["enableExecuteCommand"])
}

func TestService_EmptyArrays_NotNull(t *testing.T) {
	t.Parallel()

	t.Run("create loadBalancers empty not null", func(t *testing.T) {
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
	})

	t.Run("create serviceRegistries empty not null", func(t *testing.T) {
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
	})

	t.Run("describe loadBalancers and serviceRegistries empty not null", func(t *testing.T) {
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
	})
}

func TestHandler_CreateService_DeploymentConfigDefaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentConfiguration"].(map[string]any)
	assert.InDelta(t, float64(100), dc["minimumHealthyPercent"], 0.001)
	assert.InDelta(t, float64(200), dc["maximumPercent"], 0.001)
}

func TestHandler_CreateService_DeploymentConfigCustom(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
		"deploymentConfiguration": map[string]any{
			"minimumHealthyPercent": 50,
			"maximumPercent":        150,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentConfiguration"].(map[string]any)
	assert.InDelta(t, float64(50), dc["minimumHealthyPercent"], 0.001)
	assert.InDelta(t, float64(150), dc["maximumPercent"], 0.001)
}

func TestHandler_CreateService_DeploymentController_CodeDeploy_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":          "my-svc",
		"taskDefinition":       "myapp",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "CODE_DEPLOY"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "CODE_DEPLOY")
}

func TestHandler_CreateService_DeploymentController_Rolling_Accepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":          "my-svc",
		"taskDefinition":       "myapp",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "ROLLING"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentController"].(map[string]any)
	assert.Equal(t, "ROLLING", dc["type"])
}

func TestHandler_CreateService_LoadBalancers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
		"loadBalancers": []map[string]any{
			{
				"targetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/my-tg/abcd1234",
				"containerName":  "app",
				"containerPort":  8080,
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	lbs := svc["loadBalancers"].([]any)
	require.Len(t, lbs, 1)
	lb := lbs[0].(map[string]any)
	assert.InDelta(t, float64(8080), lb["containerPort"], 0.001)
	assert.Equal(t, "app", lb["containerName"])
}

func TestHandler_CreateService_ServiceRegistries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
		"serviceRegistries": []map[string]any{
			{
				"registryArn":   "arn:aws:servicediscovery:us-east-1:000000000000:service/srv-abc123",
				"containerName": "app",
				"containerPort": 80,
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	regs := svc["serviceRegistries"].([]any)
	require.Len(t, regs, 1)
	reg := regs[0].(map[string]any)
	assert.Equal(t, "app", reg["containerName"])
}

func TestHandler_CreateService_NetworkConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
		"networkConfiguration": map[string]any{
			"awsvpcConfiguration": map[string]any{
				"subnets":        []string{"subnet-abc123", "subnet-def456"},
				"securityGroups": []string{"sg-12345678"},
				"assignPublicIp": "ENABLED",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	nc := svc["networkConfiguration"].(map[string]any)
	avpc := nc["awsvpcConfiguration"].(map[string]any)
	subnets := avpc["subnets"].([]any)
	assert.Len(t, subnets, 2)
	assert.Equal(t, "ENABLED", avpc["assignPublicIp"])
}

func TestHandler_UpdateService_NetworkConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	_ = doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
	})

	rec := doECSRequest(t, h, "UpdateService", map[string]any{
		"service": "my-svc",
		"networkConfiguration": map[string]any{
			"awsvpcConfiguration": map[string]any{
				"subnets": []string{"subnet-new123"},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	nc := svc["networkConfiguration"].(map[string]any)
	avpc := nc["awsvpcConfiguration"].(map[string]any)
	subnets := avpc["subnets"].([]any)
	require.Len(t, subnets, 1)
	assert.Equal(t, "subnet-new123", subnets[0])
}

func TestHandler_UpdateService_LoadBalancers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	_ = doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
	})

	rec := doECSRequest(t, h, "UpdateService", map[string]any{
		"service": "my-svc",
		"loadBalancers": []map[string]any{
			{
				"targetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/new-tg/xyz",
				"containerPort":  9090,
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	lbs := svc["loadBalancers"].([]any)
	require.Len(t, lbs, 1)
	lb := lbs[0].(map[string]any)
	assert.InDelta(t, float64(9090), lb["containerPort"], 0.001)
}

func TestHandler_UpdateService_DeploymentConfig_MinMaxPercent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	_ = doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
	})

	rec := doECSRequest(t, h, "UpdateService", map[string]any{
		"service": "my-svc",
		"deploymentConfiguration": map[string]any{
			"minimumHealthyPercent": 25,
			"maximumPercent":        300,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentConfiguration"].(map[string]any)
	assert.InDelta(t, float64(25), dc["minimumHealthyPercent"], 0.001)
	assert.InDelta(t, float64(300), dc["maximumPercent"], 0.001)
}

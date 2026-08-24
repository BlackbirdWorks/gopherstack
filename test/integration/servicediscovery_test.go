package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func servicediscoveryRequest(t *testing.T, op string, body any) *http.Response {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint+"/", bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Route53AutoNaming_v20170314."+op)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func servicediscoveryReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_ServiceDiscovery_CreateHTTPNamespace(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := servicediscoveryRequest(t, "CreateHttpNamespace", map[string]any{
		"Name": "integ-http-namespace",
	})
	body := servicediscoveryReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "OperationId")
}

func TestIntegration_ServiceDiscovery_NamespaceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create a namespace.
	createResp := servicediscoveryRequest(t, "CreateHttpNamespace", map[string]any{
		"Name": "integ-lifecycle-ns",
	})
	createBody := servicediscoveryReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode, "body: %s", createBody)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))
	opID, ok := createResult["OperationId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, opID)

	// Get the operation to find the namespace ID.
	opResp := servicediscoveryRequest(t, "GetOperation", map[string]any{"OperationId": opID})
	opBody := servicediscoveryReadBody(t, opResp)
	require.Equal(t, http.StatusOK, opResp.StatusCode, "body: %s", opBody)

	var opResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(opBody), &opResult))
	operation := opResult["Operation"].(map[string]any)
	targets := operation["Targets"].(map[string]any)
	nsID, ok := targets["NAMESPACE"].(string)
	require.True(t, ok)
	require.NotEmpty(t, nsID)

	// Get the namespace.
	getResp := servicediscoveryRequest(t, "GetNamespace", map[string]any{"Id": nsID})
	getBody := servicediscoveryReadBody(t, getResp)
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "body: %s", getBody)
	assert.Contains(t, getBody, "integ-lifecycle-ns")

	// List namespaces.
	listResp := servicediscoveryRequest(t, "ListNamespaces", map[string]any{})
	listBody := servicediscoveryReadBody(t, listResp)
	assert.Equal(t, http.StatusOK, listResp.StatusCode, "body: %s", listBody)
	assert.Contains(t, listBody, "Namespaces")
}

func TestIntegration_ServiceDiscovery_ServiceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create a service.
	createResp := servicediscoveryRequest(t, "CreateService", map[string]any{
		"Name": "integ-svc",
	})
	createBody := servicediscoveryReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode, "body: %s", createBody)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))
	svcData := createResult["Service"].(map[string]any)
	svcID, ok := svcData["Id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, svcID)

	// Get the service.
	getResp := servicediscoveryRequest(t, "GetService", map[string]any{"Id": svcID})
	getBody := servicediscoveryReadBody(t, getResp)
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "body: %s", getBody)
	assert.Contains(t, getBody, "integ-svc")

	// Register an instance.
	regResp := servicediscoveryRequest(t, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-integ-001",
		"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
	})
	regBody := servicediscoveryReadBody(t, regResp)
	assert.Equal(t, http.StatusOK, regResp.StatusCode, "body: %s", regBody)

	// List instances.
	listInstResp := servicediscoveryRequest(t, "ListInstances", map[string]any{"ServiceId": svcID})
	listInstBody := servicediscoveryReadBody(t, listInstResp)
	assert.Equal(t, http.StatusOK, listInstResp.StatusCode, "body: %s", listInstBody)
	assert.Contains(t, listInstBody, "Instances")

	// Deregister the instance.
	deregResp := servicediscoveryRequest(t, "DeregisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-integ-001",
	})
	deregBody := servicediscoveryReadBody(t, deregResp)
	assert.Equal(t, http.StatusOK, deregResp.StatusCode, "body: %s", deregBody)
}

func TestIntegration_ServiceDiscovery_UpdateHttpNamespace(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create an HTTP namespace.
	createResp := servicediscoveryRequest(t, "CreateHttpNamespace", map[string]any{
		"Name": "integ-update-http-ns",
	})
	createBody := servicediscoveryReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode, "body: %s", createBody)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))
	opID := createResult["OperationId"].(string)

	// Get the operation to find the namespace ID.
	opResp := servicediscoveryRequest(t, "GetOperation", map[string]any{"OperationId": opID})
	opBody := servicediscoveryReadBody(t, opResp)
	var opResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(opBody), &opResult))
	nsID := opResult["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	// Update the namespace.
	updateResp := servicediscoveryRequest(t, "UpdateHttpNamespace", map[string]any{
		"Id":        nsID,
		"Namespace": map[string]any{"Description": "updated description"},
	})
	updateBody := servicediscoveryReadBody(t, updateResp)
	assert.Equal(t, http.StatusOK, updateResp.StatusCode, "body: %s", updateBody)
	assert.Contains(t, updateBody, "OperationId")
}

func TestIntegration_ServiceDiscovery_UpdatePrivateDnsNamespace(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := servicediscoveryRequest(t, "CreatePrivateDnsNamespace", map[string]any{
		"Name": "integ-update-private-ns",
	})
	createBody := servicediscoveryReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode, "body: %s", createBody)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))
	opID := createResult["OperationId"].(string)

	opResp := servicediscoveryRequest(t, "GetOperation", map[string]any{"OperationId": opID})
	opBody := servicediscoveryReadBody(t, opResp)
	var opResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(opBody), &opResult))
	nsID := opResult["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	updateResp := servicediscoveryRequest(t, "UpdatePrivateDnsNamespace", map[string]any{
		"Id":        nsID,
		"Namespace": map[string]any{"Description": "updated private dns"},
	})
	updateBody := servicediscoveryReadBody(t, updateResp)
	assert.Equal(t, http.StatusOK, updateResp.StatusCode, "body: %s", updateBody)
	assert.Contains(t, updateBody, "OperationId")
}

func TestIntegration_ServiceDiscovery_UpdatePublicDnsNamespace(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := servicediscoveryRequest(t, "CreatePublicDnsNamespace", map[string]any{
		"Name": "integ-update-public-ns",
	})
	createBody := servicediscoveryReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode, "body: %s", createBody)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))
	opID := createResult["OperationId"].(string)

	opResp := servicediscoveryRequest(t, "GetOperation", map[string]any{"OperationId": opID})
	opBody := servicediscoveryReadBody(t, opResp)
	var opResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(opBody), &opResult))
	nsID := opResult["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	updateResp := servicediscoveryRequest(t, "UpdatePublicDnsNamespace", map[string]any{
		"Id":        nsID,
		"Namespace": map[string]any{"Description": "updated public dns"},
	})
	updateBody := servicediscoveryReadBody(t, updateResp)
	assert.Equal(t, http.StatusOK, updateResp.StatusCode, "body: %s", updateBody)
	assert.Contains(t, updateBody, "OperationId")
}

func TestIntegration_ServiceDiscovery_UpdateService(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	createResp := servicediscoveryRequest(t, "CreateService", map[string]any{
		"Name": "integ-update-svc",
	})
	createBody := servicediscoveryReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode, "body: %s", createBody)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))
	svcID := createResult["Service"].(map[string]any)["Id"].(string)

	updateResp := servicediscoveryRequest(t, "UpdateService", map[string]any{
		"Id":      svcID,
		"Service": map[string]any{"Description": "updated description"},
	})
	updateBody := servicediscoveryReadBody(t, updateResp)
	assert.Equal(t, http.StatusOK, updateResp.StatusCode, "body: %s", updateBody)
	assert.Contains(t, updateBody, "OperationId")

	// Verify the description was updated.
	getResp := servicediscoveryRequest(t, "GetService", map[string]any{"Id": svcID})
	getBody := servicediscoveryReadBody(t, getResp)
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "body: %s", getBody)
	assert.Contains(t, getBody, "updated description")
}

func TestIntegration_ServiceDiscovery_ServiceAttributesLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create a service.
	createResp := servicediscoveryRequest(t, "CreateService", map[string]any{
		"Name": "integ-attrs-svc",
	})
	createBody := servicediscoveryReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode, "body: %s", createBody)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))
	svcID := createResult["Service"].(map[string]any)["Id"].(string)

	// UpdateServiceAttributes.
	updateResp := servicediscoveryRequest(t, "UpdateServiceAttributes", map[string]any{
		"ServiceId":  svcID,
		"Attributes": map[string]string{"env": "staging", "version": "2.0"},
	})
	updateBody := servicediscoveryReadBody(t, updateResp)
	assert.Equal(t, http.StatusOK, updateResp.StatusCode, "body: %s", updateBody)

	// GetServiceAttributes.
	getResp := servicediscoveryRequest(t, "GetServiceAttributes", map[string]any{"ServiceId": svcID})
	getBody := servicediscoveryReadBody(t, getResp)
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "body: %s", getBody)
	assert.Contains(t, getBody, "ServiceAttributes")
	assert.Contains(t, getBody, "staging")

	// DeleteServiceAttributes.
	deleteResp := servicediscoveryRequest(t, "DeleteServiceAttributes", map[string]any{"ServiceId": svcID})
	deleteBody := servicediscoveryReadBody(t, deleteResp)
	assert.Equal(t, http.StatusOK, deleteResp.StatusCode, "body: %s", deleteBody)

	// GetServiceAttributes after delete should return 400.
	getResp2 := servicediscoveryRequest(t, "GetServiceAttributes", map[string]any{"ServiceId": svcID})
	getBody2 := servicediscoveryReadBody(t, getResp2)
	assert.Equal(t, http.StatusBadRequest, getResp2.StatusCode, "body: %s", getBody2)
}

func TestIntegration_ServiceDiscovery_UpdateInstanceCustomHealthStatus(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create a service and register an instance. UpdateInstanceCustomHealthStatus is only
	// valid for services created with a HealthCheckCustomConfig; without it real AWS returns
	// CustomHealthNotFound, so the service must declare custom health here.
	createResp := servicediscoveryRequest(t, "CreateService", map[string]any{
		"Name":                    "integ-health-svc",
		"HealthCheckCustomConfig": map[string]any{"FailureThreshold": 1},
	})
	createBody := servicediscoveryReadBody(t, createResp)
	require.Equal(t, http.StatusOK, createResp.StatusCode, "body: %s", createBody)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createBody), &createResult))
	svcID := createResult["Service"].(map[string]any)["Id"].(string)

	regResp := servicediscoveryRequest(t, "RegisterInstance", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-health-001",
		"Attributes": map[string]string{"AWS_INSTANCE_IPV4": "10.0.1.1"},
	})
	regBody := servicediscoveryReadBody(t, regResp)
	require.Equal(t, http.StatusOK, regResp.StatusCode, "body: %s", regBody)

	// Set health to UNHEALTHY.
	updateResp := servicediscoveryRequest(t, "UpdateInstanceCustomHealthStatus", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-health-001",
		"Status":     "UNHEALTHY",
	})
	updateBody := servicediscoveryReadBody(t, updateResp)
	assert.Equal(t, http.StatusOK, updateResp.StatusCode, "body: %s", updateBody)

	// Set health back to HEALTHY.
	updateResp2 := servicediscoveryRequest(t, "UpdateInstanceCustomHealthStatus", map[string]any{
		"ServiceId":  svcID,
		"InstanceId": "i-health-001",
		"Status":     "HEALTHY",
	})
	updateBody2 := servicediscoveryReadBody(t, updateResp2)
	assert.Equal(t, http.StatusOK, updateResp2.StatusCode, "body: %s", updateBody2)
}

func TestIntegration_ServiceDiscovery_DiscoverInstancesRevision(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Create a namespace and service.
	createNsResp := servicediscoveryRequest(t, "CreateHttpNamespace", map[string]any{
		"Name": "integ-revision-ns",
	})
	createNsBody := servicediscoveryReadBody(t, createNsResp)
	require.Equal(t, http.StatusOK, createNsResp.StatusCode, "body: %s", createNsBody)

	var createNsResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(createNsBody), &createNsResult))
	opID := createNsResult["OperationId"].(string)

	opResp := servicediscoveryRequest(t, "GetOperation", map[string]any{"OperationId": opID})
	opBody := servicediscoveryReadBody(t, opResp)
	var opResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(opBody), &opResult))
	nsID := opResult["Operation"].(map[string]any)["Targets"].(map[string]any)["NAMESPACE"].(string)

	createSvcResp := servicediscoveryRequest(t, "CreateService", map[string]any{
		"Name":        "integ-revision-svc",
		"NamespaceId": nsID,
	})
	createSvcBody := servicediscoveryReadBody(t, createSvcResp)
	require.Equal(t, http.StatusOK, createSvcResp.StatusCode, "body: %s", createSvcBody)

	// Get initial revision.
	revResp := servicediscoveryRequest(t, "DiscoverInstancesRevision", map[string]any{
		"NamespaceName": "integ-revision-ns",
		"ServiceName":   "integ-revision-svc",
	})
	revBody := servicediscoveryReadBody(t, revResp)
	assert.Equal(t, http.StatusOK, revResp.StatusCode, "body: %s", revBody)
	assert.Contains(t, revBody, "InstancesRevision")
}

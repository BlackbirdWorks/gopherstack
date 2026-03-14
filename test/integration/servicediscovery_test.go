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

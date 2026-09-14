package integration_test

// This file deliberately does NOT use an azure-sdk-for-go ARM SDK client
// (e.g. sdk/resourcemanager/resources/armresources or
// sdk/resourcemanager/storage/armstorage): those clients' auth pipeline is
// built around azidentity's credential chain, which -- to be pointed at a
// custom ARM endpoint at all -- requires either a custom cloud.Configuration
// (itself requiring the exact environment-descriptor shape
// services/azurearm/metadata.go serves, plus a client transport that trusts
// the self-signed certificate) or bypassing auth entirely via a
// policy.ClientOptions{Cloud: ...} override that still expects TLS trust to
// be configured at the http.Client level. Standing up that pipeline just for
// an integration test duplicates most of what test/terraform/azure's
// hashicorp/azurerm run already proves end-to-end against a real Terraform
// provider. Mirroring test/integration/azureservicebus_test.go's own
// documented choice (that package is AMQP-only and has no REST transport),
// this file exercises the ARM REST surface directly via net/http with
// InsecureSkipVerify -- a real, if less turnkey, integration test of the
// exact wire behavior azurerm and other REST-based ARM clients depend on.

import (
	"crypto/tls"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// armInsecureClient is an *http.Client that trusts azureARMEndpoint's
// self-signed certificate (AZURE.md section 10.8) -- production clients
// instead rely on the system trust store plus SSL_CERT_FILE, as
// test/terraform/azure's harness does for the real hashicorp/azurerm
// provider.
func armInsecureClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	}
}

// armRequest performs one ARM REST call against azureARMEndpoint and returns
// the response, skipping the test if the endpoint isn't available (mirroring
// sbRequest's t.Skip pattern in azureservicebus_test.go).
func armRequest(t *testing.T, method, path string, body []byte) *http.Response {
	t.Helper()

	if azureARMEndpoint == "" {
		t.Skip("Azure ARM endpoint not available (mapped port could not be determined)")
	}

	var bodyReader *strings.Reader
	if body != nil {
		bodyReader = strings.NewReader(string(body))
	} else {
		bodyReader = strings.NewReader("")
	}

	req, err := http.NewRequestWithContext(t.Context(), method, azureARMEndpoint+path, bodyReader)
	require.NoError(t, err)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := armInsecureClient().Do(req)
	require.NoError(t, err)

	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

func armDecodeJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))

	return body
}

// TestIntegration_AzureARM_MetadataAndToken proves the discovery documents
// that terraform-provider-azurerm's provider initialization depends on are
// reachable over the real HTTPS listener, and that the client-credentials
// token endpoint issues a usable bearer token.
func TestIntegration_AzureARM_MetadataAndToken(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := armRequest(t, http.MethodGet, "/metadata/endpoints?api-version=2022-09-01", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var doc map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&doc))
	assert.Equal(t, "gopherstack", doc["name"])
	assert.NotEmpty(t, doc["resourceManagerEndpoint"])

	tenant := "00000000-0000-0000-0000-000000000000"

	resp = armRequest(t, http.MethodGet, "/"+tenant+"/v2.0/.well-known/openid-configuration", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	form := "grant_type=client_credentials&client_id=" + tenant +
		"&client_secret=gopherstack&scope=https://management.azure.com/.default"

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		azureARMEndpoint+"/"+tenant+"/oauth2/v2.0/token", strings.NewReader(form))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	tokenResp, err := armInsecureClient().Do(req)
	require.NoError(t, err)

	defer tokenResp.Body.Close()
	require.Equal(t, http.StatusOK, tokenResp.StatusCode)

	tokenBody := armDecodeJSON(t, tokenResp)
	assert.Equal(t, "Bearer", tokenBody["token_type"])
	assert.NotEmpty(t, tokenBody["access_token"])
}

// TestIntegration_AzureARM_ResourceGroupAndStorageAccountLifecycle drives
// the exact CRUD sequence terraform-provider-azurerm's azurerm_resource_group
// + azurerm_storage_account resources perform, end to end against a running
// gopherstack instance: create the resource group, create a storage account
// in it, fetch listKeys, then delete both.
func TestIntegration_AzureARM_ResourceGroupAndStorageAccountLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	sub := "00000000-0000-0000-0000-000000000000"
	rg := "test-rg-" + uuid.NewString()[:8]
	acct := "acct" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]

	base := "/subscriptions/" + sub

	// Create resource group.
	resp := armRequest(t, http.MethodPut, base+"/resourcegroups/"+rg, []byte(`{"location":"local"}`))
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	rgBody := armDecodeJSON(t, resp)
	assert.Equal(t, rg, rgBody["name"])

	// Create storage account.
	resourcePath := base + "/resourceGroups/" + rg + "/providers/Microsoft.Storage/storageAccounts/" + acct

	resp = armRequest(t, http.MethodPut, resourcePath,
		[]byte(`{"location":"local","sku":{"name":"Standard_LRS"},"kind":"StorageV2"}`))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	acctBody := armDecodeJSON(t, resp)
	assert.Equal(t, acct, acctBody["name"])

	props, ok := acctBody["properties"].(map[string]any)
	require.True(t, ok)

	endpoints, ok := props["primaryEndpoints"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, endpoints["blob"], acct)

	// Get storage account.
	resp = armRequest(t, http.MethodGet, resourcePath, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// listKeys.
	resp = armRequest(t, http.MethodPost, resourcePath+"/listKeys", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	keysBody := armDecodeJSON(t, resp)
	keys, ok := keysBody["keys"].([]any)
	require.True(t, ok)
	assert.Len(t, keys, 2)

	// Delete storage account, then resource group.
	resp = armRequest(t, http.MethodDelete, resourcePath, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = armRequest(t, http.MethodDelete, base+"/resourcegroups/"+rg, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestIntegration_AzureARM_ServiceBusLifecycleAndCrossServiceWiring drives
// the ARM control-plane CRUD for a Microsoft.ServiceBus namespace/queue/
// topic/subscription, then makes a direct HTTP call against
// services/azureservicebus's own listener (port 10003) to prove the
// ARM-created queue is genuinely queryable there -- i.e. that
// wireAzureARMResourceProviders's cross-service ServiceBusEntities adapter is
// really wired, before Terraform is even involved (AZURE.md section 10.10's
// M9 entry).
func TestIntegration_AzureARM_ServiceBusLifecycleAndCrossServiceWiring(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	sub := "00000000-0000-0000-0000-000000000000"
	rg := "test-sb-rg-" + uuid.NewString()[:8]
	ns := "sbns" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	queue := "sbq" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	topic := "sbt" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	sub2 := "subscription1"

	base := "/subscriptions/" + sub

	resp := armRequest(t, http.MethodPut, base+"/resourcegroups/"+rg, []byte(`{"location":"local"}`))
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	nsPath := base + "/resourceGroups/" + rg + "/providers/Microsoft.ServiceBus/namespaces/" + ns

	resp = armRequest(t, http.MethodPut, nsPath, []byte(`{"location":"local","sku":{"name":"Standard"}}`))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	nsBody := armDecodeJSON(t, resp)
	assert.Equal(t, ns, nsBody["name"])

	resp = armRequest(t, http.MethodGet, nsPath, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Create queue.
	queuePath := nsPath + "/queues/" + queue
	resp = armRequest(t, http.MethodPut, queuePath, []byte(`{"properties":{"lockDuration":"PT30S",`+
		`"defaultMessageTimeToLive":"P1D","maxDeliveryCount":10}}`))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = armRequest(t, http.MethodGet, queuePath, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Create topic + subscription.
	topicPath := nsPath + "/topics/" + topic
	resp = armRequest(t, http.MethodPut, topicPath, []byte(`{"properties":{"defaultMessageTimeToLive":"P1D"}}`))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	subPath := topicPath + "/subscriptions/" + sub2
	resp = armRequest(t, http.MethodPut, subPath, []byte(`{"properties":{"lockDuration":"PT1M","maxDeliveryCount":5}}`))
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = armRequest(t, http.MethodGet, subPath, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// listKeys via a namespace authorizationRule.
	ruleName := "RootManageSharedAccessKey"
	rulePath := nsPath + "/authorizationRules/" + ruleName

	resp = armRequest(t, http.MethodPut, rulePath, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = armRequest(t, http.MethodPost, rulePath+"/listKeys", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	keysBody := armDecodeJSON(t, resp)
	assert.NotEmpty(t, keysBody["primaryConnectionString"])
	assert.Equal(t, ruleName, keysBody["keyName"])

	// Cross-service assertion: the ARM-created queue must be genuinely
	// queryable against services/azureservicebus's own listener (port 10003,
	// azureServiceBusEndpoint) -- proving wireAzureARMResourceProviders's
	// ServiceBusEntities adapter really delegated CreateQueue through, not
	// just recorded local ARM bookkeeping.
	if azureServiceBusEndpoint == "" {
		t.Skip("Azure Service Bus endpoint not available (mapped port could not be determined)")
	}

	sbReq, err := http.NewRequestWithContext(t.Context(), http.MethodGet, azureServiceBusEndpoint+"/"+queue, nil)
	require.NoError(t, err)

	sbResp, err := http.DefaultClient.Do(sbReq)
	require.NoError(t, err)

	defer sbResp.Body.Close()
	assert.Equal(t, http.StatusOK, sbResp.StatusCode,
		"ARM-created queue %q should be genuinely queryable on services/azureservicebus's own listener", queue)

	// Cleanup.
	resp = armRequest(t, http.MethodDelete, subPath, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = armRequest(t, http.MethodDelete, topicPath, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = armRequest(t, http.MethodDelete, queuePath, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = armRequest(t, http.MethodDelete, nsPath, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = armRequest(t, http.MethodDelete, base+"/resourcegroups/"+rg, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

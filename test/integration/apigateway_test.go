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

func apigwPost(t *testing.T, action string, body any) *http.Response {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "APIGateway."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func apigwReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func apigwReadJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()

	var m map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&m))

	return m
}

func TestIntegration_APIGateway_FullLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// 1. CreateRestApi
	resp := apigwPost(t, "CreateRestApi", map[string]any{
		"name":        "integ-test-api",
		"description": "integration test REST API",
	})
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	created := apigwReadJSON(t, resp)
	apiID, ok := created["id"].(string)
	require.True(t, ok, "id should be a string")
	assert.NotEmpty(t, apiID)
	assert.Equal(t, "integ-test-api", created["name"])

	// 2. GetRestApi
	resp2 := apigwPost(t, "GetRestApi", map[string]any{"restApiId": apiID})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	got := apigwReadJSON(t, resp2)
	assert.Equal(t, apiID, got["id"])

	// 3. GetRestApis (list)
	resp3 := apigwPost(t, "GetRestApis", map[string]any{})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	list := apigwReadJSON(t, resp3)
	assert.Contains(t, list, "item")

	// 4. GetResources — should return at least the root resource
	resp4 := apigwPost(t, "GetResources", map[string]any{"restApiId": apiID})
	assert.Equal(t, http.StatusOK, resp4.StatusCode)
	resources := apigwReadJSON(t, resp4)
	items, ok := resources["item"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, items)

	// Extract root resource ID
	rootRes, ok := items[0].(map[string]any)
	require.True(t, ok)
	rootID, ok := rootRes["id"].(string)
	require.True(t, ok)

	// 5. CreateResource (child of root)
	resp5 := apigwPost(t, "CreateResource", map[string]any{
		"restApiId": apiID,
		"parentId":  rootID,
		"pathPart":  "items",
	})
	assert.Equal(t, http.StatusCreated, resp5.StatusCode)
	newRes := apigwReadJSON(t, resp5)
	resID, ok := newRes["id"].(string)
	require.True(t, ok)
	assert.Equal(t, "items", newRes["pathPart"])

	// 6. PutMethod on the new resource
	resp6 := apigwPost(t, "PutMethod", map[string]any{
		"restApiId":         apiID,
		"resourceId":        resID,
		"httpMethod":        "GET",
		"authorizationType": "NONE",
	})
	assert.Equal(t, http.StatusCreated, resp6.StatusCode)

	// 7. GetMethod
	resp7 := apigwPost(t, "GetMethod", map[string]any{
		"restApiId":  apiID,
		"resourceId": resID,
		"httpMethod": "GET",
	})
	assert.Equal(t, http.StatusOK, resp7.StatusCode)
	method := apigwReadJSON(t, resp7)
	assert.Equal(t, "GET", method["httpMethod"])

	// 8. PutIntegration (HTTP_PROXY type)
	resp8 := apigwPost(t, "PutIntegration", map[string]any{
		"restApiId":  apiID,
		"resourceId": resID,
		"httpMethod": "GET",
		"type":       "HTTP_PROXY",
		"uri":        "https://httpbin.org/get",
	})
	assert.Equal(t, http.StatusCreated, resp8.StatusCode)
	integration := apigwReadJSON(t, resp8)
	assert.Equal(t, "HTTP_PROXY", integration["type"])

	// 9. GetIntegration
	resp9 := apigwPost(t, "GetIntegration", map[string]any{
		"restApiId":  apiID,
		"resourceId": resID,
		"httpMethod": "GET",
	})
	assert.Equal(t, http.StatusOK, resp9.StatusCode)
	integ := apigwReadJSON(t, resp9)
	assert.Equal(t, "HTTP_PROXY", integ["type"])

	// 10. CreateDeployment
	resp10 := apigwPost(t, "CreateDeployment", map[string]any{
		"restApiId":   apiID,
		"stageName":   "prod",
		"description": "production deployment",
	})
	assert.Equal(t, http.StatusCreated, resp10.StatusCode)
	deployment := apigwReadJSON(t, resp10)
	assert.NotEmpty(t, deployment["id"])

	// 11. GetDeployments
	resp11 := apigwPost(t, "GetDeployments", map[string]any{"restApiId": apiID})
	assert.Equal(t, http.StatusOK, resp11.StatusCode)
	deploys := apigwReadJSON(t, resp11)
	assert.Contains(t, deploys, "item")

	// 12. GetStages — should include the "prod" stage created with deployment
	resp12 := apigwPost(t, "GetStages", map[string]any{"restApiId": apiID})
	assert.Equal(t, http.StatusOK, resp12.StatusCode)
	stages := apigwReadJSON(t, resp12)
	stageItems, ok := stages["item"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, stageItems, "expected at least one stage")

	var foundProd bool
	for _, s := range stageItems {
		sm, isSM := s.(map[string]any)
		if isSM && sm["stageName"] == "prod" {
			foundProd = true
		}
	}
	assert.True(t, foundProd, "expected prod stage")

	// 13. GetStage
	resp13 := apigwPost(t, "GetStage", map[string]any{
		"restApiId": apiID,
		"stageName": "prod",
	})
	assert.Equal(t, http.StatusOK, resp13.StatusCode)
	stage := apigwReadJSON(t, resp13)
	assert.Equal(t, "prod", stage["stageName"])

	// 14. DeleteStage
	resp14 := apigwPost(t, "DeleteStage", map[string]any{
		"restApiId": apiID,
		"stageName": "prod",
	})
	assert.Equal(t, http.StatusNoContent, resp14.StatusCode)
	resp14.Body.Close()

	// 15. DeleteResource
	resp15 := apigwPost(t, "DeleteResource", map[string]any{
		"restApiId":  apiID,
		"resourceId": resID,
	})
	assert.Equal(t, http.StatusNoContent, resp15.StatusCode)
	resp15.Body.Close()

	// 16. DeleteRestApi
	resp16 := apigwPost(t, "DeleteRestApi", map[string]any{"restApiId": apiID})
	assert.Equal(t, http.StatusAccepted, resp16.StatusCode)
	resp16.Body.Close()

	// Verify deletion
	resp17 := apigwPost(t, "GetRestApi", map[string]any{"restApiId": apiID})
	assert.Equal(t, http.StatusNotFound, resp17.StatusCode)
	resp17.Body.Close()
}

func TestIntegration_APIGateway_CreateRestApi_EmptyName(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := apigwPost(t, "CreateRestApi", map[string]any{"name": ""})
	defer resp.Body.Close()
	body := apigwReadBody(t, resp)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body: %s", body)
}

func TestIntegration_APIGateway_GetRestApi_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := apigwPost(t, "GetRestApi", map[string]any{"restApiId": "notexist"})
	defer resp.Body.Close()
	body := apigwReadBody(t, resp)

	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "body: %s", body)
}

func TestIntegration_APIGateway_UnknownOperation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := apigwPost(t, "UnknownOp", map[string]any{})
	defer resp.Body.Close()
	body := apigwReadBody(t, resp)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body: %s", body)
}

// apigwDo sends a GET request to the mock server endpoint at the given path.
func apigwDo(t *testing.T, path string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint+path, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

// apigwSetupAPI creates an API Gateway REST API with one resource and returns
// (apiID, rootResourceID).
func apigwSetupAPI(t *testing.T, name string) (string, string) {
	t.Helper()

	resp := apigwPost(t, "CreateRestApi", map[string]any{"name": name})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	created := apigwReadJSON(t, resp)
	apiID := created["id"].(string)

	resp = apigwPost(t, "GetResources", map[string]any{"restApiId": apiID})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resources := apigwReadJSON(t, resp)
	items := resources["item"].([]any)
	rootID := items[0].(map[string]any)["id"].(string)

	return apiID, rootID
}

func TestIntegration_APIGateway_DataPlane_MockIntegration(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	apiID, rootID := apigwSetupAPI(t, "dataplane-mock-test")

	// Create a /ping resource.
	resp := apigwPost(t, "CreateResource", map[string]any{
		"restApiId": apiID,
		"parentId":  rootID,
		"pathPart":  "ping",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	res := apigwReadJSON(t, resp)
	resID := res["id"].(string)

	// Add GET method.
	resp = apigwPost(t, "PutMethod", map[string]any{
		"restApiId":         apiID,
		"resourceId":        resID,
		"httpMethod":        "GET",
		"authorizationType": "NONE",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	// Add MOCK integration.
	resp = apigwPost(t, "PutIntegration", map[string]any{
		"restApiId":  apiID,
		"resourceId": resID,
		"httpMethod": "GET",
		"type":       "MOCK",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	// Deploy.
	resp = apigwPost(t, "CreateDeployment", map[string]any{
		"restApiId": apiID,
		"stageName": "test",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	// Invoke via _user_request_.
	userResp := apigwDo(t,
		"/restapis/"+apiID+"/test/_user_request_/ping")
	defer userResp.Body.Close()

	assert.Equal(t, http.StatusOK, userResp.StatusCode)
}

func TestIntegration_APIGateway_DataPlane_PathVariable(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	apiID, rootID := apigwSetupAPI(t, "dataplane-pathvar-test")

	// Create /items/{id} resource.
	resp := apigwPost(t, "CreateResource", map[string]any{
		"restApiId": apiID,
		"parentId":  rootID,
		"pathPart":  "items",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	itemsRes := apigwReadJSON(t, resp)
	itemsID := itemsRes["id"].(string)

	resp = apigwPost(t, "CreateResource", map[string]any{
		"restApiId": apiID,
		"parentId":  itemsID,
		"pathPart":  "{id}",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	idRes := apigwReadJSON(t, resp)
	idResID := idRes["id"].(string)

	// Add GET method with MOCK integration.
	resp = apigwPost(t, "PutMethod", map[string]any{
		"restApiId":         apiID,
		"resourceId":        idResID,
		"httpMethod":        "GET",
		"authorizationType": "NONE",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	resp = apigwPost(t, "PutIntegration", map[string]any{
		"restApiId":  apiID,
		"resourceId": idResID,
		"httpMethod": "GET",
		"type":       "MOCK",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	// Deploy.
	resp = apigwPost(t, "CreateDeployment", map[string]any{
		"restApiId": apiID,
		"stageName": "test",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	// Invoke /items/42 — should match {id}.
	userResp := apigwDo(t,
		"/restapis/"+apiID+"/test/_user_request_/items/42")
	defer userResp.Body.Close()

	assert.Equal(t, http.StatusOK, userResp.StatusCode)

	// Invoke /items/99 — should also match.
	userResp2 := apigwDo(t,
		"/restapis/"+apiID+"/test/_user_request_/items/99")
	defer userResp2.Body.Close()

	assert.Equal(t, http.StatusOK, userResp2.StatusCode)

	// Invoke /items/unknown/path — should NOT match (wrong depth).
	userResp3 := apigwDo(t,
		"/restapis/"+apiID+"/test/_user_request_/items/99/extra")
	defer userResp3.Body.Close()

	assert.Equal(t, http.StatusNotFound, userResp3.StatusCode)
}

package apigatewayv2_test

// parity_c_test.go — §C parity: route fields, deployment stage validation,
// and CreateAPI apiKeySelectionExpression defaults.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Route: apiKeyRequired field
// ---------------------------------------------------------------------------

// TestParity_CreateRoute_APIKeyRequiredPresentAndFalse verifies that every
// CreateRoute response includes apiKeyRequired=false even when the caller
// does not set it.  Real AWS always returns this field.
func TestParity_CreateRoute_APIKeyRequiredPresentAndFalse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "test-api")

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes",
		map[string]any{"routeKey": "GET /items"})

	require.Equal(t, http.StatusCreated, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	v, hasKey := resp["apiKeyRequired"]
	assert.True(t, hasKey, "apiKeyRequired should be present in response")
	assert.Equal(t, false, v, "apiKeyRequired should be false by default")
}

// ---------------------------------------------------------------------------
// Route: authorizationScopes field
// ---------------------------------------------------------------------------

// TestParity_CreateRoute_AuthorizationScopesIsEmptyArray verifies that
// authorizationScopes is always [] (never null or absent) in CreateRoute
// responses.  Real AWS returns an empty array by default.
func TestParity_CreateRoute_AuthorizationScopesIsEmptyArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "scopes-api")

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes",
		map[string]any{"routeKey": "POST /orders"})

	require.Equal(t, http.StatusCreated, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	raw, hasKey := resp["authorizationScopes"]
	assert.True(t, hasKey, "authorizationScopes should be present in response")

	scopes, ok := raw.([]any)
	assert.True(t, ok, "authorizationScopes should be a JSON array, got %T", raw)
	assert.Empty(t, scopes, "authorizationScopes should be empty array by default")
}

// TestParity_GetRoute_AuthorizationScopesIsEmptyArray verifies the same
// guarantee on a GetRoute round-trip.
func TestParity_GetRoute_AuthorizationScopesIsEmptyArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "get-scopes-api")

	// Create a route.
	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/routes",
		map[string]any{"routeKey": "GET /ping"})
	require.Equal(t, http.StatusCreated, rr.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &created))
	routeID, _ := created["routeId"].(string)
	require.NotEmpty(t, routeID)

	// Get the route back.
	rr2 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/routes/"+routeID, nil)
	require.Equal(t, http.StatusOK, rr2.Code)

	var got map[string]any
	require.NoError(t, json.Unmarshal(rr2.Body.Bytes(), &got))

	raw, hasKey := got["authorizationScopes"]
	assert.True(t, hasKey, "authorizationScopes should be present in GetRoute response")

	scopes, ok := raw.([]any)
	assert.True(t, ok, "authorizationScopes should be a JSON array")
	assert.Empty(t, scopes)
}

// ---------------------------------------------------------------------------
// CreateAPI: apiKeySelectionExpression default
// ---------------------------------------------------------------------------

// TestParity_CreateAPI_HTTP_DefaultAPIKeySelectionExpression verifies that
// HTTP APIs automatically get $request.header.x-api-key when the caller
// omits apiKeySelectionExpression.
func TestParity_CreateAPI_HTTP_DefaultAPIKeySelectionExpression(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name":         "http-api",
		"protocolType": "HTTP",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.Equal(t, "$request.header.x-api-key", resp["apiKeySelectionExpression"])
}

// TestParity_CreateAPI_WebSocket_DefaultAPIKeySelectionExpression verifies
// that WEBSOCKET APIs automatically get $context.authorizer.usageIdentifierKey.
func TestParity_CreateAPI_WebSocket_DefaultAPIKeySelectionExpression(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name":         "ws-api",
		"protocolType": "WEBSOCKET",
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.Equal(t, "$context.authorizer.usageIdentifierKey", resp["apiKeySelectionExpression"])
}

// TestParity_CreateAPI_ExplicitAPIKeySelectionExpressionPreserved verifies
// that an explicitly supplied apiKeySelectionExpression is not overridden.
func TestParity_CreateAPI_ExplicitAPIKeySelectionExpressionPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	custom := "$context.identity.apiKey"

	rr := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{
		"name":                      "custom-api",
		"protocolType":              "HTTP",
		"apiKeySelectionExpression": custom,
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.Equal(t, custom, resp["apiKeySelectionExpression"])
}

// ---------------------------------------------------------------------------
// CreateDeployment: invalid stageName returns 404
// ---------------------------------------------------------------------------

// TestParity_CreateDeployment_InvalidStageName404 verifies that supplying a
// non-existent stageName in CreateDeployment returns HTTP 404, not 500.
// Real AWS returns NotFoundException when the stage does not exist.
func TestParity_CreateDeployment_InvalidStageName404(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "deploy-api")

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/deployments",
		map[string]any{"stageName": "nonexistent-stage"})

	assert.Equal(t, http.StatusNotFound, rr.Code,
		"CreateDeployment with unknown stageName must return 404, got body: %s", rr.Body.String())
}

// TestParity_CreateDeployment_ValidStageName201 verifies the happy path:
// when the stage exists the deployment is created successfully.
func TestParity_CreateDeployment_ValidStageName201(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	apiID := createAPI(t, h, "deploy-api2")

	// Create a stage first.
	rrStage := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/stages",
		map[string]any{"stageName": "prod"})
	require.Equal(t, http.StatusCreated, rrStage.Code)

	// Create a deployment referencing that stage.
	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/deployments",
		map[string]any{"stageName": "prod"})
	assert.Equal(t, http.StatusCreated, rr.Code)

	var dep map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &dep))
	assert.Equal(t, "DEPLOYED", dep["deploymentStatus"])
}

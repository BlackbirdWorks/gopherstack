package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// boostSetup is a local helper that creates a shared handler+echo for multi-step tests.
func boostSetup() (*apigateway.Handler, *echo.Echo) {
	backend := apigateway.NewInMemoryBackend()
	handler := apigateway.NewHandler(backend)
	e := echo.New()

	return handler, e
}

// boostAPI creates a REST API and returns its ID.
func boostAPI(t *testing.T, handler *apigateway.Handler, e *echo.Echo) string {
	t.Helper()
	rec := postWithHandler(t, handler, e, "CreateRestApi", `{"name":"boost-api"}`)
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["id"].(string)
}

// boostRootResource returns the root resource ID for an API.
func boostRootResource(t *testing.T, handler *apigateway.Handler, e *echo.Echo, apiID string) string {
	t.Helper()
	rec := postWithHandler(t, handler, e, "GetResources", fmt.Sprintf(`{"restApiId":%q}`, apiID))
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["item"].([]any)
	require.NotEmpty(t, items)

	return items[0].(map[string]any)["id"].(string)
}

// boostDeployment creates a deployment and returns its ID.
func boostDeployment(t *testing.T, handler *apigateway.Handler, e *echo.Echo, apiID, stageName string) string {
	t.Helper()
	rec := postWithHandler(t, handler, e, "CreateDeployment",
		fmt.Sprintf(`{"restApiId":%q,"stageName":%q}`, apiID, stageName))
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["id"].(string)
}

// boostAuthorizer creates an authorizer and returns its ID.
func boostAuthorizer(t *testing.T, handler *apigateway.Handler, e *echo.Echo, apiID string) string {
	t.Helper()
	rec := postWithHandler(
		t,
		handler,
		e,
		"CreateAuthorizer",
		fmt.Sprintf(
			`{"restApiId":%q,"name":"auth","type":"TOKEN","authorizerUri":"arn:aws:lambda:us-east-1:123:function:auth"}`,
			apiID,
		),
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["id"].(string)
}

// boostDocPart creates a documentation part and returns its ID.
func boostDocPart(t *testing.T, handler *apigateway.Handler, e *echo.Echo, apiID string) string {
	t.Helper()
	rec := postWithHandler(t, handler, e, "CreateDocumentationPart",
		fmt.Sprintf(`{"restApiId":%q,"location":{"type":"API"},"properties":"{}"}`, apiID))
	require.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["id"].(string)
}

// boostDocVersion creates a documentation version and returns its name.
func boostDocVersion(t *testing.T, handler *apigateway.Handler, e *echo.Echo, apiID, version string) {
	t.Helper()
	rec := postWithHandler(t, handler, e, "CreateDocumentationVersion",
		fmt.Sprintf(`{"restApiId":%q,"documentationVersion":%q}`, apiID, version))
	require.Equal(t, http.StatusCreated, rec.Code)
}

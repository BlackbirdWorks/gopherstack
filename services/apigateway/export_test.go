package apigateway_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestAPIGateway_GetExport_RESTRoute(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "export-api"})
	require.NoError(t, err)
	_, err = backend.CreateDeployment(api.ID, "prod", "")
	require.NoError(t, err)

	rec := restCall(t, h, http.MethodGet, "/restapis/"+api.ID+"/stages/prod/exports/oas30", "", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc))
	assert.Equal(t, "3.0.1", doc["openapi"])
}

func TestGetExport_OAS30_IncludesOperations(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "export-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	child, _ := b.CreateResource(api.ID, rootID, "items")

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        child.ID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
		OperationName:     "ListItems",
		RequestModels:     map[string]string{"application/json": "Empty"},
	})
	_, _ = b.PutIntegration(api.ID, child.ID, "GET", apigateway.PutIntegrationInput{
		Type:       "AWS_PROXY",
		HTTPMethod: "POST",
		URI:        "arn:aws:lambda:::function:items-fn",
	})
	_, _ = b.PutMethodResponse(api.ID, child.ID, "GET", "200", apigateway.PutMethodResponseInput{
		ResponseModels: map[string]string{"application/json": "Empty"},
	})
	depl, _ := b.CreateDeployment(api.ID, "prod", "v1")
	_ = depl

	doc, err := b.GetExport(api.ID, "prod", "oas30")
	require.NoError(t, err)

	assert.Equal(t, "3.0.1", doc["openapi"])

	paths, ok := doc["paths"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, paths, "/items")

	pathItem, ok := paths["/items"].(map[string]any)
	require.True(t, ok)

	op, ok := pathItem["get"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ListItems", op["operationId"])
}

func TestGetExport_Swagger20(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "sw-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	child, _ := b.CreateResource(api.ID, rootID, "users")

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        child.ID,
		HTTPMethod:        "POST",
		AuthorizationType: "NONE",
		APIKeyRequired:    true,
	})
	_, _ = b.PutIntegration(api.ID, child.ID, "POST", apigateway.PutIntegrationInput{Type: "MOCK"})

	doc, err := b.GetExport(api.ID, "prod", "swagger")
	require.NoError(t, err)

	assert.Equal(t, "2.0", doc["swagger"])
	paths, _ := doc["paths"].(map[string]any)
	pathItem, _ := paths["/users"].(map[string]any)
	op, _ := pathItem["post"].(map[string]any)
	security, _ := op["security"].([]map[string]any)
	require.Len(t, security, 1)
	assert.Contains(t, security[0], "api_key")
}

package apigateway_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
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

// TestAPIGateway_GetExport_HeadersNotBody_RealClient covers gopherstack-eax4:
// GetExportOutput shares GetSdk's wire shape -- ContentType/ContentDisposition
// are HTTP response headers (awsRestjson1_deserializeOpHttpBindingsGetExportOutput,
// apigateway@v1.42.4 deserializers.go:10166) and Body is the raw payload
// (awsRestjson1_deserializeOpDocumentGetExportOutput, deserializers.go:10183),
// never JSON fields. Unlike GetSdk, the old handler's body was never
// double-wrapped (the export map was already the sole JSON payload), so this
// only closes the missing Content-Disposition header; Content-Type happened
// to already read "application/json" by coincidence, since this emulator
// only ever produces a JSON export document.
func TestAPIGateway_GetExport_HeadersNotBody_RealClient(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "export-api"})
	require.NoError(t, err)
	_, err = backend.CreateDeployment(api.ID, "prod", "")
	require.NoError(t, err)

	client := newTestAPIGatewayClient(t, h)

	var rawContentType string

	out, err := client.GetExport(t.Context(), &apigatewaysdk.GetExportInput{
		RestApiId:  aws.String(api.ID),
		StageName:  aws.String("prod"),
		ExportType: aws.String("oas30"),
	}, captureRawResponseHeader("Content-Type", &rawContentType))
	require.NoError(t, err)
	require.NotNil(t, out)

	assert.NotEmpty(t, rawContentType, "wire Content-Type header must be set")

	require.NotNil(t, out.ContentDisposition)
	assert.NotEmpty(t, *out.ContentDisposition,
		"ContentDisposition must round-trip via the real Content-Disposition response header")

	require.NotEmpty(t, out.Body, "Body must round-trip as raw bytes, not a base64 JSON field")

	var doc map[string]any
	require.NoError(t, json.Unmarshal(out.Body, &doc))
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

package apigateway_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

const testSwaggerSpec = `{
  "swagger": "2.0",
  "info": {"title": "Pet Store", "description": "A sample API"},
  "paths": {
    "/pets": {
      "get": {"summary": "list pets"},
      "post": {"summary": "create pet"}
    },
    "/pets/{petId}": {
      "get": {"summary": "get pet"}
    }
  }
}`

const testSwaggerSpecYAML = `
swagger: "2.0"
info:
  title: YAML API
  description: from yaml
paths:
  /widgets:
    get:
      summary: list widgets
`

// ----- Direct action dispatch (JSON-protocol convenience path) -----

func TestAPIGateway_ImportRestApi_DirectDispatch(t *testing.T) {
	t.Parallel()

	rec := post(t, "ImportRestApi", testSwaggerSpec)
	require.Equal(t, http.StatusCreated, rec.Code)

	var api map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &api))

	assert.Equal(t, "Pet Store", api["name"])
	assert.Equal(t, "A sample API", api["description"])
	assert.NotEmpty(t, api["id"])
	assert.NotEmpty(t, api["rootResourceId"])
}

func TestAPIGateway_ImportRestApi_YAML(t *testing.T) {
	t.Parallel()

	rec := post(t, "ImportRestApi", testSwaggerSpecYAML)
	require.Equal(t, http.StatusCreated, rec.Code)

	var api map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &api))
	assert.Equal(t, "YAML API", api["name"])
	assert.Equal(t, "from yaml", api["description"])
}

func TestAPIGateway_ImportRestApi_CreatesResourcesFromPaths(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	handler := apigateway.NewHandler(backend)
	e := echo.New()

	rec := postWithHandler(t, handler, e, "ImportRestApi", testSwaggerSpec)
	require.Equal(t, http.StatusCreated, rec.Code)

	var api map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &api))
	apiID, _ := api["id"].(string)
	require.NotEmpty(t, apiID)

	resources, _, err := backend.GetResources(apiID, "", 0)
	require.NoError(t, err)

	// root ("/") + "pets" + "{petId}" = 3 resources.
	require.Len(t, resources, 3)

	paths := make(map[string]apigateway.Resource, len(resources))
	for _, r := range resources {
		paths[r.Path] = r
	}

	petsRes, ok := paths["/pets"]
	require.True(t, ok)
	assert.Contains(t, petsRes.ResourceMethods, "GET")
	assert.Contains(t, petsRes.ResourceMethods, "POST")

	petIDRes, ok := paths["/pets/{petId}"]
	require.True(t, ok)
	assert.Contains(t, petIDRes.ResourceMethods, "GET")
	assert.Equal(t, "NONE", petIDRes.ResourceMethods["GET"].AuthorizationType)
}

func TestAPIGateway_ImportRestApi_InvalidBody(t *testing.T) {
	t.Parallel()

	rec := post(t, "ImportRestApi", "not json or yaml: [unterminated")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAPIGateway_ImportRestApi_EmptyBody(t *testing.T) {
	t.Parallel()

	rec := post(t, "ImportRestApi", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAPIGateway_PutRestApi_DirectDispatch_Merge(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	handler := apigateway.NewHandler(backend)
	e := echo.New()

	created, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "existing-api"})
	require.NoError(t, err)

	existingResource, err := backend.CreateResource(created.ID, created.RootResourceID, "existing")
	require.NoError(t, err)
	_, err = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID: created.ID, ResourceID: existingResource.ID, HTTPMethod: "GET", AuthorizationType: "NONE",
	})
	require.NoError(t, err)

	body := `{"restApiId":"` + created.ID + `","swagger":"2.0","info":{"title":"merged"},` +
		`"paths":{"/pets":{"get":{}}}}`

	rec := postWithHandler(t, handler, e, "PutRestApi", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var api map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &api))
	assert.Equal(t, "merged", api["name"])

	resources, _, err := backend.GetResources(created.ID, "", 0)
	require.NoError(t, err)

	paths := make([]string, 0, len(resources))
	for _, r := range resources {
		paths = append(paths, r.Path)
	}

	// merge mode keeps the pre-existing resource and adds the new one.
	assert.Contains(t, paths, "/existing")
	assert.Contains(t, paths, "/pets")
}

func TestAPIGateway_PutRestApi_NotFound(t *testing.T) {
	t.Parallel()

	body := `{"restApiId":"does-not-exist","swagger":"2.0","info":{"title":"x"},"paths":{}}`
	rec := post(t, "PutRestApi", body)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ----- REST-style routing (the shape real aws-sdk-go-v2 requests take) -----

func TestAPIGateway_ImportRestApi_RESTRoute(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/restapis?mode=import", strings.NewReader(testSwaggerSpec))
	req.Header.Set("Content-Type", "application/octet-stream")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusCreated, rec.Code)

	var api map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &api))
	assert.Equal(t, "Pet Store", api["name"])

	// A plain POST /restapis (no mode=import) must still route to CreateRestApi
	// and must NOT be treated as an import of its JSON body.
	req2 := httptest.NewRequest(http.MethodPost, "/restapis", strings.NewReader(`{"name":"plain-create"}`))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	require.NoError(t, h.Handler()(c2))
	require.Equal(t, http.StatusCreated, rec2.Code)

	var api2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &api2))
	assert.Equal(t, "plain-create", api2["name"])
}

func TestAPIGateway_PutRestApi_RESTRoute_Overwrite(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	created, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "to-overwrite"})
	require.NoError(t, err)

	existingResource, err := backend.CreateResource(created.ID, created.RootResourceID, "old-path")
	require.NoError(t, err)
	_, err = backend.PutMethod(apigateway.PutMethodInput{
		RestAPIID: created.ID, ResourceID: existingResource.ID, HTTPMethod: "GET", AuthorizationType: "NONE",
	})
	require.NoError(t, err)

	req := httptest.NewRequest(
		http.MethodPut, "/restapis/"+created.ID+"?mode=overwrite", strings.NewReader(testSwaggerSpec),
	)
	req.Header.Set("Content-Type", "application/octet-stream")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusOK, rec.Code)

	var api map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &api))
	assert.Equal(t, "Pet Store", api["name"])

	resources, _, err := backend.GetResources(created.ID, "", 0)
	require.NoError(t, err)

	paths := make([]string, 0, len(resources))
	for _, r := range resources {
		paths = append(paths, r.Path)
	}

	// overwrite mode replaces the resource tree: the old path is gone, new ones present.
	assert.NotContains(t, paths, "/old-path")
	assert.Contains(t, paths, "/pets")
	assert.Contains(t, paths, "/pets/{petId}")
}

func TestAPIGateway_PutRestApi_RESTRoute_NotFound(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	req := httptest.NewRequest(
		http.MethodPut, "/restapis/nonexistent?mode=overwrite", strings.NewReader(testSwaggerSpec),
	)
	req.Header.Set("Content-Type", "application/octet-stream")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

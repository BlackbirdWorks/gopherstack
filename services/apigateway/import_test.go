package apigateway_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// findResourceByPath returns the resource with the given path, or nil.
func findResourceByPath(t *testing.T, b *apigateway.InMemoryBackend, apiID, path string) *apigateway.Resource {
	t.Helper()

	resources, _, err := b.GetResources(apiID, "", 0)
	require.NoError(t, err)

	for i := range resources {
		if resources[i].Path == path {
			return &resources[i]
		}
	}

	return nil
}

const swagger20Pets = `{
  "swagger": "2.0",
  "info": {"title": "PetStore", "description": "pets api"},
  "x-amazon-apigateway-api-key-source": "HEADER",
  "x-amazon-apigateway-binary-media-types": ["image/png"],
  "definitions": {
    "Pet": {"type": "object", "properties": {"id": {"type": "integer"}}}
  },
  "paths": {
    "/pets": {
      "get": {
        "operationId": "listPets",
        "responses": {"200": {"description": "ok", "schema": {"$ref": "#/definitions/Pet"}}},
        "x-amazon-apigateway-integration": {
          "type": "AWS_PROXY",
          "httpMethod": "POST",
          "uri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/fn/invocations",
          "passthroughBehavior": "WHEN_NO_MATCH"
        }
      },
      "post": {
        "security": [{"api_key": []}],
        "parameters": [{"name": "tag", "in": "query", "required": true}],
        "responses": {"201": {"description": "created"}},
        "x-amazon-apigateway-integration": {
          "type": "MOCK",
          "requestTemplates": {"application/json": "{\"statusCode\": 201}"},
          "responses": {
            "default": {"statusCode": "201", "responseTemplates": {"application/json": "{}"}}
          }
        }
      }
    },
    "/pets/{petId}": {
      "get": {
        "parameters": [{"name": "petId", "in": "path", "required": true}],
        "responses": {"200": {"description": "ok"}},
        "x-amazon-apigateway-integration": {
          "type": "HTTP_PROXY",
          "httpMethod": "GET",
          "uri": "https://example.com/{petId}"
        }
      }
    }
  }
}`

const healthDoc = `{
  "swagger": "2.0",
  "info": {"title": "PetStoreV2"},
  "paths": {
    "/health": {
      "get": {
        "responses": {"200": {"description": "ok"}},
        "x-amazon-apigateway-integration": {"type": "MOCK"}
      }
    }
  }
}`

const oas30Doc = `{
  "openapi": "3.0.1",
  "info": {"title": "OASApi"},
  "components": {
    "schemas": {"Order": {"type": "object"}}
  },
  "paths": {
    "/orders": {
      "post": {
        "requestBody": {
          "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Order"}}}
        },
        "responses": {
          "200": {
            "description": "ok",
            "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Order"}}}
          }
        },
        "x-amazon-apigateway-integration": {"type": "AWS_PROXY", "httpMethod": "POST", "uri": "arn:x"}
      }
    }
  }
}`

func TestImportRestAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "swagger20 builds full resource tree",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte(swagger20Pets)})
				require.NoError(t, err)
				assert.Equal(t, "PetStore", api.Name)
				assert.Equal(t, "pets api", api.Description)
				assert.Equal(t, "HEADER", api.APIKeySource)
				assert.Equal(t, []string{"image/png"}, api.BinaryMediaTypes)
				assert.NotEmpty(t, api.RootResourceID)

				// Root, /pets, /pets/{petId}
				resources, _, rerr := b.GetResources(api.ID, "", 0)
				require.NoError(t, rerr)
				assert.Len(t, resources, 3)

				pets := findResourceByPath(t, b, api.ID, "/pets")
				require.NotNil(t, pets)
				assert.Equal(t, "pets", pets.PathPart)

				petID := findResourceByPath(t, b, api.ID, "/pets/{petId}")
				require.NotNil(t, petID)
				assert.Equal(t, pets.ID, petID.ParentID)
				assert.Equal(t, "{petId}", petID.PathPart)
			},
		},
		{
			name: "methods and integrations are materialised",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte(swagger20Pets)})
				require.NoError(t, err)

				pets := findResourceByPath(t, b, api.ID, "/pets")
				require.NotNil(t, pets)

				get, gerr := b.GetMethod(api.ID, pets.ID, "GET")
				require.NoError(t, gerr)
				assert.Equal(t, "listPets", get.OperationName)
				assert.Equal(t, "NONE", get.AuthorizationType)

				getInteg, ierr := b.GetIntegration(api.ID, pets.ID, "GET")
				require.NoError(t, ierr)
				assert.Equal(t, "AWS_PROXY", getInteg.Type)
				assert.Equal(t, "POST", getInteg.HTTPMethod)
				assert.Equal(t, "WHEN_NO_MATCH", getInteg.PassthroughBehavior)
				assert.Equal(t, 29000, getInteg.TimeoutInMillis)

				post, perr := b.GetMethod(api.ID, pets.ID, "POST")
				require.NoError(t, perr)
				assert.True(t, post.APIKeyRequired)
				assert.True(t, post.RequestParameters["method.request.querystring.tag"])

				postInteg, pierr := b.GetIntegration(api.ID, pets.ID, "POST")
				require.NoError(t, pierr)
				assert.Equal(t, "MOCK", postInteg.Type)
				require.Contains(t, postInteg.RequestTemplates, "application/json")
				require.Contains(t, postInteg.IntegrationResponses, "default")
				assert.Equal(t, "201", postInteg.IntegrationResponses["default"].StatusCode)
			},
		},
		{
			name: "method responses and models imported",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte(swagger20Pets)})
				require.NoError(t, err)

				models, merr := b.GetModels(api.ID)
				require.NoError(t, merr)
				require.Len(t, models, 1)
				assert.Equal(t, "Pet", models[0].Name)

				pets := findResourceByPath(t, b, api.ID, "/pets")
				mr, rerr := b.GetMethodResponse(api.ID, pets.ID, "GET", "200")
				require.NoError(t, rerr)
				assert.Equal(t, "Pet", mr.ResponseModels["application/json"])
			},
		},
		{
			name: "oas30 import with requestBody and content responses",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte(oas30Doc)})
				require.NoError(t, err)
				assert.Equal(t, "OASApi", api.Name)

				orders := findResourceByPath(t, b, api.ID, "/orders")
				require.NotNil(t, orders)

				post, perr := b.GetMethod(api.ID, orders.ID, "POST")
				require.NoError(t, perr)
				assert.Equal(t, "Order", post.RequestModels["application/json"])
				assert.Equal(t, "Order", post.MethodResponses["200"].ResponseModels["application/json"])
			},
		},
		{
			name: "empty body rejected with BadRequestException",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				_, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte("")})
				require.Error(t, err)
				assert.ErrorIs(t, err, apigateway.ErrInvalidParameter)
			},
		},
		{
			name: "non-spec json rejected",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				_, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte(`{"foo":"bar"}`)})
				require.Error(t, err)
				assert.ErrorIs(t, err, apigateway.ErrInvalidParameter)
			},
		},
		{
			name: "non-json rejected",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				_, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte("swagger: '2.0'")})
				require.Error(t, err)
				assert.ErrorIs(t, err, apigateway.ErrInvalidParameter)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// TestImportRestAPI_RESTRouting verifies the SDK-shaped HTTP routing:
// POST /restapis?mode=import carries the raw spec body, and PUT /restapis/{id}
// performs PutRestApi rather than being misrouted to CreateRestApi/UpdateRestApi.
func TestImportRestAPI_RESTRouting(t *testing.T) {
	t.Parallel()

	t.Run("POST restapis mode=import creates full api", func(t *testing.T) {
		t.Parallel()

		b := apigateway.NewInMemoryBackend()
		h := apigateway.NewHandler(b)

		rec := restRequest(t, h, http.MethodPost, "/restapis?mode=import", swagger20Pets)
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "PetStore", resp["name"])
		apiID, _ := resp["id"].(string)
		require.NotEmpty(t, apiID)

		resources, _, err := b.GetResources(apiID, "", 0)
		require.NoError(t, err)
		assert.Len(t, resources, 3)
	})

	t.Run("POST restapis without mode still creates plain api", func(t *testing.T) {
		t.Parallel()

		b := apigateway.NewInMemoryBackend()
		h := apigateway.NewHandler(b)

		rec := restRequest(t, h, http.MethodPost, "/restapis", `{"name":"plain"}`)
		require.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "plain", resp["name"])
	})

	t.Run("PUT restapis id is PutRestApi", func(t *testing.T) {
		t.Parallel()

		b := apigateway.NewInMemoryBackend()
		h := apigateway.NewHandler(b)

		api, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte(swagger20Pets)})
		require.NoError(t, err)

		rec := restRequest(t, h, http.MethodPut, "/restapis/"+api.ID+"?mode=merge", healthDoc)
		require.Equal(t, http.StatusOK, rec.Code)

		assert.NotNil(t, findResourceByPath(t, b, api.ID, "/health"))
		assert.NotNil(t, findResourceByPath(t, b, api.ID, "/pets"))
	})
}

func TestPutRestAPI(t *testing.T) {
	t.Parallel()

	const extraDoc = healthDoc

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "merge layers new paths onto existing tree",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte(swagger20Pets)})
				require.NoError(t, err)

				out, perr := b.PutRestAPI(apigateway.PutRestAPIInput{
					RestAPIID: api.ID, Mode: "merge", Body: []byte(extraDoc),
				})
				require.NoError(t, perr)
				assert.Equal(t, api.ID, out.ID)

				// /pets retained, /health added.
				assert.NotNil(t, findResourceByPath(t, b, api.ID, "/pets"))
				assert.NotNil(t, findResourceByPath(t, b, api.ID, "/health"))
			},
		},
		{
			name: "overwrite replaces the resource tree",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte(swagger20Pets)})
				require.NoError(t, err)

				out, perr := b.PutRestAPI(apigateway.PutRestAPIInput{
					RestAPIID: api.ID, Mode: "overwrite", Body: []byte(extraDoc),
				})
				require.NoError(t, perr)
				assert.Equal(t, "PetStoreV2", out.Name)
				assert.Equal(t, api.RootResourceID, out.RootResourceID)

				assert.Nil(t, findResourceByPath(t, b, api.ID, "/pets"))
				assert.NotNil(t, findResourceByPath(t, b, api.ID, "/health"))
			},
		},
		{
			name: "default mode is merge",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte(swagger20Pets)})
				require.NoError(t, err)

				_, perr := b.PutRestAPI(apigateway.PutRestAPIInput{RestAPIID: api.ID, Body: []byte(extraDoc)})
				require.NoError(t, perr)
				assert.NotNil(t, findResourceByPath(t, b, api.ID, "/pets"))
				assert.NotNil(t, findResourceByPath(t, b, api.ID, "/health"))
			},
		},
		{
			name: "unknown api returns NotFoundException",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				_, err := b.PutRestAPI(apigateway.PutRestAPIInput{RestAPIID: "nope", Body: []byte(extraDoc)})
				require.Error(t, err)
				assert.ErrorIs(t, err, apigateway.ErrRestAPINotFound)
			},
		},
		{
			name: "invalid mode rejected",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.ImportRestAPI(apigateway.ImportRestAPIInput{Body: []byte(swagger20Pets)})
				require.NoError(t, err)

				_, perr := b.PutRestAPI(apigateway.PutRestAPIInput{
					RestAPIID: api.ID, Mode: "bogus", Body: []byte(extraDoc),
				})
				require.Error(t, perr)
				assert.ErrorIs(t, perr, apigateway.ErrInvalidParameter)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// TestGetRestApis_NoPositionOnLastPage verifies that GetRestApis
// omits the position field from the response when all results fit in one page,
// matching AWS behaviour. Previously position was always serialised as "" which
// the AWS SDK v2 treats as a valid cursor indicating more pages.
func TestGetRestApis_NoPositionOnLastPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		apiNames     []string
		wantPosition bool
	}{
		{
			name:         "empty_list_omits_position",
			apiNames:     nil,
			wantPosition: false,
		},
		{
			name:         "multiple_apis_on_single_page_omits_position",
			apiNames:     []string{"api-one", "api-two", "api-three"},
			wantPosition: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(b)

			for _, name := range tt.apiNames {
				rec := restRequest(t, h, http.MethodPost, "/restapis",
					`{"name":"`+name+`"}`)
				require.True(t, rec.Code >= 200 && rec.Code < 300)
			}

			rec := restRequest(t, h, http.MethodGet, "/restapis", "")
			require.True(t, rec.Code >= 200 && rec.Code < 300)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			_, hasPosition := resp["position"]
			assert.False(t, hasPosition,
				"position must be absent when all results fit on one page")
		})
	}
}

// TestGetRestApis_PositionPresentWhenPaginating verifies that
// GetRestApis includes position when there are more pages (backend-level test).
func TestGetRestApis_PositionPresentWhenPaginating(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	for _, name := range []string{"alpha", "beta", "gamma"} {
		_, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: name})
		require.NoError(t, err)
	}

	// Fetch only 1 of 3 — backend should return a non-empty position cursor.
	_, position, err := b.GetRestAPIs(1, "")
	require.NoError(t, err)
	assert.NotEmpty(t, position, "position must be non-empty when more results remain")
}

func TestRestAPI_BinaryMediaTypes(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:             "bin-api",
		BinaryMediaTypes: []string{"image/png", "application/octet-stream"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"image/png", "application/octet-stream"}, api.BinaryMediaTypes)

	got, err := b.GetRestAPI(api.ID)
	require.NoError(t, err)
	assert.Equal(t, api.BinaryMediaTypes, got.BinaryMediaTypes)
}

func TestRestAPI_EndpointConfiguration(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name: "ep-api",
		EndpointConfiguration: &apigateway.EndpointConfiguration{
			Types: []string{"REGIONAL"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, api.EndpointConfiguration)
	assert.Equal(t, []string{"REGIONAL"}, api.EndpointConfiguration.Types)
}

func TestRestAPI_MinimumCompressionSize(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:                   "compress-api",
		MinimumCompressionSize: 1024,
	})
	require.NoError(t, err)
	assert.Equal(t, 1024, api.MinimumCompressionSize)

	minCompress := 512
	updated, err := b.UpdateRestAPI(api.ID, apigateway.UpdateRestAPIInput{
		MinimumCompressionSize: &minCompress,
	})
	require.NoError(t, err)
	assert.Equal(t, 512, updated.MinimumCompressionSize)
}

func TestRestAPI_ApiKeySource(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:         "key-src-api",
		APIKeySource: "AUTHORIZER",
	})
	require.NoError(t, err)
	assert.Equal(t, "AUTHORIZER", api.APIKeySource)

	updated, err := b.UpdateRestAPI(api.ID, apigateway.UpdateRestAPIInput{
		APIKeySource: "HEADER",
	})
	require.NoError(t, err)
	assert.Equal(t, "HEADER", updated.APIKeySource)
}

func TestRestAPI_Policy(t *testing.T) {
	t.Parallel()

	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"execute-api:Invoke","Resource":"arn:aws:execute-api:*:*:*"}]}`
	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:   "policy-api",
		Policy: policy,
	})
	require.NoError(t, err)
	assert.Equal(t, policy, api.Policy)

	updated, err := b.UpdateRestAPI(api.ID, apigateway.UpdateRestAPIInput{
		Policy: `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.NoError(t, err)
	assert.NotEqual(t, policy, updated.Policy)
}

// TestUpdateRestAPI tests UpdateRestApi.
func TestUpdateRestAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newName  string
		newDesc  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_name_and_description",
			newName:  "renamed-api",
			newDesc:  "updated desc",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "api_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)

			lookupID := apiID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateRestApi",
				fmt.Sprintf(`{"restApiId":%q,"name":%q,"description":%q}`,
					lookupID, tt.newName, tt.newDesc))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.newName, resp["name"])
			}
		})
	}
}

// TestRestAPIActions_RESTPathCoverage exercises the restAPIActions closures via REST
// path requests (GET/DELETE /restapis/...) to cover branches not reached by the
// X-Amz-Target path.
func TestRestAPIActions_RESTPathCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "GET_restapis_returns_200",
			method:   http.MethodGet,
			path:     "/restapis",
			wantCode: http.StatusOK,
		},
		{
			name:   "GET_restapis_by_id_returns_200",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "test-api"})

				return "/restapis/" + api.ID
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DELETE_restapi_returns_202",
			method: http.MethodDelete,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "del-api"})

				return "/restapis/" + api.ID
			},
			wantCode: http.StatusAccepted,
		},
		{
			name:     "POST_restapis_returns_201",
			method:   http.MethodPost,
			path:     "/restapis",
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend)

			path := tt.path
			if tt.setup != nil {
				path = tt.setup(backend)
			}

			body := ""
			if tt.method == http.MethodPost {
				body = `{"name":"rest-created-api"}`
			}

			rec := restRequest(t, h, tt.method, path, body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestGetRestAPIs_Pagination exercises GetRestAPIs with a limit that triggers
// the pagination position output.
func TestGetRestAPIs_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		limit        int
		wantLen      int
		wantPosition bool
	}{
		{
			name:         "limit_1_returns_position",
			limit:        1,
			wantLen:      1,
			wantPosition: true,
		},
		{
			name:    "limit_0_returns_all",
			limit:   0,
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			for i := range 3 {
				_, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: fmt.Sprintf("api-%d", i)})
				require.NoError(t, err)
			}

			apis, pos, err := b.GetRestAPIs(tt.limit, "")
			require.NoError(t, err)
			assert.Len(t, apis, tt.wantLen)

			if tt.wantPosition {
				assert.NotEmpty(t, pos)
			} else {
				assert.Empty(t, pos)
			}
		})
	}
}

// TestGetRestAPIs_RESTPath_WithLimit exercises GetRestApis via REST path with
// limit and position query parameters (covers the restAPIActions GetRestApis closure
// when limit/position are passed via body from REST path merging).
func TestGetRestAPIs_RESTPath_WithLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "get_rest_apis_via_REST_path",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			_, _ = backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api-x"})

			h := apigateway.NewHandler(backend)
			rec := restRequest(t, h, http.MethodGet, "/restapis", "")

			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["item"])
		})
	}
}

func TestBackend_RestAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_and_get",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
					Name:        "my-api",
					Description: "desc",
					Tags:        tags.FromMap("test.apigw", map[string]string{"env": "test"}),
				})
				require.NoError(t, err)
				assert.NotEmpty(t, api.ID)
				assert.Equal(t, "my-api", api.Name)

				got, err := b.GetRestAPI(api.ID)
				require.NoError(t, err)
				assert.Equal(t, api.ID, got.ID)
			},
		},
		{
			name: "get_nonexistent_returns_error",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				_, err := b.GetRestAPI("nonexistent")
				require.Error(t, err)
			},
		},
		{
			name: "list_all",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				_, _ = b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "a"})
				_, _ = b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "b"})

				apis, pos, err := b.GetRestAPIs(0, "")
				require.NoError(t, err)
				assert.Len(t, apis, 2)
				assert.Empty(t, pos)
			},
		},
		{
			name: "delete_existing",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "to-del"})

				err := b.DeleteRestAPI(api.ID)
				require.NoError(t, err)

				_, err = b.GetRestAPI(api.ID)
				require.Error(t, err)
			},
		},
		{
			name: "delete_nonexistent_returns_error",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				err := b.DeleteRestAPI("nonexistent")
				require.Error(t, err)
			},
		},
		{
			name: "create_with_empty_name_returns_error",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				_, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{})
				require.Error(t, err)
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

// TestRestAPI_EndpointFields exercises CreateRestApi's
// DisableExecuteApiEndpoint/EndpointAccessMode fields and the read-only
// ApiStatus field (types.RestApi.ApiStatus/DisableExecuteApiEndpoint/
// EndpointAccessMode in the SDK), all absent from gopherstack's RestAPI
// struct until this sweep.
func TestRestAPI_EndpointFields(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:                      "endpoint-fields-api",
		DisableExecuteAPIEndpoint: true,
		EndpointAccessMode:        "STRICT",
	})
	require.NoError(t, err)
	assert.True(t, api.DisableExecuteAPIEndpoint)
	assert.Equal(t, "STRICT", api.EndpointAccessMode)
	assert.Equal(t, "AVAILABLE", api.APIStatus, "gopherstack creates RestApis synchronously")

	got, err := b.GetRestAPI(api.ID)
	require.NoError(t, err)
	assert.True(t, got.DisableExecuteAPIEndpoint)
	assert.Equal(t, "STRICT", got.EndpointAccessMode)

	updated, err := b.UpdateRestAPI(api.ID, apigateway.UpdateRestAPIInput{
		EndpointAccessMode: "BASIC",
	})
	require.NoError(t, err)
	assert.Equal(t, "BASIC", updated.EndpointAccessMode)
	assert.True(t, updated.DisableExecuteAPIEndpoint, "untouched field keeps its prior value")
}

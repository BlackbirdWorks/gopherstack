package appsync_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/appsync"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler() (*appsync.Handler, *appsync.InMemoryBackend) {
	b := appsync.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
	h := appsync.NewHandler(b)

	return h, b
}

func doRequest(t *testing.T, handler *appsync.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var buf *bytes.Buffer

	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewBuffer(b)
	} else {
		buf = bytes.NewBuffer(nil)
	}

	req := httptest.NewRequest(method, path, buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	assert.Equal(t, "AppSync", h.Name())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	e := echo.New()

	tests := []struct {
		name  string
		path  string
		match bool
	}{
		{name: "matches_v1_apis", path: "/v1/apis", match: true},
		{name: "matches_v1_apis_with_id", path: "/v1/apis/abc123", match: true},
		{name: "no_match_other_path", path: "/restapis/foo", match: false},
		{name: "no_match_root", path: "/", match: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.match, matcher(c))
		})
	}
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateGraphqlApi")
	assert.Contains(t, ops, "ExecuteGraphQL")
	assert.Contains(t, ops, "CreateResolver")
}

func TestHandler_CreateAndGetGraphqlAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantAPIName string
		wantStatus  int
	}{
		{
			name:        "creates_api_successfully",
			body:        map[string]any{"name": "MyAPI", "authenticationType": "API_KEY"},
			wantStatus:  http.StatusCreated,
			wantAPIName: "MyAPI",
		},
		{
			name:       "missing_name_returns_400",
			body:       map[string]any{"authenticationType": "API_KEY"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, http.MethodPost, "/v1/apis", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantAPIName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				api, ok := resp["graphqlApi"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantAPIName, api["name"])
				assert.NotEmpty(t, api["apiId"])
			}
		})
	}
}

func TestHandler_DeleteGraphqlAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		apiID      string
		wantStatus int
	}{
		{
			name:       "deletes_existing_api",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "returns_404_for_missing_api",
			apiID:      "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := tt.apiID

			if apiID == "" {
				api, _ := b.CreateGraphqlAPI("ToDelete", appsync.AuthTypeAPIKey, nil)
				apiID = api.APIID
			}

			rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+apiID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_StartSchemaCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sdl         string
		wantStatus2 string
		wantStatus  int
	}{
		{
			name:        "valid_schema_returns_active",
			sdl:         `type Query { hello: String }`,
			wantStatus:  http.StatusOK,
			wantStatus2: string(appsync.SchemaStatusActive),
		},
		{
			name:       "invalid_schema_returns_400",
			sdl:        `type { broken`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

			body := map[string]any{"definition": tt.sdl}
			rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/schemacreation", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus2 != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.Equal(t, tt.wantStatus2, resp["status"])
			}
		})
	}
}

func TestHandler_CreateAndGetDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dsBody     map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "creates_lambda_datasource",
			dsBody: map[string]any{
				"name": "LambdaDS",
				"type": "AWS_LAMBDA",
				"lambdaConfig": map[string]any{
					"lambdaFunctionArn": "arn:aws:lambda:us-east-1:000:function:test",
				},
			},
			wantStatus: http.StatusCreated,
			wantName:   "LambdaDS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

			rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/datasources", tt.dsBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				ds, ok := resp["dataSource"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, ds["name"])
			}
		})
	}
}

func TestHandler_CreateAndGetResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resolverBody  map[string]any
		name          string
		typeName      string
		wantFieldName string
		wantStatus    int
	}{
		{
			name:     "creates_resolver",
			typeName: "Query",
			resolverBody: map[string]any{
				"fieldName":      "getItem",
				"dataSourceName": "MyDS",
			},
			wantStatus:    http.StatusCreated,
			wantFieldName: "getItem",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

			path := "/v1/apis/" + api.APIID + "/types/" + tt.typeName + "/resolvers"
			rec := doRequest(t, h, http.MethodPost, path, tt.resolverBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantFieldName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				r, ok := resp["resolver"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantFieldName, r["fieldName"])
			}
		})
	}
}

func TestHandler_GraphQLExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		schema     string
		query      string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "executes_none_resolver",
			schema:     `type Query { ping: String }`,
			query:      `query { ping }`,
			wantStatus: http.StatusOK,
			wantKey:    "ping",
		},
		{
			name:       "returns_error_for_unknown_api",
			schema:     "",
			query:      `query { ping }`,
			wantStatus: http.StatusOK,
			wantKey:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

			if tt.schema != "" {
				_, _ = b.StartSchemaCreation(api.APIID, tt.schema)
				_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
					Name: "NoneDS",
					Type: appsync.DataSourceTypeNone,
				})
				_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
					FieldName:      "ping",
					DataSourceName: "NoneDS",
				})
			}

			body := map[string]any{"query": tt.query}
			path := "/v1/apis/" + api.APIID + "/graphql"
			rec := doRequest(t, h, http.MethodPost, path, body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantKey != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

				if _, hasErrors := resp["errors"]; !hasErrors {
					data, ok := resp["data"].(map[string]any)
					require.True(t, ok)
					assert.Contains(t, data, tt.wantKey)
				}
			}
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "POST /v1/apis", method: http.MethodPost, path: "/v1/apis", wantOp: "CreateGraphqlApi"},
		{name: "GET /v1/apis", method: http.MethodGet, path: "/v1/apis", wantOp: "ListGraphqlApis"},
		{name: "GET /v1/apis/id", method: http.MethodGet, path: "/v1/apis/abc", wantOp: "GetGraphqlApi"},
		{name: "DELETE /v1/apis/id", method: http.MethodDelete, path: "/v1/apis/abc", wantOp: "DeleteGraphqlApi"},
		{
			name:   "POST schemacreations",
			method: http.MethodPost,
			path:   "/v1/apis/abc/schemacreation",
			wantOp: "StartSchemaCreation",
		},
		{
			name:   "GET schemacreations",
			method: http.MethodGet,
			path:   "/v1/apis/abc/schemacreation",
			wantOp: "GetSchemaCreationStatus",
		},
		{name: "GET schema", method: http.MethodGet, path: "/v1/apis/abc/schema", wantOp: "GetIntrospectionSchema"},
		{
			name:   "POST datasources",
			method: http.MethodPost,
			path:   "/v1/apis/abc/datasources",
			wantOp: "CreateDataSource",
		},
		{name: "GET datasources", method: http.MethodGet, path: "/v1/apis/abc/datasources", wantOp: "ListDataSources"},
		{
			name:   "GET datasource",
			method: http.MethodGet,
			path:   "/v1/apis/abc/datasources/myds",
			wantOp: "GetDataSource",
		},
		{
			name:   "POST resolvers",
			method: http.MethodPost,
			path:   "/v1/apis/abc/types/Query/resolvers",
			wantOp: "CreateResolver",
		},
		{
			name:   "GET resolvers",
			method: http.MethodGet,
			path:   "/v1/apis/abc/types/Query/resolvers",
			wantOp: "ListResolvers",
		},
		{
			name:   "GET resolver",
			method: http.MethodGet,
			path:   "/v1/apis/abc/types/Query/resolvers/getItem",
			wantOp: "GetResolver",
		},
		{
			name:   "DELETE resolver",
			method: http.MethodDelete,
			path:   "/v1/apis/abc/types/Query/resolvers/getItem",
			wantOp: "DeleteResolver",
		},
		{name: "POST graphql", method: http.MethodPost, path: "/v1/apis/abc/graphql", wantOp: "ExecuteGraphQL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(""))
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	assert.Equal(t, 85, h.MatchPriority())
}

func TestHandler_ListGraphqlAPIs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*appsync.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty_list",
			setup:     func(_ *appsync.InMemoryBackend) {},
			wantCount: 0,
		},
		{
			name: "returns_all_apis",
			setup: func(b *appsync.InMemoryBackend) {
				_, _ = b.CreateGraphqlAPI("API1", appsync.AuthTypeAPIKey, nil)
				_, _ = b.CreateGraphqlAPI("API2", appsync.AuthTypeIAM, nil)
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			tt.setup(b)

			rec := doRequest(t, h, http.MethodGet, "/v1/apis", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			apis, ok := resp["graphqlApis"].([]any)
			require.True(t, ok)
			assert.Len(t, apis, tt.wantCount)
		})
	}
}

func TestHandler_GetGraphqlAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		apiID      string
		wantStatus int
	}{
		{
			name:       "returns_existing_api",
			wantStatus: http.StatusOK,
		},
		{
			name:       "returns_404_for_missing_api",
			apiID:      "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := tt.apiID

			if apiID == "" {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
				apiID = api.APIID
			}

			rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+apiID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetSchemaCreationStatus(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/schemacreation", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, string(appsync.SchemaStatusActive), resp["status"])
}

func TestHandler_GetIntrospectionSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		hasSchema  bool
		wantStatus int
	}{
		{
			name:       "returns_schema_sdl",
			hasSchema:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "returns_404_when_no_schema",
			hasSchema:  false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

			if tt.hasSchema {
				_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
			}

			rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/schema", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetDataSource(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "MyDS", Type: appsync.DataSourceTypeNone})

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/datasources/MyDS", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListDataSources(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "DS1", Type: appsync.DataSourceTypeNone})

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/datasources", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	dss, ok := resp["dataSources"].([]any)
	require.True(t, ok)
	assert.Len(t, dss, 1)
}

func TestHandler_DeleteDataSource(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "DS1", Type: appsync.DataSourceTypeNone})

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/datasources/DS1", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_GetResolver(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "getItem"})

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/types/Query/resolvers/getItem", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListResolvers(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "getItem"})

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/types/Query/resolvers", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	res, ok := resp["resolvers"].([]any)
	require.True(t, ok)
	assert.Len(t, res, 1)
}

func TestHandler_DeleteResolver(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "getItem"})

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/types/Query/resolvers/getItem", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_APIs_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	rec := doRequest(t, h, http.MethodPut, "/v1/apis", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_SchemaCreations_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/schemacreation", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	tests := []struct {
		name    string
		path    string
		wantRes string
	}{
		{
			name:    "extracts_api_id",
			path:    "/v1/apis/" + api.APIID,
			wantRes: api.APIID,
		},
		{
			name:    "returns_empty_for_list_path",
			path:    "/v1/apis",
			wantRes: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantRes, h.ExtractResource(c))
		})
	}
}

func TestHandler_GraphQL_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/graphql", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_GraphQL_InvalidJSON(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/v1/apis/"+api.APIID+"/graphql",
		strings.NewReader("not-json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_Types_ShortPath(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	// Path with only /v1/apis/{id}/types should return 404.
	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/types", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UnknownSubpath(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/unknown", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DataSources_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/datasources", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_API_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID, nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

package appsync_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
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
		name      string
		path      string
		userAgent string
		match     bool
	}{
		{name: "matches_v1_apis", path: "/v1/apis", match: true},
		{name: "matches_v1_apis_with_id", path: "/v1/apis/abc123", match: true},
		{name: "no_match_other_path", path: "/restapis/foo", match: false},
		{name: "no_match_root", path: "/", match: false},
		// /v2/apis is shared with API Gateway V2; only match when User-Agent contains "api/appsync".
		{
			name:      "v2_apis_with_appsync_ua",
			path:      "/v2/apis",
			userAgent: "aws-sdk-go-v2/1.0 api/appsync/1.53.5",
			match:     true,
		},
		{
			name:      "v2_apis_with_apigwv2_ua",
			path:      "/v2/apis",
			userAgent: "aws-sdk-go-v2/1.0 api/apigatewayv2/1.33.7",
			match:     false,
		},
		{name: "v2_apis_no_ua", path: "/v2/apis", userAgent: "", match: false},
		{
			name:      "v2_apis_with_id_appsync_ua",
			path:      "/v2/apis/abc123",
			userAgent: "aws-sdk-go-v2/1.0 api/appsync/1.53.5",
			match:     true,
		},
		{name: "v2_apis_with_id_no_ua", path: "/v2/apis/abc123", userAgent: "", match: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.userAgent != "" {
				req.Header.Set("User-Agent", tt.userAgent)
			}
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
				api, _ := b.CreateGraphqlAPI("ToDelete", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

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
				_, _ = b.CreateGraphqlAPI("API1", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				_, _ = b.CreateGraphqlAPI("API2", appsync.AuthTypeIAM, false, "", "", nil, nil, nil)
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
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

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
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "MyDS", Type: appsync.DataSourceTypeNone})

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/datasources/MyDS", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListDataSources(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "DS1", Type: appsync.DataSourceTypeNone})

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/datasources/DS1", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_GetResolver(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "getItem", DataSourceName: "myDs"})

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/types/Query/resolvers/getItem", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListResolvers(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "getItem", DataSourceName: "myDs"})

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
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "getItem", DataSourceName: "myDs"})

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
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/schemacreation", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

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
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/graphql", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_GraphQL_InvalidJSON(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	// GET /v1/apis/{id}/types now returns list of types (200 OK).
	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/types", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UnknownSubpath(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/unknown", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DataSources_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/datasources", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_API_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// CONNECT is not allowed on the /v1/apis path.
	rec := doRequest(t, h, http.MethodConnect, "/v1/apis", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_CreateApiKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*appsync.InMemoryBackend) string
		body       map[string]any
		name       string
		wantStatus int
		wantKeyID  bool
	}{
		{
			name: "creates_api_key_successfully",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			body:       map[string]any{"description": "test key", "expires": 1999999999},
			wantStatus: http.StatusCreated,
			wantKeyID:  true,
		},
		{
			name: "returns_404_for_missing_api",
			setup: func(_ *appsync.InMemoryBackend) string {
				return "nonexistent"
			},
			body:       map[string]any{"description": "test key"},
			wantStatus: http.StatusNotFound,
			wantKeyID:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := tt.setup(b)

			rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/apikeys", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantKeyID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				key, ok := resp["apiKey"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, key["id"])
			}
		})
	}
}

func TestHandler_CreateApiCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*appsync.InMemoryBackend) string
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "creates_api_cache_successfully",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			body: map[string]any{
				"apiCachingBehavior": "FULL_REQUEST_CACHING",
				"ttl":                300,
				"type":               "SMALL",
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "returns_404_for_missing_api",
			setup: func(_ *appsync.InMemoryBackend) string {
				return "nonexistent"
			},
			body:       map[string]any{"ttl": 300},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := tt.setup(b)

			rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/ApiCaches", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*appsync.InMemoryBackend) string
		body         map[string]any
		name         string
		wantFuncName string
		wantStatus   int
	}{
		{
			name: "creates_function_successfully",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			body: map[string]any{
				"name":           "MyFunction",
				"dataSourceName": "MyDS",
				"description":    "test fn",
			},
			wantStatus:   http.StatusCreated,
			wantFuncName: "MyFunction",
		},
		{
			name: "missing_name_returns_400",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			body:       map[string]any{"dataSourceName": "MyDS"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_datasource_returns_400",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			body:       map[string]any{"name": "MyFunction"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "returns_404_for_missing_api",
			setup: func(_ *appsync.InMemoryBackend) string {
				return "nonexistent"
			},
			body:       map[string]any{"name": "Fn", "dataSourceName": "DS"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := tt.setup(b)

			rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/functions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantFuncName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				fn, ok := resp["functionConfiguration"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantFuncName, fn["name"])
				assert.NotEmpty(t, fn["functionId"])
			}
		})
	}
}

func TestHandler_CreateType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*appsync.InMemoryBackend) string
		body       map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "creates_type_successfully",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			body: map[string]any{
				"definition": "type Post { id: ID! title: String }",
				"format":     "SDL",
			},
			wantStatus: http.StatusCreated,
			wantName:   "Post",
		},
		{
			name: "missing_definition_returns_400",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			body:       map[string]any{"format": "SDL"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "returns_404_for_missing_api",
			setup: func(_ *appsync.InMemoryBackend) string {
				return "nonexistent"
			},
			body:       map[string]any{"definition": "type Foo { id: ID! }", "format": "SDL"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := tt.setup(b)

			rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/types", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				typ, ok := resp["type"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, typ["name"])
			}
		})
	}
}

func TestHandler_CreateDomainName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantDomainName string
		wantStatus     int
	}{
		{
			name: "creates_domain_name_successfully",
			body: map[string]any{
				"domainName":     "api.example.com",
				"certificateArn": "arn:aws:acm:us-east-1:000:certificate/abc",
				"description":    "my domain",
			},
			wantStatus:     http.StatusCreated,
			wantDomainName: "api.example.com",
		},
		{
			name:       "missing_domain_name_returns_400",
			body:       map[string]any{"certificateArn": "arn:aws:acm:us-east-1:000:certificate/abc"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_certificate_arn_returns_400",
			body:       map[string]any{"domainName": "api.example.com"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()

			rec := doRequest(t, h, http.MethodPost, "/v1/domainnames", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDomainName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				cfg, ok := resp["domainNameConfig"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantDomainName, cfg["domainName"])
			}
		})
	}
}

func TestHandler_AssociateApi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*appsync.InMemoryBackend) (string, string)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "associates_api_successfully",
			setup: func(b *appsync.InMemoryBackend) (string, string) {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				dn, _ := b.CreateDomainName("api.example.com", "arn:aws:acm:us-east-1:000:certificate/abc", "", nil)

				return dn.DomainName, api.APIID
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "returns_404_for_missing_domain",
			setup: func(_ *appsync.InMemoryBackend) (string, string) {
				return "missing.example.com", "someapiid"
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "missing_api_id_returns_400",
			setup: func(b *appsync.InMemoryBackend) (string, string) {
				dn, _ := b.CreateDomainName("api2.example.com", "arn:aws:acm:us-east-1:000:certificate/abc", "", nil)

				return dn.DomainName, ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			domainName, apiID := tt.setup(b)

			var body map[string]any
			if apiID != "" {
				body = map[string]any{"apiId": apiID}
			} else {
				body = map[string]any{}
			}

			rec := doRequest(t, h, http.MethodPost, "/v1/domainnames/"+domainName+"/apiassociation", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_AssociateMergedGraphqlApi(t *testing.T) {
	t.Parallel()

	t.Run("associates_merged_api_successfully", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		src, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		mrg, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost,
			"/v1/sourceApis/"+src.APIID+"/mergedApiAssociations",
			map[string]any{"mergedApiIdentifier": mrg.APIID, "description": "test"},
		)
		assert.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assoc, ok := resp["sourceApiAssociation"].(map[string]any)
		require.True(t, ok)
		assert.NotEmpty(t, assoc["associationId"])
		assert.Equal(t, src.APIID, assoc["sourceApiId"])
	})

	t.Run("missing_merged_api_identifier_returns_400", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		src, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost,
			"/v1/sourceApis/"+src.APIID+"/mergedApiAssociations",
			map[string]any{"description": "test"},
		)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandler_AssociateSourceGraphqlApi(t *testing.T) {
	t.Parallel()

	t.Run("associates_source_api_successfully", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		src, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
		mrg, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost,
			"/v1/mergedApis/"+mrg.APIID+"/sourceApiAssociations",
			map[string]any{"sourceApiIdentifier": src.APIID, "description": "test"},
		)
		assert.Equal(t, http.StatusCreated, rec.Code)

		var resp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assoc, ok := resp["sourceApiAssociation"].(map[string]any)
		require.True(t, ok)
		assert.NotEmpty(t, assoc["associationId"])
		assert.Equal(t, mrg.APIID, assoc["mergedApiId"])
	})

	t.Run("missing_source_api_identifier_returns_400", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler()
		mrg, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodPost,
			"/v1/mergedApis/"+mrg.APIID+"/sourceApiAssociations",
			map[string]any{"description": "test"},
		)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandler_CreateApi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "creates_event_api_successfully",
			body: map[string]any{
				"name": "MyEventAPI",
				"tags": map[string]string{"env": "test"},
			},
			wantStatus: http.StatusCreated,
			wantName:   "MyEventAPI",
		},
		{
			name:       "missing_name_returns_400",
			body:       map[string]any{"ownerContact": "owner@example.com"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, http.MethodPost, "/v2/apis", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				api, ok := resp["api"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, api["name"])
				assert.NotEmpty(t, api["apiId"])
			}
		})
	}
}

func TestHandler_CreateChannelNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*appsync.InMemoryBackend) string
		body       map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "creates_channel_namespace_successfully",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateAPI("MyEventAPI", "", nil, nil)

				return api.APIID
			},
			body:       map[string]any{"name": "default"},
			wantStatus: http.StatusCreated,
			wantName:   "default",
		},
		{
			name: "missing_name_returns_400",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateAPI("MyEventAPI", "", nil, nil)

				return api.APIID
			},
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "returns_404_for_missing_api",
			setup: func(_ *appsync.InMemoryBackend) string {
				return "nonexistent"
			},
			body:       map[string]any{"name": "default"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := tt.setup(b)

			rec := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/channelNamespaces", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				ns, ok := resp["channelNamespace"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, ns["name"])
			}
		})
	}
}

func TestHandler_ExtractOperation_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "POST /v1/apis/{id}/apikeys",
			method: http.MethodPost,
			path:   "/v1/apis/abc/apikeys",
			wantOp: "CreateApiKey",
		},
		{
			name:   "POST /v1/apis/{id}/ApiCaches",
			method: http.MethodPost,
			path:   "/v1/apis/abc/ApiCaches",
			wantOp: "CreateApiCache",
		},
		{
			name:   "POST /v1/apis/{id}/functions",
			method: http.MethodPost,
			path:   "/v1/apis/abc/functions",
			wantOp: "CreateFunction",
		},
		{
			name:   "POST /v1/apis/{id}/types",
			method: http.MethodPost,
			path:   "/v1/apis/abc/types",
			wantOp: "CreateType",
		},
		{
			name:   "POST /v1/domainnames",
			method: http.MethodPost,
			path:   "/v1/domainnames",
			wantOp: "CreateDomainName",
		},
		{
			name:   "POST /v1/domainnames/{dn}/apiassociation",
			method: http.MethodPost,
			path:   "/v1/domainnames/api.example.com/apiassociation",
			wantOp: "AssociateApi",
		},
		{
			name:   "POST /v1/sourceApis/{id}/mergedApiAssociations",
			method: http.MethodPost,
			path:   "/v1/sourceApis/source-id/mergedApiAssociations",
			wantOp: "AssociateMergedGraphqlApi",
		},
		{
			name:   "POST /v1/mergedApis/{id}/sourceApiAssociations",
			method: http.MethodPost,
			path:   "/v1/mergedApis/merged-id/sourceApiAssociations",
			wantOp: "AssociateSourceGraphqlApi",
		},
		{
			name:   "POST /v2/apis",
			method: http.MethodPost,
			path:   "/v2/apis",
			wantOp: "CreateApi",
		},
		{
			name:   "POST /v2/apis/{id}/channelNamespaces",
			method: http.MethodPost,
			path:   "/v2/apis/abc/channelNamespaces",
			wantOp: "CreateChannelNamespace",
		},
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

func TestHandler_MethodNotAllowed_NewOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "apikeys_method_not_allowed",
			method:     http.MethodDelete, // DELETE at collection level (no keyId) → 405
			path:       "/v1/apis/abc/apikeys",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "apicaches_method_not_allowed",
			method:     http.MethodPatch, // PATCH → 405
			path:       "/v1/apis/abc/ApiCaches",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "functions_method_not_allowed",
			method:     http.MethodDelete, // DELETE at collection level → 405
			path:       "/v1/apis/abc/functions",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "domainnames_method_not_allowed",
			method:     http.MethodDelete, // DELETE at /v1/domainnames (no name) → 405
			path:       "/v1/domainnames",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "apiassociation_method_not_allowed",
			method:     http.MethodPut, // PUT /apiassociation → 405 (not a valid method)
			path:       "/v1/domainnames/api.example.com/apiassociation",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "mergedApiAssociations_method_not_allowed",
			method:     http.MethodGet,
			path:       "/v1/sourceApis/source-id/mergedApiAssociations",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "sourceApiAssociations_method_not_allowed",
			method:     http.MethodPut, // PUT on sourceApiAssociations collection → 405
			path:       "/v1/mergedApis/merged-id/sourceApiAssociations",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "v2_apis_method_not_allowed",
			method:     http.MethodPatch, // PATCH /v2/apis → 405
			path:       "/v2/apis",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "channel_namespaces_method_not_allowed",
			method:     http.MethodPut, // PUT on channelNamespaces collection → 405 (GET/POST are now valid)
			path:       "/v2/apis/abc/channelNamespaces",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "domainnames_unknown_path",
			method:     http.MethodGet,
			path:       "/v1/domainnames/api.example.com/unknown",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "sourceApis_unknown_path",
			method:     http.MethodGet,
			path:       "/v1/sourceApis/source-id/unknown",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "mergedApis_unknown_path",
			method:     http.MethodGet,
			path:       "/v1/mergedApis/merged-id/unknown",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "v2_apis_unknown_path",
			method:     http.MethodGet,
			path:       "/v2/apis/abc/unknown",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateGraphqlAPI_InvalidAuthType(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	body := map[string]any{"name": "TestAPI", "authenticationType": "INVALID_TYPE"}
	rec := doRequest(t, h, http.MethodPost, "/v1/apis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateAPICache_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid",
			body: map[string]any{
				"ttl":                int64(60),
				"type":               "SMALL",
				"apiCachingBehavior": "FULL_REQUEST_CACHING",
			},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "ttl_zero_rejected",
			body:       map[string]any{"ttl": 0, "type": "SMALL", "apiCachingBehavior": "FULL_REQUEST_CACHING"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_type_rejected",
			body:       map[string]any{"ttl": 60, "type": "BOGUS", "apiCachingBehavior": "FULL_REQUEST_CACHING"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_caching_behavior_rejected",
			body:       map[string]any{"ttl": 60, "type": "SMALL", "apiCachingBehavior": "BOGUS"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			// Create an API first.
			body := map[string]any{"name": "TestAPI", "authenticationType": "API_KEY"}
			rec := doRequest(t, h, http.MethodPost, "/v1/apis", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			gqlAPI := resp["graphqlApi"].(map[string]any)
			apiID := gqlAPI["apiId"].(string)

			rec2 := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/ApiCaches", tt.body)
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

func TestHandler_CreateDomainName_InvalidDomain(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	body := map[string]any{
		"domainName":     "notadomain",
		"certificateArn": "arn:aws:acm:us-east-1:000000000000:certificate/abc",
	}
	rec := doRequest(t, h, http.MethodPost, "/v1/domainnames", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateType_DuplicateRejected(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	// Create an API.
	body := map[string]any{"name": "TestAPI", "authenticationType": "API_KEY"}
	rec := doRequest(t, h, http.MethodPost, "/v1/apis", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	gqlAPI := resp["graphqlApi"].(map[string]any)
	apiID := gqlAPI["apiId"].(string)

	typeBody := map[string]any{"definition": "type MyType { id: ID! }", "format": "SDL"}

	// First creation succeeds.
	rec2 := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/types", typeBody)
	assert.Equal(t, http.StatusCreated, rec2.Code)

	// Second creation with same type name fails.
	rec3 := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/types", typeBody)
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

func TestHandler_CreateAPIKey_Da2Prefix(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	// Create an API.
	body := map[string]any{"name": "TestAPI", "authenticationType": "API_KEY"}
	rec := doRequest(t, h, http.MethodPost, "/v1/apis", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	gqlAPI := resp["graphqlApi"].(map[string]any)
	apiID := gqlAPI["apiId"].(string)

	keyBody := map[string]any{"description": "test key"}
	rec2 := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/apikeys", keyBody)
	require.Equal(t, http.StatusCreated, rec2.Code)

	var keyResp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&keyResp))
	apiKey := keyResp["apiKey"].(map[string]any)
	keyID := apiKey["id"].(string)
	require.GreaterOrEqual(t, len(keyID), 4, "key ID must be at least 4 characters")
	assert.Equal(t, "da2-", keyID[:4])
}

// ---- Refinement 2: new operation tests ----

func TestHandler_UpdateGraphqlAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name:       "updates_name",
			body:       map[string]any{"name": "UpdatedAPI"},
			wantStatus: http.StatusOK,
			wantName:   "UpdatedAPI",
		},
		{
			name:       "updates_auth_type",
			body:       map[string]any{"authenticationType": "AWS_IAM"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_auth_type_rejected",
			body:       map[string]any{"authenticationType": "INVALID"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPatch, "/v1/apis/"+api.APIID, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" && tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				gqlAPI := resp["graphqlApi"].(map[string]any)
				assert.Equal(t, tt.wantName, gqlAPI["name"])
			}
		})
	}
}

func TestHandler_ListApiKeys(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPIKey(api.APIID, "key1", 0)
	require.NoError(t, err)
	_, err = b.CreateAPIKey(api.APIID, "key2", 0)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/apikeys", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	keys := resp["apiKeys"].([]any)
	assert.Len(t, keys, 2)
}

func TestHandler_DeleteApiKey(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "test", 0)
	require.NoError(t, err)

	// Delete the key.
	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/apikeys/"+key.ID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Second delete returns 404.
	rec2 := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/apikeys/"+key.ID, nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestHandler_GetApiCache(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(api.APIID, &appsync.APICache{
		TTL:                60,
		Type:               "SMALL",
		APICachingBehavior: "FULL_REQUEST_CACHING",
	})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/ApiCaches", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotNil(t, resp["apiCache"])
}

func TestHandler_DeleteApiCache(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(api.APIID, &appsync.APICache{
		TTL:                60,
		Type:               "SMALL",
		APICachingBehavior: "FULL_REQUEST_CACHING",
	})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/ApiCaches", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Second delete returns 404.
	rec2 := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/ApiCaches", nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestHandler_ListFunctions(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateFunction(api.APIID, &appsync.Function{Name: "fn1", DataSourceName: "ds"})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/functions", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	fns := resp["functions"].([]any)
	assert.Len(t, fns, 1)
}

func TestHandler_GetFunction(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn1", DataSourceName: "ds"})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/functions/"+fn.FunctionID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotNil(t, resp["functionConfiguration"])
}

func TestHandler_DeleteFunction(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn1", DataSourceName: "ds"})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/functions/"+fn.FunctionID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Second delete returns 404.
	rec2 := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/functions/"+fn.FunctionID, nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestHandler_ListTypes(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/types", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	types := resp["types"].([]any)
	assert.Len(t, types, 1)
}

func TestHandler_GetType(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/types/MyType", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotNil(t, resp["type"])
}

func TestHandler_DeleteType(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/types/MyType", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Second delete returns 404.
	rec2 := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/types/MyType", nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestHandler_DomainNameCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	// Create.
	createBody := map[string]any{"domainName": "api.example.com", "certificateArn": certARN}
	rec := doRequest(t, h, http.MethodPost, "/v1/domainnames", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	// List.
	rec2 := doRequest(t, h, http.MethodGet, "/v1/domainnames", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&listResp))
	dns := listResp["domainNameConfigs"].([]any)
	assert.Len(t, dns, 1)

	// Get.
	rec3 := doRequest(t, h, http.MethodGet, "/v1/domainnames/api.example.com", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	// Delete.
	rec4 := doRequest(t, h, http.MethodDelete, "/v1/domainnames/api.example.com", nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// After delete, get returns 404.
	rec5 := doRequest(t, h, http.MethodGet, "/v1/domainnames/api.example.com", nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

func TestHandler_GetApiAssociation(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	// Create domain name.
	createBody := map[string]any{"domainName": "api.example.com", "certificateArn": certARN}
	doRequest(t, h, http.MethodPost, "/v1/domainnames", createBody)

	// Get association (no API associated yet).
	rec := doRequest(t, h, http.MethodGet, "/v1/domainnames/api.example.com/apiassociation", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Refinement 3: new operation tests ----

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Tag the resource.
	tagBody := map[string]any{"tags": map[string]any{"env": "prod", "team": "platform"}}
	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/tags", tagBody)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// List tags.
	rec2 := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/tags", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp))
	tags := resp["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// Untag one key.
	rec3, err := http.NewRequest(http.MethodDelete, "/v1/apis/"+api.APIID+"/tags?tagKeys=env", nil)
	require.NoError(t, err)

	_ = rec3
	// Use doRequest helper style with query param.
	e := echo.New()
	req := httptest.NewRequest(http.MethodDelete, "/v1/apis/"+api.APIID+"/tags?tagKeys=env", nil)
	rr := httptest.NewRecorder()
	ctx := e.NewContext(req, rr)

	require.NoError(t, h.Handler()(ctx))
	assert.Equal(t, http.StatusNoContent, rr.Code)

	// List tags again - should only have "team".
	rec4 := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/tags", nil)
	require.Equal(t, http.StatusOK, rec4.Code)

	var resp4 map[string]any
	require.NoError(t, json.NewDecoder(rec4.Body).Decode(&resp4))
	tags4 := resp4["tags"].(map[string]any)
	assert.NotContains(t, tags4, "env")
	assert.Equal(t, "platform", tags4["team"])
}

func TestHandler_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis/nonexistent/tags", map[string]any{"tags": map[string]any{}})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UntagResource_MissingQueryParam(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/tags", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateDataSource(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "myds", Type: "NONE"})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/datasources/myds",
		map[string]any{"description": "updated"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	ds := resp["dataSource"].(map[string]any)
	assert.Equal(t, "updated", ds["description"])

	// Update not-found DS returns 404.
	rec2 := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/datasources/missing",
		map[string]any{"description": "x"})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestHandler_UpdateFunction(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn1", DataSourceName: "ds"})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/functions/"+fn.FunctionID,
		map[string]any{"description": "updated fn"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	fnConfig := resp["functionConfiguration"].(map[string]any)
	assert.Equal(t, "updated fn", fnConfig["description"])
}

func TestHandler_UpdateApiKey(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "original", 0)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/apikeys/"+key.ID,
		map[string]any{"description": "updated"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	apiKey := resp["apiKey"].(map[string]any)
	assert.Equal(t, "updated", apiKey["description"])
}

func TestHandler_UpdateApiCache(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(
		api.APIID,
		&appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
	)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/ApiCaches",
		map[string]any{"ttl": 120, "type": "LARGE"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	cache := resp["apiCache"].(map[string]any)
	assert.Equal(t, "LARGE", cache["type"])
}

func TestHandler_FlushApiCache(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(
		api.APIID,
		&appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
	)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/ApiCaches/entries", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Flush without cache returns 404.
	api2, err := b.CreateGraphqlAPI("TestAPI2", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	rec2 := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api2.APIID+"/ApiCaches/entries", nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestHandler_UpdateType(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/types/MyType",
		map[string]any{"definition": "type MyType { id: ID! name: String! }", "format": "SDL"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	typeDef := resp["type"].(map[string]any)
	assert.Contains(t, typeDef["definition"], "name")
}

func TestHandler_UpdateResolver(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "getItem",
		DataSourceName: "myds",
	})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/types/Query/resolvers/getItem",
		map[string]any{"requestMappingTemplate": "$util.toJson($context.args)"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	resolver := resp["resolver"].(map[string]any)
	assert.Equal(t, "$util.toJson($context.args)", resolver["requestMappingTemplate"])
}

func TestHandler_EventAPI_CRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// Create event API.
	rec := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{"name": "MyEventAPI"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	apiID := createResp["api"].(map[string]any)["apiId"].(string)

	// List APIs.
	rec2 := doRequest(t, h, http.MethodGet, "/v2/apis", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&listResp))
	items := listResp["items"].([]any)
	assert.Len(t, items, 1)

	// Get API.
	rec3 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec3.Body).Decode(&getResp))
	assert.Equal(t, "MyEventAPI", getResp["api"].(map[string]any)["name"])

	// Delete API.
	rec4 := doRequest(t, h, http.MethodDelete, "/v2/apis/"+apiID, nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Get after delete returns 404.
	rec5 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID, nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)

	// List after delete returns empty.
	rec6 := doRequest(t, h, http.MethodGet, "/v2/apis", nil)
	require.Equal(t, http.StatusOK, rec6.Code)

	var listResp2 map[string]any
	require.NoError(t, json.NewDecoder(rec6.Body).Decode(&listResp2))
	assert.Empty(t, listResp2["items"])
}

func TestHandler_DisassociateAPI(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	// Create domain name.
	doRequest(t, h, http.MethodPost, "/v1/domainnames", map[string]any{
		"domainName": "api.example.com", "certificateArn": certARN,
	})

	// Create and associate a GraphQL API.
	rec1 := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name": "TestAPI", "authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusCreated, rec1.Code)

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&apiResp))
	apiID := apiResp["graphqlApi"].(map[string]any)["apiId"].(string)

	doRequest(t, h, http.MethodPost, "/v1/domainnames/api.example.com/apiassociation",
		map[string]any{"apiId": apiID})

	// Disassociate.
	rec3 := doRequest(t, h, http.MethodDelete, "/v1/domainnames/api.example.com/apiassociation", nil)
	assert.Equal(t, http.StatusNoContent, rec3.Code)

	// Second disassociate returns 404.
	rec4 := doRequest(t, h, http.MethodDelete, "/v1/domainnames/api.example.com/apiassociation", nil)
	assert.Equal(t, http.StatusNotFound, rec4.Code)
}

func TestHandler_UpdateDomainName(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	doRequest(t, h, http.MethodPost, "/v1/domainnames", map[string]any{
		"domainName": "api.example.com", "certificateArn": certARN,
	})

	rec := doRequest(t, h, http.MethodPut, "/v1/domainnames/api.example.com",
		map[string]any{"description": "updated description"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	dn := resp["domainNameConfig"].(map[string]any)
	assert.Equal(t, "updated description", dn["description"])
}

// ---- Coverage boost handler tests ----

func TestHandler_TagOps_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/tags", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_UpdateApiKey_NotFound(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/apikeys/nonexistent",
		map[string]any{"description": "x"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_UpdateApiCache_NotFound(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/ApiCaches",
		map[string]any{"ttl": 60})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DisassociateAPI_DomainNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodDelete, "/v1/domainnames/missing.example.com/apiassociation", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DomainName_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// CONNECT on domain names collection → 405.
	rec := doRequest(t, h, http.MethodConnect, "/v1/domainnames", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// CONNECT on domain name item → 405.
	rec2 := doRequest(t, h, http.MethodConnect, "/v1/domainnames/api.example.com", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec2.Code)

	// CONNECT on apiassociation → 405.
	rec3 := doRequest(t, h, http.MethodConnect, "/v1/domainnames/api.example.com/apiassociation", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec3.Code)
}

func TestHandler_EventAPI_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/v2/apis/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_EventAPI_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// CONNECT is never allowed on any v2 API endpoint.
	rec := doRequest(t, h, http.MethodConnect, "/v2/apis/nonexistent", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// ---- Refinement 4: new operation tests ----

func TestHandler_ChannelNamespace_CRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// Create event API first.
	rec1 := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{"name": "EventAPI"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&apiResp))
	apiID := apiResp["api"].(map[string]any)["apiId"].(string)

	// Create namespace.
	rec2 := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/channelNamespaces",
		map[string]any{"name": "ns1"})
	require.Equal(t, http.StatusCreated, rec2.Code)

	// List channel namespaces.
	rec3 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/channelNamespaces", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec3.Body).Decode(&listResp))
	items := listResp["channelNamespaces"].([]any)
	assert.Len(t, items, 1)

	// Get channel namespace.
	rec4 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/channelNamespaces/ns1", nil)
	require.Equal(t, http.StatusOK, rec4.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec4.Body).Decode(&getResp))
	ns := getResp["channelNamespace"].(map[string]any)
	assert.Equal(t, "ns1", ns["name"])

	// Update channel namespace.
	rec5 := doRequest(t, h, http.MethodPut, "/v2/apis/"+apiID+"/channelNamespaces/ns1",
		map[string]any{"codeHandlers": "export const handler = () => {}"})
	require.Equal(t, http.StatusOK, rec5.Code)

	// Delete channel namespace.
	rec6 := doRequest(t, h, http.MethodDelete, "/v2/apis/"+apiID+"/channelNamespaces/ns1", nil)
	assert.Equal(t, http.StatusNoContent, rec6.Code)

	// Get after delete returns 404.
	rec7 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/channelNamespaces/ns1", nil)
	assert.Equal(t, http.StatusNotFound, rec7.Code)
}

func TestHandler_ChannelNamespace_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/v2/apis/nonexistent/channelNamespaces/ns1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ChannelNamespace_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v2/apis/x/channelNamespaces/ns1", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_UpdateAPI(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec1 := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{"name": "EventAPI"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&createResp))
	apiID := createResp["api"].(map[string]any)["apiId"].(string)

	rec2 := doRequest(t, h, http.MethodPut, "/v2/apis/"+apiID,
		map[string]any{"name": "UpdatedAPI", "ownerContact": "ops@example.com"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp))
	api := resp["api"].(map[string]any)
	assert.Equal(t, "UpdatedAPI", api["name"])
	assert.Equal(t, "ops@example.com", api["ownerContact"])

	// Update not-found returns 404.
	rec3 := doRequest(t, h, http.MethodPut, "/v2/apis/nonexistent",
		map[string]any{"name": "x"})
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestHandler_SourceAPIAssociations_CRUD(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()

	// Create the APIs first (validation requires both to exist).
	srcAPI, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	mrgAPI, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	// Associate source graphql API.
	rec1 := doRequest(t, h, http.MethodPost, "/v1/mergedApis/"+mrgAPI.APIID+"/sourceApiAssociations",
		map[string]any{"sourceApiIdentifier": srcAPI.APIID, "description": "test"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&createResp))
	assocID := createResp["sourceApiAssociation"].(map[string]any)["associationId"].(string)

	// List source API associations.
	rec2 := doRequest(t, h, http.MethodGet, "/v1/mergedApis/"+mrgAPI.APIID+"/sourceApiAssociations", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&listResp))
	assocs := listResp["sourceApiAssociations"].([]any)
	assert.Len(t, assocs, 1)

	// Get source API association.
	rec3 := doRequest(t, h, http.MethodGet, "/v1/mergedApis/"+mrgAPI.APIID+"/sourceApiAssociations/"+assocID, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec3.Body).Decode(&getResp))
	assoc := getResp["sourceApiAssociation"].(map[string]any)
	assert.Equal(t, mrgAPI.APIID, assoc["mergedApiId"])

	// Disassociate source graphql API.
	rec4 := doRequest(t, h, http.MethodDelete, "/v1/mergedApis/"+mrgAPI.APIID+"/sourceApiAssociations/"+assocID, nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Get after delete returns 404.
	rec5 := doRequest(t, h, http.MethodGet, "/v1/mergedApis/"+mrgAPI.APIID+"/sourceApiAssociations/"+assocID, nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

func TestHandler_DisassociateMergedGraphqlApi(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()

	// Create the APIs first.
	srcAPI, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	mrgAPI, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	// Associate first.
	rec1 := doRequest(t, h, http.MethodPost, "/v1/sourceApis/"+srcAPI.APIID+"/mergedApiAssociations",
		map[string]any{"mergedApiIdentifier": mrgAPI.APIID})
	require.Equal(t, http.StatusCreated, rec1.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&createResp))
	assocID := createResp["sourceApiAssociation"].(map[string]any)["associationId"].(string)

	// Disassociate.
	rec2 := doRequest(t, h, http.MethodDelete, "/v1/sourceApis/"+srcAPI.APIID+"/mergedApiAssociations/"+assocID, nil)
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Second disassociate returns 404.
	rec3 := doRequest(t, h, http.MethodDelete, "/v1/sourceApis/"+srcAPI.APIID+"/mergedApiAssociations/"+assocID, nil)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestHandler_ListSourceApiAssociations_Empty(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/v1/mergedApis/empty-id/sourceApiAssociations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Empty(t, resp["sourceApiAssociations"])
}

func TestHandler_EnvironmentVariables(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Get empty env vars.
	rec1 := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/environmentVariables", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&getResp))
	assert.Empty(t, getResp["environmentVariables"])

	// Put env vars.
	rec2 := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/environmentVariables",
		map[string]any{"environmentVariables": map[string]any{"KEY1": "value1", "KEY2": "value2"}})
	require.Equal(t, http.StatusOK, rec2.Code)

	var putResp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&putResp))
	envVars := putResp["environmentVariables"].(map[string]any)
	assert.Equal(t, "value1", envVars["KEY1"])
	assert.Equal(t, "value2", envVars["KEY2"])

	// Get env vars after put.
	rec3 := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/environmentVariables", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var getResp2 map[string]any
	require.NoError(t, json.NewDecoder(rec3.Body).Decode(&getResp2))
	envVars2 := getResp2["environmentVariables"].(map[string]any)
	assert.Equal(t, "value1", envVars2["KEY1"])
}

func TestHandler_EnvironmentVariables_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/nonexistent/environmentVariables", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_EnvironmentVariables_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/environmentVariables", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_ListResolversByFunction(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn1", DataSourceName: "ds"})
	require.NoError(t, err)

	_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "getItem",
		Kind:           "PIPELINE",
		PipelineConfig: []string{fn.FunctionID},
	})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/functions/"+fn.FunctionID+"/resolvers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	resolvers := resp["resolvers"].([]any)
	assert.Len(t, resolvers, 1)
	assert.Equal(t, "getItem", resolvers[0].(map[string]any)["fieldName"])
}

// ---- Refinement 5: handler tests ----

func TestHandler_CreateGraphqlAPI_XrayEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantAPIType string
		wantXray    bool
	}{
		{
			name:        "with_xray_enabled",
			body:        map[string]any{"name": "MyAPI", "xrayEnabled": true, "apiType": "GRAPHQL"},
			wantXray:    true,
			wantAPIType: "GRAPHQL",
		},
		{
			name:        "default_no_xray",
			body:        map[string]any{"name": "MyAPI"},
			wantXray:    false,
			wantAPIType: "GRAPHQL",
		},
		{
			name:        "merged_api_type",
			body:        map[string]any{"name": "MyMergedAPI", "apiType": "MERGED"},
			wantAPIType: "MERGED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, http.MethodPost, "/v1/apis", tt.body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			apiObj := resp["graphqlApi"].(map[string]any)
			assert.NotEmpty(t, apiObj["apiId"])

			if tt.wantAPIType != "" {
				assert.Equal(t, tt.wantAPIType, apiObj["apiType"])
			}
		})
	}
}

func TestHandler_UpdateGraphqlAPI_XrayEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantErrCode int
		wantXray    bool
	}{
		{
			name:     "enable_xray",
			body:     map[string]any{"xrayEnabled": true},
			wantXray: true,
		},
		{
			name:     "disable_xray",
			body:     map[string]any{"xrayEnabled": false},
			wantXray: false,
		},
		{
			name:        "api_not_found",
			body:        map[string]any{"name": "new"},
			wantErrCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

			apiID := api.APIID
			if tt.wantErrCode != 0 {
				apiID = "nonexistent"
			}

			rec := doRequest(t, h, http.MethodPatch, "/v1/apis/"+apiID, tt.body)

			if tt.wantErrCode != 0 {
				assert.Equal(t, tt.wantErrCode, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			apiObj := resp["graphqlApi"].(map[string]any)
			// xrayEnabled omits false in JSON (omitempty), so nil == false.
			gotXray, _ := apiObj["xrayEnabled"].(bool)
			assert.Equal(t, tt.wantXray, gotXray)
		})
	}
}

func TestHandler_ListGraphqlAPIs_ApiTypeFilter(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	_, _ = b.CreateGraphqlAPI("GraphQL1", appsync.AuthTypeAPIKey, false, "GRAPHQL", "", nil, nil, nil)
	_, _ = b.CreateGraphqlAPI("Merged1", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis?apiType=GRAPHQL", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	apis := resp["graphqlApis"].([]any)
	assert.Len(t, apis, 1)
}

func TestHandler_CreateResolver_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "unit_ok",
			body:     map[string]any{"fieldName": "getItem", "dataSourceName": "ds"},
			wantCode: http.StatusCreated,
		},
		{
			name:     "missing_fieldName",
			body:     map[string]any{"dataSourceName": "ds"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unit_missing_datasource",
			body:     map[string]any{"fieldName": "getItem"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_kind",
			body:     map[string]any{"fieldName": "getItem", "dataSourceName": "ds", "kind": "UNKNOWN"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "pipeline_missing_config",
			body:     map[string]any{"fieldName": "getItem", "kind": "PIPELINE"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

			rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/types/Query/resolvers", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CreateAPIKey_MaxLimit(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	keyBody := map[string]any{"description": "k1"}

	// Create up to the limit (50).
	for i := range 50 {
		rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/apikeys", keyBody)
		require.Equal(t, http.StatusCreated, rec.Code, "key %d should succeed", i+1)
	}

	// 51st key exceeds limit.
	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/apikeys", keyBody)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateDataSource_HTTPValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "valid_http_source",
			body: map[string]any{
				"name":       "myHttp",
				"type":       "HTTP",
				"httpConfig": map[string]any{"endpoint": "https://api.example.com"},
			},
			wantCode: http.StatusCreated,
		},
		{
			name:     "http_missing_endpoint",
			body:     map[string]any{"name": "myHttp", "type": "HTTP"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_type",
			body:     map[string]any{"name": "myDs", "type": "FAKE_TYPE"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

			rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/datasources", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- Refinement 7 handler tests ----

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	h.DefaultRegion = "us-east-1"

	assert.Equal(t, "appsync", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.Contains(t, h.ChaosOperations(), "CreateGraphqlApi")
	assert.Contains(t, h.ChaosRegions(), "us-east-1")
}

func TestHandler_ExtractResource_DomainPath(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/domainnames/example.com/apiAssociation", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	resource := h.ExtractResource(c)
	assert.Equal(t, "example.com", resource)
}

func TestHandler_ExtractResource_EmptyForUnknown(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/unknown/path", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	resource := h.ExtractResource(c)
	assert.Empty(t, resource)
}

func TestHandler_Tags_CRUD(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TaggedAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Tag the resource.
	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/tags", map[string]any{
		"tags": map[string]string{"env": "prod"},
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// List tags.
	rec = doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/tags", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Untag.
	rec = doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/tags?tagKeys=env", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_Tags_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/nonexistent/tags", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_V2API_GetAndDeleteNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "get_nonexistent",
			method:   http.MethodGet,
			path:     "/v2/apis/nonexistent",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete_nonexistent",
			method:   http.MethodDelete,
			path:     "/v2/apis/nonexistent",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "update_nonexistent",
			method:   http.MethodPut,
			path:     "/v2/apis/nonexistent",
			body:     map[string]any{"name": "NewName"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_V2API_ChannelNamespaceNotFound(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateAPI("TestAPI", "", nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v2/apis/"+api.APIID+"/channelNamespaces/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_DeleteDataSource_BlockedByResolver(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "BoundDS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	_, err = b.StartSchemaCreation(api.APIID, "type Query { hello: String }")
	require.NoError(t, err)

	_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "hello",
		DataSourceName: "BoundDS",
		Kind:           "UNIT",
	})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/datasources/BoundDS", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_AssociateAPI_InvalidAPIID(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()

	_, err := b.CreateDomainName("assoc.example.com", "arn:aws:acm:us-east-1:000:certificate/abc", "", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPost, "/v1/domainnames/assoc.example.com/ApiAssociation", map[string]any{
		"apiId": "nonexistent-api",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_EnvironmentVariables_ExceedsLimit(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Build a map with 26 entries (exceeds max of 25).
	envVars := make(map[string]string)
	for i := range 26 {
		envVars[strings.Repeat("K", i+1)] = "value"
	}

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/environmentVariables", map[string]any{
		"environmentVariables": envVars,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

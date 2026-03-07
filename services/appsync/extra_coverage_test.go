package appsync_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appsync"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Provider tests ----

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &appsync.Provider{}
	assert.Equal(t, "AppSync", p.Name())
}

func TestProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &appsync.Provider{}
	svc, err := p.Init(nil)
	require.NoError(t, err)
	require.NotNil(t, svc)
}

func TestProvider_Init_WithCtx(t *testing.T) {
	t.Parallel()

	p := &appsync.Provider{}
	ctx := &service.AppContext{}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, svc)
}

// ---- GraphQL resolveValue / findOperation coverage ----

func TestInMemoryBackend_ExecuteGraphQL_NamedOperation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "NoneDS",
		Type: appsync.DataSourceTypeNone,
	})
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "hello",
		DataSourceName: "NoneDS",
	})

	result, err := b.ExecuteGraphQL(t.Context(), api.APIID,
		`query MyQuery { hello }`, "MyQuery", nil)
	require.NoError(t, err)
	assert.Contains(t, result, "hello")
}

func TestInMemoryBackend_ExecuteGraphQL_OperationNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)

	_, err := b.ExecuteGraphQL(t.Context(), api.APIID,
		`query MyQuery { hello }`, "NonExistentOp", nil)
	require.Error(t, err)
}

func TestInMemoryBackend_ExecuteGraphQL_Subscription(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `
		type Query { dummy: String }
		type Subscription { onEvent: String }
	`)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "NoneDS",
		Type: appsync.DataSourceTypeNone,
	})
	_, _ = b.CreateResolver(api.APIID, "Subscription", &appsync.Resolver{
		FieldName:      "onEvent",
		DataSourceName: "NoneDS",
	})

	result, err := b.ExecuteGraphQL(t.Context(), api.APIID,
		`subscription { onEvent }`, "", nil)
	require.NoError(t, err)
	assert.Contains(t, result, "onEvent")
}

// ---- DynamoDB resolver tests ----

// mockDynamoDB is a test double for DynamoDB operations.
type mockDynamoDB struct {
	items map[string]map[string]any
}

func (m *mockDynamoDB) GetItemRaw(_ context.Context, tableName string, key map[string]any) (map[string]any, error) {
	tableKey := fmt.Sprintf("%s::%v", tableName, key)
	if item, ok := m.items[tableKey]; ok {
		return item, nil
	}

	return map[string]any{}, nil
}

func (m *mockDynamoDB) PutItemRaw(_ context.Context, tableName string, item map[string]any) error {
	if m.items == nil {
		m.items = make(map[string]map[string]any)
	}

	m.items[fmt.Sprintf("%s::%v", tableName, item)] = item

	return nil
}

func TestInMemoryBackend_ExecuteGraphQL_DynamoDBResolver_GetItem(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ddb := &mockDynamoDB{}
	b.SetDynamoDBBackend(ddb)

	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { getItem(id: String): String }`)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DDBDataSource",
		Type: appsync.DataSourceTypeDynamoDB,
		DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{
			TableName: "items",
			AWSRegion: "us-east-1",
		},
	})
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:               "getItem",
		DataSourceName:          "DDBDataSource",
		RequestMappingTemplate:  `{"operation": "GetItem", "key": {"id": "$ctx.args.id"}}`,
		ResponseMappingTemplate: `$util.toJson($context.result)`,
	})

	result, err := b.ExecuteGraphQL(t.Context(), api.APIID,
		`query { getItem(id: "123") }`, "", nil)
	require.NoError(t, err)
	assert.Contains(t, result, "getItem")
}

func TestInMemoryBackend_ExecuteGraphQL_DynamoDBResolver_NoTemplate(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ddb := &mockDynamoDB{}
	b.SetDynamoDBBackend(ddb)

	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { getItem(id: String): String }`)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name:           "DDBDataSource",
		Type:           appsync.DataSourceTypeDynamoDB,
		DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{TableName: "items"},
	})
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "getItem",
		DataSourceName: "DDBDataSource",
		// No request template — uses default GetItem.
	})

	result, err := b.ExecuteGraphQL(t.Context(), api.APIID,
		`query { getItem(id: "123") }`, "", nil)
	require.NoError(t, err)
	assert.Contains(t, result, "getItem")
}

func TestInMemoryBackend_ExecuteGraphQL_DynamoDBResolver_UnsupportedOperation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ddb := &mockDynamoDB{}
	b.SetDynamoDBBackend(ddb)

	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { getItem(id: String): String }`)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name:           "DDBDataSource",
		Type:           appsync.DataSourceTypeDynamoDB,
		DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{TableName: "items"},
	})
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:              "getItem",
		DataSourceName:         "DDBDataSource",
		RequestMappingTemplate: `{"operation": "Scan"}`,
	})

	_, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { getItem(id: "1") }`, "", nil)
	require.Error(t, err)
}

func TestInMemoryBackend_ExecuteGraphQL_DynamoDBResolver_NilConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ddb := &mockDynamoDB{}
	b.SetDynamoDBBackend(ddb)

	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { getItem(id: String): String }`)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DDBDataSource",
		Type: appsync.DataSourceTypeDynamoDB,
		// No DynamoDBConfig.
	})
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "getItem",
		DataSourceName: "DDBDataSource",
	})

	_, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { getItem(id: "1") }`, "", nil)
	require.Error(t, err)
}

func TestInMemoryBackend_ExecuteGraphQL_DynamoDBResolver_NilBackend(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	// Do NOT set DynamoDB backend.

	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { getItem(id: String): String }`)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name:           "DDBDataSource",
		Type:           appsync.DataSourceTypeDynamoDB,
		DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{TableName: "items"},
	})
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "getItem",
		DataSourceName: "DDBDataSource",
	})

	_, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { getItem(id: "1") }`, "", nil)
	require.Error(t, err)
}

func TestInMemoryBackend_ExecuteGraphQL_LambdaResolver_NilInvoker(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	// Do NOT set lambda invoker.

	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "LambdaDS",
		Type: appsync.DataSourceTypeLambda,
		LambdaConfig: &appsync.LambdaDataSourceConfig{
			LambdaFunctionARN: "arn:aws:lambda:us-east-1:000:function:fn",
		},
	})
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "hello",
		DataSourceName: "LambdaDS",
	})

	_, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { hello }`, "", nil)
	require.Error(t, err)
}

func TestInMemoryBackend_ExecuteGraphQL_LambdaResolver_NilLambdaConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	mock := &mockLambdaInvoker{}
	b.SetLambdaInvoker(mock)

	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "LambdaDS",
		Type: appsync.DataSourceTypeLambda,
		// No LambdaConfig set.
	})
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "hello",
		DataSourceName: "LambdaDS",
	})

	_, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { hello }`, "", nil)
	require.Error(t, err)
}

func TestInMemoryBackend_ExecuteGraphQL_NilResolver(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)

	// No resolvers defined at all — field should return nil.
	result, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { hello }`, "", nil)
	require.NoError(t, err)
	assert.Nil(t, result["hello"])
}

func TestInMemoryBackend_ExecuteGraphQL_MissingDataSource(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
	// Create resolver but NOT the data source.
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "hello",
		DataSourceName: "MissingDS",
	})

	_, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { hello }`, "", nil)
	require.Error(t, err)
}

// ---- Handler base64 schema test ----

func TestHandler_StartSchemaCreation_Base64Encoded(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	sdl := `type Query { hello: String }`
	encoded := base64.StdEncoding.EncodeToString([]byte(sdl))

	body := map[string]any{"definition": encoded}
	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/schemacreation", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Handler error paths ----

func TestHandler_HandleError_InternalError(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	// Schema with unsupported data source causes InternalFailure.
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "HTTPDS",
		Type: appsync.DataSourceTypeHTTP,
	})
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "hello",
		DataSourceName: "HTTPDS",
	})

	body := map[string]any{"query": `query { hello }`}
	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/graphql", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_GraphQL_NoSchema(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	// No schema uploaded.

	body := map[string]any{"query": `query { hello }`}
	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/graphql", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_Types_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	// PUT on resolver should return method not allowed.
	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/types/Query/resolvers", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_Types_ResolverMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	// PUT on individual resolver should return method not allowed.
	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/types/Query/resolvers/getItem", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_DataSource_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

	// PUT on named datasource.
	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/datasources/myds", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_Unknown_Short_Segs(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v1/unknown", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- VTL resolveExpr coverage ----

func TestRenderVTL_ResolveExpr_AllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tmpl   string
		args   map[string]any
		result any
		want   string
	}{
		{
			name:   "resolve_context_arguments_key",
			tmpl:   "$util.toJson($context.arguments.id)",
			args:   map[string]any{"id": "test"},
			result: nil,
			want:   `"test"`,
		},
		{
			name:   "resolve_context_arguments_bare",
			tmpl:   "$util.toJson($context.arguments)",
			args:   map[string]any{"x": "y"},
			result: nil,
			want:   `{"x":"y"}`,
		},
		{
			name:   "resolve_ctx_args_key",
			tmpl:   "$util.toJson($ctx.args.name)",
			args:   map[string]any{"name": "alice"},
			result: nil,
			want:   `"alice"`,
		},
		{
			name:   "resolve_ctx_args_bare",
			tmpl:   "$util.toJson($ctx.args)",
			args:   map[string]any{"a": "b"},
			result: nil,
			want:   `{"a":"b"}`,
		},
		{
			name:   "resolve_context_result_bare",
			tmpl:   "$util.toJson($context.result)",
			args:   nil,
			result: map[string]any{"id": "1"},
			want:   `{"id":"1"}`,
		},
		{
			name:   "unknown_expr_passthrough",
			tmpl:   "$util.toJson(something_unknown)",
			args:   nil,
			result: nil,
			want:   `"something_unknown"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := appsync.RenderVTL(tt.tmpl, tt.args, tt.result)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

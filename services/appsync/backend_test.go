package appsync_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBackend() *appsync.InMemoryBackend {
	return appsync.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")
}

// ---- GraphqlApi tests ----

func TestInMemoryBackend_CreateGraphqlAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		apiName  string
		authType appsync.AuthenticationType
		wantName string
		wantAuth appsync.AuthenticationType
		wantErr  bool
	}{
		{
			name:     "creates_api_with_api_key_auth",
			apiName:  "MyAPI",
			authType: appsync.AuthTypeAPIKey,
			wantName: "MyAPI",
			wantAuth: appsync.AuthTypeAPIKey,
		},
		{
			name:     "creates_api_with_iam_auth",
			apiName:  "IAMApi",
			authType: appsync.AuthTypeIAM,
			wantName: "IAMApi",
			wantAuth: appsync.AuthTypeIAM,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI(tt.apiName, tt.authType, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantName, api.Name)
			assert.Equal(t, tt.wantAuth, api.AuthenticationType)
			assert.NotEmpty(t, api.APIID)
			assert.NotEmpty(t, api.ARN)
			assert.Contains(t, api.URIs["GRAPHQL"], api.APIID)
		})
	}
}

func TestInMemoryBackend_GetGraphqlAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*appsync.InMemoryBackend) string
		apiID   string
		wantErr bool
	}{
		{
			name: "returns_existing_api",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

				return api.APIID
			},
			wantErr: false,
		},
		{
			name:    "returns_not_found_for_missing_api",
			setup:   func(_ *appsync.InMemoryBackend) string { return "nonexistent" },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			apiID := tt.setup(b)
			api, err := b.GetGraphqlAPI(apiID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, apiID, api.APIID)
		})
	}
}

func TestInMemoryBackend_ListGraphqlAPIs(t *testing.T) {
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

			b := newTestBackend()
			tt.setup(b)
			apis, err := b.ListGraphqlAPIs()
			require.NoError(t, err)
			assert.Len(t, apis, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_DeleteGraphqlAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		apiID   string
		wantErr bool
	}{
		{
			name:    "deletes_existing_api",
			wantErr: false,
		},
		{
			name:    "error_for_missing_api",
			apiID:   "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			apiID := tt.apiID

			if apiID == "" {
				api, _ := b.CreateGraphqlAPI("ToDelete", appsync.AuthTypeAPIKey, nil)
				apiID = api.APIID
			}

			err := b.DeleteGraphqlAPI(apiID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)

			_, getErr := b.GetGraphqlAPI(apiID)
			require.Error(t, getErr)
		})
	}
}

// ---- Schema tests ----

func TestInMemoryBackend_StartSchemaCreation(t *testing.T) {
	t.Parallel()

	validSchema := `type Query { hello: String }`
	invalidSchema := `type { broken schema }`

	tests := []struct {
		name       string
		sdl        string
		wantStatus appsync.SchemaStatus
		wantErr    bool
	}{
		{
			name:       "valid_schema_is_accepted",
			sdl:        validSchema,
			wantStatus: appsync.SchemaStatusActive,
		},
		{
			name:    "invalid_schema_returns_error",
			sdl:     invalidSchema,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			schema, err := b.StartSchemaCreation(api.APIID, tt.sdl)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, schema.Status)
		})
	}
}

func TestInMemoryBackend_GetSchemaCreationStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*appsync.InMemoryBackend, string)
		wantStatus appsync.SchemaStatus
	}{
		{
			name:       "returns_not_applicable_when_no_schema",
			setup:      func(_ *appsync.InMemoryBackend, _ string) {},
			wantStatus: appsync.SchemaStatusNotApplicable,
		},
		{
			name: "returns_active_after_valid_schema_upload",
			setup: func(b *appsync.InMemoryBackend, apiID string) {
				_, _ = b.StartSchemaCreation(apiID, `type Query { hello: String }`)
			},
			wantStatus: appsync.SchemaStatusActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			tt.setup(b, api.APIID)
			schema, err := b.GetSchemaCreationStatus(api.APIID)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, schema.Status)
		})
	}
}

// ---- DataSource tests ----

func TestInMemoryBackend_CreateDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ds      *appsync.DataSource
		name    string
		wantErr bool
	}{
		{
			name: "creates_lambda_datasource",
			ds: &appsync.DataSource{
				Name: "LambdaDS",
				Type: appsync.DataSourceTypeLambda,
				LambdaConfig: &appsync.LambdaDataSourceConfig{
					LambdaFunctionARN: "arn:aws:lambda:us-east-1:000000000000:function:test",
				},
			},
		},
		{
			name: "creates_none_datasource",
			ds: &appsync.DataSource{
				Name: "NoneDS",
				Type: appsync.DataSourceTypeNone,
			},
		},
		{
			name: "error_on_duplicate_name",
			ds: &appsync.DataSource{
				Name: "DupDS",
				Type: appsync.DataSourceTypeNone,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

			if tt.wantErr {
				// Create once to set up duplicate condition.
				_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
					Name: tt.ds.Name,
					Type: appsync.DataSourceTypeNone,
				})
			}

			ds, err := b.CreateDataSource(api.APIID, tt.ds)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrAlreadyExists)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.ds.Name, ds.Name)
			assert.Equal(t, api.APIID, ds.APIID)
			assert.NotEmpty(t, ds.DataSourceARN)
		})
	}
}

// ---- Resolver tests ----

func TestInMemoryBackend_CreateResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		resolver  *appsync.Resolver
		typeName  string
		wantErr   bool
		duplicate bool
	}{
		{
			name:     "creates_resolver",
			typeName: "Query",
			resolver: &appsync.Resolver{
				FieldName:      "getItem",
				DataSourceName: "MyDS",
			},
		},
		{
			name:      "error_on_duplicate_resolver",
			typeName:  "Query",
			resolver:  &appsync.Resolver{FieldName: "getItem", DataSourceName: "MyDS"},
			wantErr:   true,
			duplicate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

			if tt.duplicate {
				_, _ = b.CreateResolver(api.APIID, tt.typeName, &appsync.Resolver{
					FieldName: tt.resolver.FieldName,
				})
			}

			r, err := b.CreateResolver(api.APIID, tt.typeName, tt.resolver)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrAlreadyExists)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.resolver.FieldName, r.FieldName)
			assert.Equal(t, tt.typeName, r.TypeName)
			assert.Equal(t, api.APIID, r.APIID)
			assert.NotEmpty(t, r.ResolverARN)
		})
	}
}

func TestInMemoryBackend_ListResolvers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(*appsync.InMemoryBackend, string)
		typeName  string
		wantCount int
	}{
		{
			name:      "empty_list",
			setup:     func(_ *appsync.InMemoryBackend, _ string) {},
			typeName:  "Query",
			wantCount: 0,
		},
		{
			name: "returns_resolvers_for_type",
			setup: func(b *appsync.InMemoryBackend, apiID string) {
				_, _ = b.CreateResolver(apiID, "Query", &appsync.Resolver{FieldName: "getItem"})
				_, _ = b.CreateResolver(apiID, "Query", &appsync.Resolver{FieldName: "listItems"})
				_, _ = b.CreateResolver(apiID, "Mutation", &appsync.Resolver{FieldName: "createItem"})
			},
			typeName:  "Query",
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			tt.setup(b, api.APIID)
			resolvers, err := b.ListResolvers(api.APIID, tt.typeName)
			require.NoError(t, err)
			assert.Len(t, resolvers, tt.wantCount)
		})
	}
}

// ---- VTL tests ----

func TestRenderVTL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tmpl   string
		args   map[string]any
		result any
		want   string
	}{
		{
			name:   "empty_template_with_nil_result",
			tmpl:   "",
			args:   nil,
			result: nil,
			want:   "{}",
		},
		{
			name:   "empty_template_with_result",
			tmpl:   "",
			args:   nil,
			result: map[string]any{"id": "123"},
			want:   `{"id":"123"}`,
		},
		{
			name:   "substitutes_context_arguments",
			tmpl:   `{"id": "$context.arguments.id"}`,
			args:   map[string]any{"id": "abc"},
			result: nil,
			want:   `{"id": "abc"}`,
		},
		{
			name:   "substitutes_ctx_args",
			tmpl:   `{"key": "$ctx.args.key"}`,
			args:   map[string]any{"key": "val"},
			result: nil,
			want:   `{"key": "val"}`,
		},
		{
			name:   "substitutes_context_result",
			tmpl:   "$util.toJson($context.result)",
			args:   nil,
			result: map[string]any{"name": "test"},
			want:   `{"name":"test"}`,
		},
		{
			name:   "substitutes_context_result_field",
			tmpl:   "$context.result.name",
			args:   nil,
			result: map[string]any{"name": "hello"},
			want:   "hello",
		},
		{
			name:   "handles_return_directive",
			tmpl:   "#return($context.result)",
			args:   nil,
			result: map[string]any{"id": "1"},
			want:   `{"id":"1"}`,
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

// ---- ExecuteGraphQL tests ----

// mockLambdaInvoker is a test double for Lambda invocations.
type mockLambdaInvoker struct {
	payload []byte
	calls   []invocationCall
}

type invocationCall struct {
	name    string
	invType string
	payload []byte
}

func (m *mockLambdaInvoker) InvokeFunction(
	_ context.Context,
	name, invType string,
	payload []byte,
) ([]byte, int, error) {
	m.calls = append(m.calls, invocationCall{name: name, invType: invType, payload: payload})

	if m.payload != nil {
		return m.payload, 200, nil
	}

	return []byte(`{"result":"ok"}`), 200, nil
}

func TestInMemoryBackend_ExecuteGraphQL_LambdaResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantValue     any
		name          string
		schema        string
		query         string
		wantField     string
		lambdaPayload []byte
		wantCalls     int
	}{
		{
			name:          "executes_query_via_lambda_resolver",
			schema:        `type Query { hello: String }`,
			query:         `query { hello }`,
			lambdaPayload: []byte(`"world"`),
			wantField:     "hello",
			wantValue:     "world",
			wantCalls:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			mock := &mockLambdaInvoker{payload: tt.lambdaPayload}
			b.SetLambdaInvoker(mock)

			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			_, _ = b.StartSchemaCreation(api.APIID, tt.schema)
			_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "LambdaDS",
				Type: appsync.DataSourceTypeLambda,
				LambdaConfig: &appsync.LambdaDataSourceConfig{
					LambdaFunctionARN: "arn:aws:lambda:us-east-1:000000000000:function:hello-fn",
				},
			})
			_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
				FieldName:      "hello",
				DataSourceName: "LambdaDS",
			})

			result, err := b.ExecuteGraphQL(t.Context(), api.APIID, tt.query, "", nil)
			require.NoError(t, err)
			assert.Len(t, mock.calls, tt.wantCalls)
			assert.Equal(t, tt.wantValue, result[tt.wantField])
		})
	}
}

func TestInMemoryBackend_ExecuteGraphQL_NoneResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		schema    string
		query     string
		variables map[string]any
		wantField string
		wantErr   bool
	}{
		{
			name:      "none_resolver_returns_args",
			schema:    `type Query { echo(message: String): String }`,
			query:     `query { echo(message: "hi") }`,
			wantField: "echo",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			_, _ = b.StartSchemaCreation(api.APIID, tt.schema)
			_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "NoneDS",
				Type: appsync.DataSourceTypeNone,
			})
			_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
				FieldName:      "echo",
				DataSourceName: "NoneDS",
			})

			result, err := b.ExecuteGraphQL(t.Context(), api.APIID, tt.query, "", tt.variables)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, result, tt.wantField)
		})
	}
}

func TestInMemoryBackend_ExecuteGraphQL_NoSchema(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { hello }`, "", nil)
	require.Error(t, err)
}

func TestInMemoryBackend_ExecuteGraphQL_InvalidQuery(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)

	_, err := b.ExecuteGraphQL(t.Context(), api.APIID, `{ not valid gql`, "", nil)
	require.Error(t, err)
}

// ---- Additional backend coverage tests ----

func TestInMemoryBackend_GetDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dsName  string
		wantErr bool
	}{
		{
			name:    "returns_existing_datasource",
			dsName:  "MyDS",
			wantErr: false,
		},
		{
			name:    "returns_not_found_for_missing_datasource",
			dsName:  "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "MyDS",
				Type: appsync.DataSourceTypeNone,
			})

			ds, err := b.GetDataSource(api.APIID, tt.dsName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.dsName, ds.Name)
		})
	}
}

func TestInMemoryBackend_ListDataSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*appsync.InMemoryBackend, string)
		name      string
		wantCount int
	}{
		{
			name:      "empty_for_new_api",
			setup:     func(_ *appsync.InMemoryBackend, _ string) {},
			wantCount: 0,
		},
		{
			name: "returns_all_datasources",
			setup: func(b *appsync.InMemoryBackend, apiID string) {
				_, _ = b.CreateDataSource(apiID, &appsync.DataSource{Name: "DS1", Type: appsync.DataSourceTypeNone})
				_, _ = b.CreateDataSource(apiID, &appsync.DataSource{
					Name: "DS2",
					Type: appsync.DataSourceTypeLambda,
					LambdaConfig: &appsync.LambdaDataSourceConfig{
						LambdaFunctionARN: "arn:aws:lambda:us-east-1:000:function:fn",
					},
				})
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			tt.setup(b, api.APIID)
			dss, err := b.ListDataSources(api.APIID)
			require.NoError(t, err)
			assert.Len(t, dss, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_DeleteDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dsName  string
		wantErr bool
	}{
		{
			name:    "deletes_existing_datasource",
			dsName:  "MyDS",
			wantErr: false,
		},
		{
			name:    "error_for_missing_datasource",
			dsName:  "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "MyDS",
				Type: appsync.DataSourceTypeNone,
			})

			err := b.DeleteDataSource(api.APIID, tt.dsName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)

			// Verify it's gone.
			_, getErr := b.GetDataSource(api.APIID, "MyDS")
			require.Error(t, getErr)
		})
	}
}

func TestInMemoryBackend_GetResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fieldName string
		wantErr   bool
	}{
		{
			name:      "returns_existing_resolver",
			fieldName: "getItem",
			wantErr:   false,
		},
		{
			name:      "returns_not_found_for_missing_resolver",
			fieldName: "nonexistent",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "getItem"})

			r, err := b.GetResolver(api.APIID, "Query", tt.fieldName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.fieldName, r.FieldName)
		})
	}
}

func TestInMemoryBackend_DeleteResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fieldName string
		wantErr   bool
	}{
		{
			name:      "deletes_existing_resolver",
			fieldName: "getItem",
			wantErr:   false,
		},
		{
			name:      "error_for_missing_resolver",
			fieldName: "nonexistent",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "getItem"})

			err := b.DeleteResolver(api.APIID, "Query", tt.fieldName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_GetIntrospectionSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantSDL   string
		hasSchema bool
		wantErr   bool
	}{
		{
			name:      "returns_schema_sdl",
			hasSchema: true,
			wantSDL:   `type Query { hello: String }`,
		},
		{
			name:      "error_when_no_schema",
			hasSchema: false,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)

			if tt.hasSchema {
				_, _ = b.StartSchemaCreation(api.APIID, tt.wantSDL)
			}

			sdl, err := b.GetIntrospectionSchema(api.APIID, "SDL")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantSDL, string(sdl))
		})
	}
}

func TestInMemoryBackend_SetDynamoDBBackend(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	b.SetDynamoDBBackend(nil) // just ensure it's callable
}

func TestInMemoryBackend_ExecuteGraphQL_MissingAPI(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.ExecuteGraphQL(t.Context(), "nonexistent", `query { hello }`, "", nil)
	require.Error(t, err)
}

func TestInMemoryBackend_ExecuteGraphQL_LambdaResolver_WithTemplates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		schema        string
		query         string
		reqTemplate   string
		respTemplate  string
		wantField     string
		lambdaPayload []byte
		wantCalls     int
	}{
		{
			name:          "uses_request_template",
			schema:        `type Query { greet(name: String): String }`,
			query:         `query { greet(name: "Alice") }`,
			reqTemplate:   `{"name": "$ctx.args.name"}`,
			respTemplate:  `$util.toJson($context.result)`,
			lambdaPayload: []byte(`"Hello, Alice"`),
			wantField:     "greet",
			wantCalls:     1,
		},
		{
			name:          "no_template_passes_args",
			schema:        `type Query { greet(name: String): String }`,
			query:         `query { greet(name: "Bob") }`,
			reqTemplate:   "",
			respTemplate:  "",
			lambdaPayload: []byte(`"Hi Bob"`),
			wantField:     "greet",
			wantCalls:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			mock := &mockLambdaInvoker{payload: tt.lambdaPayload}
			b.SetLambdaInvoker(mock)

			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			_, _ = b.StartSchemaCreation(api.APIID, tt.schema)
			_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "LambdaDS",
				Type: appsync.DataSourceTypeLambda,
				LambdaConfig: &appsync.LambdaDataSourceConfig{
					LambdaFunctionARN: "arn:aws:lambda:us-east-1:000:function:fn",
				},
			})
			_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
				FieldName:               "greet",
				DataSourceName:          "LambdaDS",
				RequestMappingTemplate:  tt.reqTemplate,
				ResponseMappingTemplate: tt.respTemplate,
			})

			result, err := b.ExecuteGraphQL(t.Context(), api.APIID, tt.query, "", nil)
			require.NoError(t, err)
			assert.Len(t, mock.calls, tt.wantCalls)
			assert.NotNil(t, result[tt.wantField])
		})
	}
}

func TestInMemoryBackend_ExecuteGraphQL_NoneResolver_WithTemplates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		schema       string
		query        string
		reqTemplate  string
		respTemplate string
		wantField    string
	}{
		{
			name:         "none_resolver_with_response_template",
			schema:       `type Query { echo(msg: String): String }`,
			query:        `query { echo(msg: "hello") }`,
			reqTemplate:  `{"msg": "$ctx.args.msg"}`,
			respTemplate: `$util.toJson($context.result)`,
			wantField:    "echo",
		},
		{
			name:         "none_resolver_bare_args",
			schema:       `type Query { echo(msg: String): String }`,
			query:        `query { echo(msg: "hi") }`,
			reqTemplate:  "",
			respTemplate: "",
			wantField:    "echo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
			_, _ = b.StartSchemaCreation(api.APIID, tt.schema)
			_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "NoneDS",
				Type: appsync.DataSourceTypeNone,
			})
			_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
				FieldName:               "echo",
				DataSourceName:          "NoneDS",
				RequestMappingTemplate:  tt.reqTemplate,
				ResponseMappingTemplate: tt.respTemplate,
			})

			result, err := b.ExecuteGraphQL(t.Context(), api.APIID, tt.query, "", nil)
			require.NoError(t, err)
			assert.Contains(t, result, tt.wantField)
		})
	}
}

func TestInMemoryBackend_ExecuteGraphQL_Mutation(t *testing.T) {
	t.Parallel()

	schema := `type Query { dummy: String }
type Mutation { createItem(name: String): String }`
	query := `mutation { createItem(name: "test") }`

	b := newTestBackend()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, schema)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "NoneDS",
		Type: appsync.DataSourceTypeNone,
	})
	_, _ = b.CreateResolver(api.APIID, "Mutation", &appsync.Resolver{
		FieldName:      "createItem",
		DataSourceName: "NoneDS",
	})

	result, err := b.ExecuteGraphQL(t.Context(), api.APIID, query, "", nil)
	require.NoError(t, err)
	assert.Contains(t, result, "createItem")
}

func TestInMemoryBackend_ExecuteGraphQL_UnsupportedDataSource(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
	_, _ = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "HTTPDS",
		Type: appsync.DataSourceTypeHTTP,
	})
	_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "hello",
		DataSourceName: "HTTPDS",
	})

	_, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { hello }`, "", nil)
	require.Error(t, err)
}

// ---- VTL extended tests ----

func TestRenderVTL_Extended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tmpl   string
		args   map[string]any
		result any
		want   string
	}{
		{
			name:   "bare_context_arguments",
			tmpl:   "$context.arguments",
			args:   map[string]any{"key": "val"},
			result: nil,
			want:   `{"key":"val"}`,
		},
		{
			name:   "bare_ctx_args",
			tmpl:   "$ctx.args",
			args:   map[string]any{"x": 1.0},
			result: nil,
			want:   `{"x":1}`,
		},
		{
			name:   "util_to_json_bare_result",
			tmpl:   "$util.toJson($context.result)",
			args:   nil,
			result: "hello",
			want:   `"hello"`,
		},
		{
			name:   "dynamodb_to_json_string",
			tmpl:   `$util.dynamodb.toDynamoDBJson($ctx.args.id)`,
			args:   map[string]any{"id": "abc"},
			result: nil,
			want:   `{"S":"abc"}`,
		},
		{
			name:   "result_field_not_found_returns_null",
			tmpl:   "$context.result.missing",
			args:   nil,
			result: map[string]any{"other": "val"},
			want:   "null",
		},
		{
			name:   "nil_result_returns_null",
			tmpl:   "$context.result",
			args:   nil,
			result: nil,
			want:   "null",
		},
		{
			name:   "missing_arg_returns_null",
			tmpl:   "$ctx.args.missing",
			args:   map[string]any{"other": "val"},
			result: nil,
			want:   "null",
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

func TestToDynamoDBJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input any
		want  string
	}{
		{name: "string", input: "hello", want: `{"S":"hello"}`},
		{name: "float64", input: float64(42), want: `{"N":"42"}`},
		{name: "bool_true", input: true, want: `{"BOOL":true}`},
		{name: "bool_false", input: false, want: `{"BOOL":false}`},
		{name: "nil", input: nil, want: `{"NULL":true}`},
		{name: "map_passes_through", input: map[string]any{"x": 1}, want: `{"x":1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := appsync.ToDynamoDBJSON(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

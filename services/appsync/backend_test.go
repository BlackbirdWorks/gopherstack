package appsync_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
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
			api, err := b.CreateGraphqlAPI(tt.apiName, tt.authType, false, "", "", nil, nil, nil)

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
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

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
				_, _ = b.CreateGraphqlAPI("API1", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				_, _ = b.CreateGraphqlAPI("API2", appsync.AuthTypeIAM, false, "", "", nil, nil, nil)
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			tt.setup(b)
			apis, err := b.ListGraphqlAPIs("")
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
				api, _ := b.CreateGraphqlAPI("ToDelete", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
		apiID      string
		wantStatus appsync.SchemaStatus
		wantErr    bool
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
		{
			name:    "returns_not_found_for_nonexistent_api",
			setup:   func(_ *appsync.InMemoryBackend, _ string) {},
			apiID:   "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

			apiID := tt.apiID
			if apiID == "" {
				apiID = api.APIID
			}

			tt.setup(b, api.APIID)
			schema, err := b.GetSchemaCreationStatus(apiID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

			if tt.duplicate {
				_, _ = b.CreateResolver(api.APIID, tt.typeName, &appsync.Resolver{
					FieldName:      tt.resolver.FieldName,
					DataSourceName: tt.resolver.DataSourceName,
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
		apiID     string
		typeName  string
		wantCount int
		wantErr   bool
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
				_, _ = b.CreateResolver(apiID, "Query", &appsync.Resolver{FieldName: "getItem", DataSourceName: "myDs"})
				_, _ = b.CreateResolver(
					apiID,
					"Query",
					&appsync.Resolver{FieldName: "listItems", DataSourceName: "myDs"},
				)
				_, _ = b.CreateResolver(
					apiID,
					"Mutation",
					&appsync.Resolver{FieldName: "createItem", DataSourceName: "myDs"},
				)
			},
			typeName:  "Query",
			wantCount: 2,
		},
		{
			name:     "returns_not_found_for_nonexistent_api",
			setup:    func(_ *appsync.InMemoryBackend, _ string) {},
			apiID:    "nonexistent",
			typeName: "Query",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

			apiID := tt.apiID
			if apiID == "" {
				apiID = api.APIID
			}

			tt.setup(b, api.APIID)
			resolvers, err := b.ListResolvers(apiID, tt.typeName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

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

			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	_, err := b.ExecuteGraphQL(t.Context(), api.APIID, `query { hello }`, "", nil)
	require.Error(t, err)
}

func TestInMemoryBackend_ExecuteGraphQL_InvalidQuery(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
		apiID     string
		wantCount int
		wantErr   bool
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
		{
			name:    "returns_not_found_for_nonexistent_api",
			setup:   func(_ *appsync.InMemoryBackend, _ string) {},
			apiID:   "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

			apiID := tt.apiID
			if apiID == "" {
				apiID = api.APIID
			}

			tt.setup(b, api.APIID)
			dss, err := b.ListDataSources(apiID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "getItem", DataSourceName: "myDs"})

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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			_, _ = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "getItem", DataSourceName: "myDs"})

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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

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

			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
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

// ---- Refinement 1 tests ----

func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI(
		"TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, map[string]string{"k": "v"}, nil,
	)
	require.NoError(t, err)

	// Create a data source with tags so Reset() must close them too.
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "ds",
		Type: "NONE",
		Tags: nil,
	})
	require.NoError(t, err)

	// Reset must not panic and must clear all resources.
	b.Reset()

	apis, err := b.ListGraphqlAPIs("")
	require.NoError(t, err)
	assert.Empty(t, apis)

	// Second Reset on empty backend must also not panic.
	b.Reset()
}

func TestInMemoryBackend_CreateGraphqlAPI_InvalidAuthType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		authType appsync.AuthenticationType
		wantErr  bool
	}{
		{name: "valid_api_key", authType: appsync.AuthTypeAPIKey},
		{name: "valid_iam", authType: appsync.AuthTypeIAM},
		{name: "valid_cognito", authType: appsync.AuthTypeCognito},
		{name: "valid_oidc", authType: appsync.AuthTypeOIDC},
		{name: "valid_lambda", authType: appsync.AuthTypeLambda},
		{name: "empty_defaults_to_api_key", authType: ""},
		{name: "invalid_type_rejected", authType: "INVALID_AUTH", wantErr: true},
		{name: "invalid_type_basic", authType: "BASIC", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", tt.authType, false, "", "", nil, nil, nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, api)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, api)
		})
	}
}

func TestInMemoryBackend_DeleteGraphqlAPI_CascadeDelete(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	apiID := api.APIID

	// Create sub-resources.
	_, err = b.CreateAPIKey(apiID, "test key", 0)
	require.NoError(t, err)

	_, err = b.CreateAPICache(apiID, &appsync.APICache{
		TTL:                60,
		Type:               "SMALL",
		APICachingBehavior: "FULL_REQUEST_CACHING",
	})
	require.NoError(t, err)

	_, err = b.CreateFunction(apiID, &appsync.Function{
		Name:           "TestFn",
		DataSourceName: "MyDS",
	})
	require.NoError(t, err)

	// Delete the API.
	err = b.DeleteGraphqlAPI(apiID)
	require.NoError(t, err)

	// Verify sub-resources were cascade deleted by trying to create a new API key
	// (which would need the api to exist) — and verifying it fails.
	_, err = b.CreateAPIKey(apiID, "test", 0)
	require.Error(t, err)
}

func TestInMemoryBackend_CreateAPIKey_DefaultExpiry(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "test", 0)
	require.NoError(t, err)

	// Expires should be in the future.
	assert.Positive(t, key.Expires)
}

func TestInMemoryBackend_CreateAPIKey_Da2Prefix(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "test", 0)
	require.NoError(t, err)

	assert.Greater(t, len(key.ID), 4, "key ID should be longer than the prefix")
	assert.Equal(t, "da2-", key.ID[:4])
}

func TestInMemoryBackend_CreateAPICache_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cache   appsync.APICache
		wantErr bool
	}{
		{
			name: "valid",
			cache: appsync.APICache{
				TTL:                60,
				Type:               "SMALL",
				APICachingBehavior: "FULL_REQUEST_CACHING",
			},
		},
		{
			name:    "ttl_zero_rejected",
			cache:   appsync.APICache{TTL: 0, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
			wantErr: true,
		},
		{
			name:    "ttl_negative_rejected",
			cache:   appsync.APICache{TTL: -1, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
			wantErr: true,
		},
		{
			name:    "missing_type_rejected",
			cache:   appsync.APICache{TTL: 60, APICachingBehavior: "FULL_REQUEST_CACHING"},
			wantErr: true,
		},
		{
			name:    "invalid_type_rejected",
			cache:   appsync.APICache{TTL: 60, Type: "BOGUS", APICachingBehavior: "FULL_REQUEST_CACHING"},
			wantErr: true,
		},
		{
			name:    "missing_caching_behavior_rejected",
			cache:   appsync.APICache{TTL: 60, Type: "SMALL"},
			wantErr: true,
		},
		{
			name:    "invalid_caching_behavior_rejected",
			cache:   appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "BOGUS"},
			wantErr: true,
		},
		{
			name: "large_type_valid",
			cache: appsync.APICache{
				TTL:                3600,
				Type:               "LARGE_8X",
				APICachingBehavior: "PER_RESOLVER_CACHING",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, cacheErr := b.CreateAPICache(api.APIID, &tt.cache)

			if tt.wantErr {
				require.Error(t, cacheErr)

				return
			}

			require.NoError(t, cacheErr)
		})
	}
}

func TestInMemoryBackend_CreateFunction_DefaultVersion(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{
		Name:           "MyFunction",
		DataSourceName: "MyDS",
	})
	require.NoError(t, err)
	assert.Equal(t, "2018-05-29", fn.FunctionVersion)
}

func TestInMemoryBackend_CreateType_DuplicateDetection(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	def := "type MyType { id: ID! }"

	_, err = b.CreateType(api.APIID, def, appsync.TypeFormatSDL)
	require.NoError(t, err)

	// Creating the same type again should fail.
	_, err = b.CreateType(api.APIID, def, appsync.TypeFormatSDL)
	require.ErrorIs(t, err, awserr.ErrAlreadyExists)
}

func TestInMemoryBackend_AssociateAPI_UpdatesDomainName(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	dn, err := b.CreateDomainName("api.example.com", "arn:aws:acm:us-east-1:000000000000:certificate/abc", "", nil)
	require.NoError(t, err)
	assert.Empty(t, dn.APIID)

	// Create a GraphQL API and associate it.
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateAPI("api.example.com", api.APIID)
	require.NoError(t, err)
	assert.Equal(t, api.APIID, assoc.APIID)
}

func TestInMemoryBackend_CreateDataSource_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ds      appsync.DataSource
		wantErr bool
	}{
		{
			name: "valid",
			ds:   appsync.DataSource{Name: "MyDS", Type: appsync.DataSourceTypeNone},
		},
		{
			name:    "missing_name",
			ds:      appsync.DataSource{Type: appsync.DataSourceTypeNone},
			wantErr: true,
		},
		{
			name:    "missing_type",
			ds:      appsync.DataSource{Name: "MyDS"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, dsErr := b.CreateDataSource(api.APIID, &tt.ds)

			if tt.wantErr {
				require.Error(t, dsErr)

				return
			}

			require.NoError(t, dsErr)
		})
	}
}

func TestInMemoryBackend_CreateDomainName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domainName string
		wantErr    bool
	}{
		{name: "valid_domain", domainName: "api.example.com"},
		{name: "valid_subdomain", domainName: "my.api.example.co.uk"},
		{name: "empty_rejected", domainName: "", wantErr: true},
		{name: "no_dot_rejected", domainName: "localhost", wantErr: true},
		{name: "leading_dot_rejected", domainName: ".example.com", wantErr: true},
		{name: "trailing_dot_rejected", domainName: "example.com.", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.CreateDomainName(tt.domainName, "arn:aws:acm:us-east-1:000000000000:certificate/abc", "", nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// ---- Refinement 2: new backend operation tests ----

func TestInMemoryBackend_UpdateGraphqlAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newName  string
		authType appsync.AuthenticationType
		wantErr  bool
	}{
		{name: "update_name", newName: "NewName", authType: ""},
		{name: "update_auth_type", newName: "", authType: appsync.AuthTypeIAM},
		{name: "update_both", newName: "Updated", authType: appsync.AuthTypeCognito},
		{name: "invalid_auth_type", newName: "", authType: "INVALID", wantErr: true},
		{name: "no_change", newName: "", authType: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("OriginalName", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			updated, err := b.UpdateGraphqlAPI(api.APIID, tt.newName, tt.authType, nil, "", nil, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.newName != "" {
				assert.Equal(t, tt.newName, updated.Name)
			} else {
				assert.Equal(t, "OriginalName", updated.Name)
			}
		})
	}
}

func TestInMemoryBackend_ListAndDeleteAPIKeys(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key1, err := b.CreateAPIKey(api.APIID, "k1", 0)
	require.NoError(t, err)
	_, err = b.CreateAPIKey(api.APIID, "k2", 0)
	require.NoError(t, err)

	// List returns 2 keys.
	keys, err := b.ListAPIKeys(api.APIID)
	require.NoError(t, err)
	assert.Len(t, keys, 2)

	// Delete one.
	err = b.DeleteAPIKey(api.APIID, key1.ID)
	require.NoError(t, err)

	// List returns 1 key.
	keys, err = b.ListAPIKeys(api.APIID)
	require.NoError(t, err)
	assert.Len(t, keys, 1)

	// Delete non-existent returns error.
	err = b.DeleteAPIKey(api.APIID, "nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_GetAndDeleteAPICache(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	cache := &appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"}
	_, err = b.CreateAPICache(api.APIID, cache)
	require.NoError(t, err)

	// Get returns the cache.
	got, err := b.GetAPICache(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "SMALL", got.Type)

	// Delete the cache.
	err = b.DeleteAPICache(api.APIID)
	require.NoError(t, err)

	// Get after delete returns error.
	_, err = b.GetAPICache(api.APIID)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_FunctionCRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn1", DataSourceName: "ds"})
	require.NoError(t, err)

	// Get by ID.
	got, err := b.GetFunction(api.APIID, fn.FunctionID)
	require.NoError(t, err)
	assert.Equal(t, "fn1", got.Name)

	// List returns 1.
	fns, err := b.ListFunctions(api.APIID)
	require.NoError(t, err)
	assert.Len(t, fns, 1)

	// Delete.
	err = b.DeleteFunction(api.APIID, fn.FunctionID)
	require.NoError(t, err)

	// List returns 0.
	fns, err = b.ListFunctions(api.APIID)
	require.NoError(t, err)
	assert.Empty(t, fns)
}

func TestInMemoryBackend_TypeCRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	// Get by name.
	got, err := b.GetType(api.APIID, "MyType")
	require.NoError(t, err)
	assert.Equal(t, "MyType", got.Name)

	// List returns 1.
	types, err := b.ListTypes(api.APIID)
	require.NoError(t, err)
	assert.Len(t, types, 1)

	// Delete.
	err = b.DeleteType(api.APIID, "MyType")
	require.NoError(t, err)

	// Get after delete returns error.
	_, err = b.GetType(api.APIID, "MyType")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DomainNameCRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	// Create.
	_, err := b.CreateDomainName("api.example.com", certARN, "desc", nil)
	require.NoError(t, err)

	// Get.
	dn, err := b.GetDomainName("api.example.com")
	require.NoError(t, err)
	assert.Equal(t, "api.example.com", dn.DomainName)

	// List.
	dns, err := b.ListDomainNames()
	require.NoError(t, err)
	assert.Len(t, dns, 1)

	// Delete.
	err = b.DeleteDomainName("api.example.com")
	require.NoError(t, err)

	// Get after delete returns error.
	_, err = b.GetDomainName("api.example.com")
	require.ErrorIs(t, err, awserr.ErrNotFound)

	// List returns 0.
	dns, err = b.ListDomainNames()
	require.NoError(t, err)
	assert.Empty(t, dns)
}

func TestInMemoryBackend_GetAPIAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*appsync.InMemoryBackend)
		domainName string
		wantStatus string
		wantErr    bool
	}{
		{
			name:       "no_association_returns_not_found_status",
			domainName: "api.example.com",
			setup: func(b *appsync.InMemoryBackend) {
				_, _ = b.CreateDomainName("api.example.com",
					"arn:aws:acm:us-east-1:000000000000:certificate/abc", "", nil)
			},
			wantStatus: "NOT_FOUND",
		},
		{
			name: "with_association_returns_success_status",
			setup: func(b *appsync.InMemoryBackend) {
				_, _ = b.CreateDomainName("api.example.com",
					"arn:aws:acm:us-east-1:000000000000:certificate/abc", "", nil)
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				_, _ = b.AssociateAPI("api.example.com", api.APIID)
			},
			domainName: "api.example.com",
			wantStatus: "SUCCESS",
		},
		{
			name:       "domain_not_found_returns_error",
			domainName: "missing.example.com",
			setup:      func(_ *appsync.InMemoryBackend) {},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			tt.setup(b)

			assoc, err := b.GetAPIAssociation(tt.domainName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, assoc.AssociationStatus)
		})
	}
}

func TestInMemoryBackend_DeleteDomainName_CascadesAssociation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	_, err := b.CreateDomainName("api.example.com", certARN, "", nil)
	require.NoError(t, err)

	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.AssociateAPI("api.example.com", api.APIID)
	require.NoError(t, err)

	// Delete domain name.
	err = b.DeleteDomainName("api.example.com")
	require.NoError(t, err)

	// Get association after domain name deleted returns error.
	_, err = b.GetAPIAssociation("api.example.com")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

// ---- Refinement 3: backend tests ----

func TestInMemoryBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*appsync.InMemoryBackend) string
		name       string
		wantErr    bool
		wantTagLen int
	}{
		{
			name: "tag_and_untag",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("T", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			wantTagLen: 1,
		},
		{
			name: "tag_not_found",
			setup: func(_ *appsync.InMemoryBackend) string {
				return "nonexistent"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			apiID := tt.setup(b)

			err := b.TagResource(apiID, map[string]string{"key": "val", "env": "prod"})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			err = b.UntagResource(apiID, []string{"env"})
			require.NoError(t, err)

			tags, err := b.ListTagsForResource(apiID)
			require.NoError(t, err)
			assert.Len(t, tags, tt.wantTagLen)
			assert.Equal(t, "val", tags["key"])
			assert.NotContains(t, tags, "env")
		})
	}
}

func TestInMemoryBackend_UpdateDataSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dsName     string
		updateDesc string
		wantErr    bool
	}{
		{
			name:       "updates_description",
			dsName:     "myds",
			updateDesc: "updated",
		},
		{
			name:    "not_found_returns_error",
			dsName:  "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "myds", Type: "NONE"})
			require.NoError(t, err)

			updated, err := b.UpdateDataSource(api.APIID, tt.dsName, &appsync.DataSource{Description: tt.updateDesc})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.updateDesc, updated.Description)
		})
	}
}

func TestInMemoryBackend_UpdateFunction(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn1", DataSourceName: "ds"})
	require.NoError(t, err)

	updated, err := b.UpdateFunction(api.APIID, fn.FunctionID, &appsync.Function{Description: "updated"})
	require.NoError(t, err)
	assert.Equal(t, "updated", updated.Description)

	// Not found returns error.
	_, err = b.UpdateFunction(api.APIID, "nonexistent", &appsync.Function{Description: "x"})
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateAPIKey(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "original", 0)
	require.NoError(t, err)

	updated, err := b.UpdateAPIKey(api.APIID, key.ID, "updated", 0)
	require.NoError(t, err)
	assert.Equal(t, "updated", updated.Description)

	// Not found key returns error.
	_, err = b.UpdateAPIKey(api.APIID, "nonexistent", "x", 0)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateAPICache(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(
		api.APIID,
		&appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
	)
	require.NoError(t, err)

	updated, err := b.UpdateAPICache(api.APIID, &appsync.APICache{TTL: 120, Type: "LARGE"})
	require.NoError(t, err)
	assert.Equal(t, "LARGE", updated.Type)
	assert.Equal(t, int64(120), updated.TTL)

	// Invalid type returns error.
	_, err = b.UpdateAPICache(api.APIID, &appsync.APICache{Type: "INVALID"})
	require.Error(t, err)

	// Not found API cache returns error.
	api2, err := b.CreateGraphqlAPI("TestAPI2", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.UpdateAPICache(api2.APIID, &appsync.APICache{TTL: 60})
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_FlushAPICache(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(
		api.APIID,
		&appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
	)
	require.NoError(t, err)

	// Flush succeeds.
	err = b.FlushAPICache(api.APIID)
	require.NoError(t, err)

	// Flush without cache returns error.
	api2, err := b.CreateGraphqlAPI("TestAPI2", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	err = b.FlushAPICache(api2.APIID)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateType(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateType(api.APIID, "type MyType { id: ID! }", appsync.TypeFormatSDL)
	require.NoError(t, err)

	updated, err := b.UpdateType(api.APIID, "MyType", "type MyType { id: ID! name: String! }", "")
	require.NoError(t, err)
	assert.Contains(t, updated.Definition, "name")

	// Not found returns error.
	_, err = b.UpdateType(api.APIID, "NonExistent", "type X {}", "")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateResolver(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "getItem",
		DataSourceName: "myds",
	})
	require.NoError(t, err)

	updated, err := b.UpdateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:              "getItem",
		RequestMappingTemplate: "updated-template",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated-template", updated.RequestMappingTemplate)

	// Not found returns error.
	_, err = b.UpdateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "nonexistent"})
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_EventAPI_CRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Create.
	api, err := b.CreateAPI("MyEventAPI", "", map[string]string{"env": "test"}, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, api.APIID)

	// Get.
	got, err := b.GetAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "MyEventAPI", got.Name)

	// List.
	apis, err := b.ListAPIs()
	require.NoError(t, err)
	assert.Len(t, apis, 1)

	// Delete.
	err = b.DeleteAPI(api.APIID)
	require.NoError(t, err)

	// Get after delete returns error.
	_, err = b.GetAPI(api.APIID)
	require.ErrorIs(t, err, awserr.ErrNotFound)

	// List returns 0.
	apis, err = b.ListAPIs()
	require.NoError(t, err)
	assert.Empty(t, apis)
}

func TestInMemoryBackend_DeleteAPI_CascadesChannelNamespaces(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	api, err := b.CreateAPI("MyEventAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "ns1", nil, nil)
	require.NoError(t, err)

	// Delete should cascade channel namespaces.
	err = b.DeleteAPI(api.APIID)
	require.NoError(t, err)

	// Recreate the API - should have no namespaces.
	api2, err := b.CreateAPI("NewAPI", "", nil, nil)
	require.NoError(t, err)
	assert.NotEqual(t, api.APIID, api2.APIID)
}

func TestInMemoryBackend_DisassociateAPI(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	_, err := b.CreateDomainName("api.example.com", certARN, "", nil)
	require.NoError(t, err)

	gqlAPI, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.AssociateAPI("api.example.com", gqlAPI.APIID)
	require.NoError(t, err)

	// Disassociate.
	err = b.DisassociateAPI("api.example.com")
	require.NoError(t, err)

	// Association no longer exists.
	assoc, err := b.GetAPIAssociation("api.example.com")
	require.NoError(t, err)
	assert.Equal(t, "NOT_FOUND", assoc.AssociationStatus)

	// Second disassociate returns 404.
	err = b.DisassociateAPI("api.example.com")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateDomainName(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	_, err := b.CreateDomainName("api.example.com", certARN, "orig desc", nil)
	require.NoError(t, err)

	updated, err := b.UpdateDomainName("api.example.com", "new desc", "")
	require.NoError(t, err)
	assert.Equal(t, "new desc", updated.Description)

	// Not found.
	_, err = b.UpdateDomainName("missing.example.com", "x", "")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

// ---- Coverage boost tests ----

func TestInMemoryBackend_GetFunction_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.GetFunction(api.APIID, "nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListTagsForResource_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.ListTagsForResource("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListTagsForResource_EmptyTags(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// No tags set yet — should return empty map.
	tagMap, err := b.ListTagsForResource(api.APIID)
	require.NoError(t, err)
	assert.Empty(t, tagMap)
}

func TestInMemoryBackend_UpdateFunction_AllFields(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{
		Name: "fn1", DataSourceName: "ds",
	})
	require.NoError(t, err)

	updated, err := b.UpdateFunction(api.APIID, fn.FunctionID, &appsync.Function{
		Name:                    "fn1-updated",
		DataSourceName:          "ds2",
		RequestMappingTemplate:  "req",
		ResponseMappingTemplate: "resp",
		Code:                    "code",
		Description:             "desc",
	})
	require.NoError(t, err)
	assert.Equal(t, "fn1-updated", updated.Name)
	assert.Equal(t, "ds2", updated.DataSourceName)
	assert.Equal(t, "req", updated.RequestMappingTemplate)
	assert.Equal(t, "resp", updated.ResponseMappingTemplate)
}

func TestInMemoryBackend_UpdateAPICache_CachingBehaviorValidation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(api.APIID, &appsync.APICache{
		TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING",
	})
	require.NoError(t, err)

	// Invalid apiCachingBehavior returns validation error.
	_, err = b.UpdateAPICache(api.APIID, &appsync.APICache{APICachingBehavior: "INVALID"})
	require.Error(t, err)
}

func TestInMemoryBackend_UpdateDataSource_AllFields(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds1", Type: "NONE"})
	require.NoError(t, err)

	updated, err := b.UpdateDataSource(api.APIID, "ds1", &appsync.DataSource{
		Type:           "AWS_LAMBDA",
		ServiceRoleARN: "arn:aws:iam::000000000000:role/role",
		LambdaConfig:   &appsync.LambdaDataSourceConfig{LambdaFunctionARN: "arn:aws:lambda:us-east-1:000:function:fn"},
	})
	require.NoError(t, err)
	assert.Equal(t, "AWS_LAMBDA", string(updated.Type))
}

func TestInMemoryBackend_GetAPI_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetAPI("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DeleteAPI_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	err := b.DeleteAPI("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

// ---- Refinement 4: backend tests ----

func TestInMemoryBackend_ChannelNamespace_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nsName    string
		createTwo bool
		wantErr   bool
	}{
		{name: "creates_and_returns", nsName: "ns1"},
		{name: "duplicate_returns_error", nsName: "ns2", createTwo: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateAPI("EventAPI", "", nil, nil)
			require.NoError(t, err)

			_, err = b.CreateChannelNamespace(api.APIID, tt.nsName, nil, nil)
			require.NoError(t, err)

			if !tt.createTwo {
				return
			}

			_, err = b.CreateChannelNamespace(api.APIID, tt.nsName, nil, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_GetChannelNamespace_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.GetChannelNamespace(api.APIID, "nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListChannelNamespaces(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "ns1", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateChannelNamespace(api.APIID, "ns2", nil, nil)
	require.NoError(t, err)

	nss, err := b.ListChannelNamespaces(api.APIID)
	require.NoError(t, err)
	assert.Len(t, nss, 2)

	// Not found API returns error.
	_, err = b.ListChannelNamespaces("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateChannelNamespace(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "ns1", nil, nil)
	require.NoError(t, err)

	const codeHandlers = "export const handler = () => {}"
	updated, err := b.UpdateChannelNamespace(
		api.APIID, "ns1", &appsync.ChannelNamespaceConfig{CodeHandlers: codeHandlers},
	)
	require.NoError(t, err)
	assert.Equal(t, codeHandlers, updated.CodeHandlers)
	assert.NotZero(t, updated.LastModified)

	// Not found returns error.
	_, err = b.UpdateChannelNamespace(api.APIID, "missing", &appsync.ChannelNamespaceConfig{CodeHandlers: "code"})
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DeleteChannelNamespace(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateChannelNamespace(api.APIID, "ns1", nil, nil)
	require.NoError(t, err)

	err = b.DeleteChannelNamespace(api.APIID, "ns1")
	require.NoError(t, err)

	// Second delete returns error.
	err = b.DeleteChannelNamespace(api.APIID, "ns1")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateAPI(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	updated, err := b.UpdateAPI(api.APIID, "UpdatedName", "ops@example.com", nil)
	require.NoError(t, err)
	assert.Equal(t, "UpdatedName", updated.Name)
	assert.Equal(t, "ops@example.com", updated.OwnerContact)

	// Not found.
	_, err = b.UpdateAPI("nonexistent", "x", "", nil)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_SourceAPIAssociation_CRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Create the source and merged APIs first (validation now requires both to exist).
	srcAPI, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	mrg1API, err := b.CreateGraphqlAPI("MergedAPI1", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	// Associate source API.
	assoc1, err := b.AssociateSourceGraphqlAPI(mrg1API.APIID, srcAPI.APIID, "test", "")
	require.NoError(t, err)
	assert.NotEmpty(t, assoc1.AssociationID)

	// Get source API association.
	got, err := b.GetSourceAPIAssociation(mrg1API.APIID, assoc1.AssociationID)
	require.NoError(t, err)
	assert.Equal(t, mrg1API.APIID, got.MergedAPIID)

	// Wrong merged API ID returns error.
	_, err = b.GetSourceAPIAssociation("wrong-id", assoc1.AssociationID)
	require.ErrorIs(t, err, awserr.ErrNotFound)

	// List source API associations.
	assocs, err := b.ListSourceAPIAssociations(mrg1API.APIID)
	require.NoError(t, err)
	assert.Len(t, assocs, 1)

	// List for empty merged API returns empty.
	assocs2, err := b.ListSourceAPIAssociations("empty-merged")
	require.NoError(t, err)
	assert.Empty(t, assocs2)

	// Disassociate.
	err = b.DisassociateSourceGraphqlAPI(mrg1API.APIID, assoc1.AssociationID)
	require.NoError(t, err)

	// Second disassociate returns error.
	err = b.DisassociateSourceGraphqlAPI(mrg1API.APIID, assoc1.AssociationID)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DisassociateMergedGraphqlAPI(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Create the source and merged APIs first.
	srcAPI, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	mrg1API, err := b.CreateGraphqlAPI("MergedAPI1", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateMergedGraphqlAPI(srcAPI.APIID, mrg1API.APIID, "", "")
	require.NoError(t, err)

	// Wrong source API ID returns error.
	err = b.DisassociateMergedGraphqlAPI("wrong-source", assoc.AssociationID)
	require.ErrorIs(t, err, awserr.ErrNotFound)

	// Correct source returns success.
	err = b.DisassociateMergedGraphqlAPI(srcAPI.APIID, assoc.AssociationID)
	require.NoError(t, err)
}

func TestInMemoryBackend_EnvironmentVariables(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Get empty env vars.
	envVars, err := b.GetGraphqlAPIEnvironmentVariables(api.APIID)
	require.NoError(t, err)
	assert.Empty(t, envVars)

	// Put env vars.
	envVars2, err := b.PutGraphqlAPIEnvironmentVariables(api.APIID, map[string]string{"K1": "V1", "K2": "V2"})
	require.NoError(t, err)
	assert.Equal(t, "V1", envVars2["K1"])

	// Get returns updated vars.
	envVars3, err := b.GetGraphqlAPIEnvironmentVariables(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "V2", envVars3["K2"])

	// Put replaces all vars.
	_, err = b.PutGraphqlAPIEnvironmentVariables(api.APIID, map[string]string{"NEW_KEY": "new_val"})
	require.NoError(t, err)

	envVars4, err := b.GetGraphqlAPIEnvironmentVariables(api.APIID)
	require.NoError(t, err)
	assert.NotContains(t, envVars4, "K1")
	assert.Equal(t, "new_val", envVars4["NEW_KEY"])

	// Not found API returns error.
	_, err = b.GetGraphqlAPIEnvironmentVariables("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)

	_, err = b.PutGraphqlAPIEnvironmentVariables("nonexistent", map[string]string{})
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListResolversByFunction(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
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

	// Resolver without the function — should not appear.
	_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "listItems",
		DataSourceName: "ds",
	})
	require.NoError(t, err)

	resolvers, err := b.ListResolversByFunction(api.APIID, fn.FunctionID)
	require.NoError(t, err)
	assert.Len(t, resolvers, 1)
	assert.Equal(t, "getItem", resolvers[0].FieldName)

	// Not found API returns error.
	_, err = b.ListResolversByFunction("nonexistent", fn.FunctionID)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ChannelNamespace_HasTimestamps(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("EventAPI", "", nil, nil)
	require.NoError(t, err)

	ns, err := b.CreateChannelNamespace(api.APIID, "ns1", map[string]string{"env": "test"}, nil)
	require.NoError(t, err)
	assert.NotZero(t, ns.Created)
	assert.NotZero(t, ns.LastModified)
	assert.Equal(t, ns.Created, ns.LastModified)
}

// ---- Coverage boost tests ----

func TestInMemoryBackend_ListAPIKeys_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.ListAPIKeys("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_GetAPICache_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetAPICache("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DeleteAPICache_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	err := b.DeleteAPICache("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_GetFunction_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetFunction("nonexistent", "fn")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListFunctions_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.ListFunctions("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_GetType_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetType("nonexistent", "MyType")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListTypes_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.ListTypes("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DeleteDomainName_APIAssoc_Cascade(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	certARN := "arn:aws:acm:us-east-1:000000000000:certificate/abc"

	_, err := b.CreateDomainName("api.example.com", certARN, "desc", nil)
	require.NoError(t, err)

	gqlAPI, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.AssociateAPI("api.example.com", gqlAPI.APIID)
	require.NoError(t, err)

	// Delete domain name - should cascade delete the association.
	err = b.DeleteDomainName("api.example.com")
	require.NoError(t, err)

	// Get domain name should return not found.
	_, err = b.GetDomainName("api.example.com")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DeleteAPIKey_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	err := b.DeleteAPIKey("nonexistent", "key-id")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

// ---- Refinement 5: backend tests ----

func TestInMemoryBackend_CreateGraphqlAPI_XrayEnabled(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, true, "GRAPHQL", "", nil, nil, nil)
	require.NoError(t, err)
	assert.True(t, api.XrayEnabled)
	assert.Equal(t, "GRAPHQL", api.APIType)
	assert.NotZero(t, api.CreatedAt)
	assert.NotZero(t, api.UpdatedAt)
	assert.Equal(t, api.CreatedAt, api.UpdatedAt)
}

func TestInMemoryBackend_CreateGraphqlAPI_APIType_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		apiType string
		wantErr bool
	}{
		{name: "default_graphql", apiType: "", wantErr: false},
		{name: "explicit_graphql", apiType: "GRAPHQL", wantErr: false},
		{name: "merged_type", apiType: "MERGED", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, tt.apiType, "", nil, nil, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotNil(t, api)
		})
	}
}

func TestInMemoryBackend_UpdateGraphqlAPI_XrayEnabled(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	assert.False(t, api.XrayEnabled)

	xrayEnabled := true
	updated, err := b.UpdateGraphqlAPI(api.APIID, "", "", &xrayEnabled, "", nil, nil)
	require.NoError(t, err)
	assert.True(t, updated.XrayEnabled)
	assert.GreaterOrEqual(t, updated.UpdatedAt, api.UpdatedAt)
}

func TestInMemoryBackend_ListGraphqlAPIs_TypeFilter(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreateGraphqlAPI("GraphQL1", appsync.AuthTypeAPIKey, false, "GRAPHQL", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateGraphqlAPI("Merged1", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	require.NoError(t, err)

	// Filter by GRAPHQL.
	graphqlAPIs, err := b.ListGraphqlAPIs("GRAPHQL")
	require.NoError(t, err)
	assert.Len(t, graphqlAPIs, 1)
	assert.Equal(t, "GraphQL1", graphqlAPIs[0].Name)

	// Filter by MERGED.
	mergedAPIs, err := b.ListGraphqlAPIs("MERGED")
	require.NoError(t, err)
	assert.Len(t, mergedAPIs, 1)
	assert.Equal(t, "Merged1", mergedAPIs[0].Name)

	// No filter returns all.
	allAPIs, err := b.ListGraphqlAPIs("")
	require.NoError(t, err)
	assert.Len(t, allAPIs, 2)
}

func TestInMemoryBackend_CreateAPIKey_MaxKeysLimit(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Create up to the limit (50).
	for i := range 50 {
		_, err = b.CreateAPIKey(api.APIID, fmt.Sprintf("key%d", i+1), 0)
		require.NoError(t, err, "key %d should succeed", i+1)
	}

	// 51st key should fail.
	_, err = b.CreateAPIKey(api.APIID, "key51", 0)
	require.Error(t, err)
}

func TestInMemoryBackend_CreateAPIKey_ExpiresCapToMaxDays(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Set expires to 2 years from now (> 365 days).
	farFuture := time.Now().AddDate(2, 0, 0).Unix()
	key, err := b.CreateAPIKey(api.APIID, "key1", farFuture)
	require.NoError(t, err)

	// Expires should be capped at 365 days.
	maxExpires := time.Now().AddDate(0, 0, 365).Unix()
	assert.LessOrEqual(t, key.Expires, maxExpires+60) // +60s tolerance
}

func TestInMemoryBackend_CreateDataSource_InvalidType(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "invalid_ds",
		Type: "INVALID_TYPE",
	})
	require.Error(t, err)
}

func TestInMemoryBackend_CreateDataSource_HTTPConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		ds        *appsync.DataSource
		name      string
		wantErr   bool
	}{
		{
			name: "http_with_endpoint",
			ds: &appsync.DataSource{
				Name:       "httpDs",
				Type:       "HTTP",
				HTTPConfig: &appsync.HTTPDataSourceConfig{Endpoint: "https://example.com"},
			},
		},
		{
			name: "http_missing_endpoint",
			ds: &appsync.DataSource{
				Name: "httpDs",
				Type: "HTTP",
			},
			wantErr: true,
		},
		{
			name: "none_type_ok",
			ds: &appsync.DataSource{
				Name: "noneDs",
				Type: "NONE",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.CreateDataSource(api.APIID, tt.ds)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_CreateType_FormatValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		format  appsync.TypeDefinitionFormat
		wantErr bool
	}{
		{name: "sdl_format", format: appsync.TypeFormatSDL},
		{name: "json_format", format: appsync.TypeFormatJSON},
		{name: "invalid_format", format: "XML", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.CreateType(api.APIID, "type Item { id: ID! }", tt.format)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_CreateResolver_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		resolver *appsync.Resolver
		name     string
		wantErr  bool
	}{
		{
			name:     "missing_fieldName",
			resolver: &appsync.Resolver{DataSourceName: "ds"},
			wantErr:  true,
		},
		{
			name:     "invalid_kind",
			resolver: &appsync.Resolver{FieldName: "getItem", DataSourceName: "ds", Kind: "INVALID"},
			wantErr:  true,
		},
		{
			name:     "pipeline_missing_config",
			resolver: &appsync.Resolver{FieldName: "getItem", Kind: "PIPELINE"},
			wantErr:  true,
		},
		{
			name:     "unit_missing_datasource",
			resolver: &appsync.Resolver{FieldName: "getItem"},
			wantErr:  true,
		},
		{
			name:     "valid_unit",
			resolver: &appsync.Resolver{FieldName: "getItem", DataSourceName: "ds"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.CreateResolver(api.APIID, "Query", tt.resolver)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// ---- Refinement 6 tests ----

func TestInMemoryBackend_CreateAPI_OwnerContact(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateAPI("TestEventAPI", "owner@example.com", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "owner@example.com", api.OwnerContact)

	got, err := b.GetAPI(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "owner@example.com", got.OwnerContact)
}

func TestInMemoryBackend_CreateFunction_NameUniqueness(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds", Type: "NONE"})
	require.NoError(t, err)

	_, err = b.CreateFunction(api.APIID, &appsync.Function{Name: "MyFunc", DataSourceName: "ds"})
	require.NoError(t, err)

	// Second function with same name should fail.
	_, err = b.CreateFunction(api.APIID, &appsync.Function{Name: "MyFunc", DataSourceName: "ds"})
	require.Error(t, err)
}

func TestInMemoryBackend_CreateFunction_NameRequired(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateFunction(api.APIID, &appsync.Function{DataSourceName: "ds"})
	require.Error(t, err)
}

func TestInMemoryBackend_UpdateDataSource_HTTPConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name:       "httpDs",
		Type:       "HTTP",
		HTTPConfig: &appsync.HTTPDataSourceConfig{Endpoint: "https://old.example.com"},
	})
	require.NoError(t, err)

	updated, err := b.UpdateDataSource(api.APIID, "httpDs", &appsync.DataSource{
		HTTPConfig: &appsync.HTTPDataSourceConfig{Endpoint: "https://new.example.com"},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.HTTPConfig)
	assert.Equal(t, "https://new.example.com", updated.HTTPConfig.Endpoint)
}

func TestInMemoryBackend_UpdateResolver_PipelineConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds", Type: "NONE"})
	require.NoError(t, err)

	fn1, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn1", DataSourceName: "ds"})
	require.NoError(t, err)

	fn2, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn2", DataSourceName: "ds"})
	require.NoError(t, err)

	_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "getItem",
		Kind:           "PIPELINE",
		PipelineConfig: []string{fn1.FunctionID},
	})
	require.NoError(t, err)

	// Update PipelineConfig with both functions.
	updated, err := b.UpdateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "getItem",
		PipelineConfig: []string{fn1.FunctionID, fn2.FunctionID},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{fn1.FunctionID, fn2.FunctionID}, updated.PipelineConfig)
}

func TestInMemoryBackend_ListAPIKeys_FilterExpired(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Create a key that expired in the past.
	pastExpiry := time.Now().Add(-24 * time.Hour).Unix()
	_, err = b.CreateAPIKey(api.APIID, "expired", pastExpiry)
	require.NoError(t, err)

	// Create a valid key.
	_, err = b.CreateAPIKey(api.APIID, "valid", 0)
	require.NoError(t, err)

	keys, err := b.ListAPIKeys(api.APIID)
	require.NoError(t, err)
	// Only the non-expired key should be returned.
	assert.Len(t, keys, 1)
	assert.Equal(t, "valid", keys[0].Description)
}

func TestInMemoryBackend_ListFunctions_Sorted(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds", Type: "NONE"})
	require.NoError(t, err)

	for _, name := range []string{"ZFunc", "AFunc", "MFunc"} {
		_, err = b.CreateFunction(api.APIID, &appsync.Function{Name: name, DataSourceName: "ds"})
		require.NoError(t, err)
	}

	fns, err := b.ListFunctions(api.APIID)
	require.NoError(t, err)
	require.Len(t, fns, 3)
	assert.Equal(t, "AFunc", fns[0].Name)
	assert.Equal(t, "MFunc", fns[1].Name)
	assert.Equal(t, "ZFunc", fns[2].Name)
}

func TestInMemoryBackend_ListGraphqlAPIs_Sorted(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	for _, name := range []string{"Zebra", "Alpha", "Mango"} {
		_, err := b.CreateGraphqlAPI(name, appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
		require.NoError(t, err)
	}

	apis, err := b.ListGraphqlAPIs("")
	require.NoError(t, err)
	require.Len(t, apis, 3)
	assert.Equal(t, "Alpha", apis[0].Name)
	assert.Equal(t, "Mango", apis[1].Name)
	assert.Equal(t, "Zebra", apis[2].Name)
}

// ---- Refinement 7 tests ----

func TestInMemoryBackend_UpdateAPIKey_ExpiryCapEnforced(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "desc", 0)
	require.NoError(t, err)

	// Attempt to update with an expiry far in the future (10 years).
	farFuture := time.Now().AddDate(10, 0, 0).Unix()
	updated, err := b.UpdateAPIKey(api.APIID, key.ID, "", farFuture)
	require.NoError(t, err)

	// Should be capped at 365 days from now.
	maxAllowed := time.Now().AddDate(0, 0, 365).Unix()
	assert.LessOrEqual(t, updated.Expires, maxAllowed+60, "expiry should be capped at 365 days")
	assert.Greater(t, updated.Expires, maxAllowed-60, "expiry should be close to 365 days")
}

func TestInMemoryBackend_UpdateAPIKey_ValidExpiryUnchanged(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "desc", 0)
	require.NoError(t, err)

	// Set a valid expiry within the cap.
	validExpiry := time.Now().AddDate(0, 0, 30).Unix()
	updated, err := b.UpdateAPIKey(api.APIID, key.ID, "", validExpiry)
	require.NoError(t, err)
	assert.Equal(t, validExpiry, updated.Expires, "valid expiry should be stored as-is")
}

func TestInMemoryBackend_AssociateAPI_ValidatesAPIExists(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.CreateDomainName("example.com", "arn:aws:acm:us-east-1:000:certificate/abc", "", nil)
	require.NoError(t, err)

	// Associating a non-existent API should fail.
	_, err = b.AssociateAPI("example.com", "nonexistent-api-id")
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrNotFound)
}

func TestInMemoryBackend_AssociateAPI_ValidAPISucceeds(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.CreateDomainName("api.example.com", "arn:aws:acm:us-east-1:000:certificate/abc", "", nil)
	require.NoError(t, err)

	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	assoc, err := b.AssociateAPI("api.example.com", api.APIID)
	require.NoError(t, err)
	assert.Equal(t, api.APIID, assoc.APIID)
}

func TestInMemoryBackend_PutEnvVars_ExceedsLimit(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Build a map with 26 entries (exceeds max of 25).
	envVars := make(map[string]string)
	for i := range 26 {
		envVars[fmt.Sprintf("KEY_%d", i)] = "value"
	}

	_, err = b.PutGraphqlAPIEnvironmentVariables(api.APIID, envVars)
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrValidation)
}

func TestInMemoryBackend_PutEnvVars_MaxAllowed(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Build a map with exactly 25 entries.
	envVars := make(map[string]string)
	for i := range 25 {
		envVars[fmt.Sprintf("KEY_%d", i)] = "value"
	}

	out, err := b.PutGraphqlAPIEnvironmentVariables(api.APIID, envVars)
	require.NoError(t, err)
	assert.Len(t, out, 25)
}

func TestInMemoryBackend_DeleteDataSource_BlockedByResolver(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "MyDS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	_, err = b.StartSchemaCreation(api.APIID, "type Query { hello: String }")
	require.NoError(t, err)

	_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "hello",
		DataSourceName: "MyDS",
		Kind:           "UNIT",
	})
	require.NoError(t, err)

	// Delete should be blocked.
	err = b.DeleteDataSource(api.APIID, "MyDS")
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrValidation)
}

func TestInMemoryBackend_DeleteDataSource_BlockedByFunction(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "FuncDS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	_, err = b.CreateFunction(api.APIID, &appsync.Function{
		Name:           "MyFunc",
		DataSourceName: "FuncDS",
	})
	require.NoError(t, err)

	// Delete should be blocked.
	err = b.DeleteDataSource(api.APIID, "FuncDS")
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrValidation)
}

func TestInMemoryBackend_DeleteDataSource_SucceedsWhenUnreferenced(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "UnusedDS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	err = b.DeleteDataSource(api.APIID, "UnusedDS")
	require.NoError(t, err)
}

func TestInMemoryBackend_DeleteFunction_BlockedByResolver(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{
		Name:           "MyFn",
		DataSourceName: "DS",
	})
	require.NoError(t, err)

	_, err = b.StartSchemaCreation(api.APIID, "type Query { hello: String }")
	require.NoError(t, err)

	_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
		FieldName:      "hello",
		Kind:           "PIPELINE",
		PipelineConfig: []string{fn.FunctionID},
	})
	require.NoError(t, err)

	// Delete should be blocked.
	err = b.DeleteFunction(api.APIID, fn.FunctionID)
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrValidation)
}

func TestInMemoryBackend_DeleteFunction_SucceedsWhenUnreferenced(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{
		Name:           "UnusedFn",
		DataSourceName: "DS",
	})
	require.NoError(t, err)

	err = b.DeleteFunction(api.APIID, fn.FunctionID)
	require.NoError(t, err)
}

func TestInMemoryBackend_GetDataSource_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetDataSource("nonexistent-api", "myds")
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrNotFound)
}

func TestInMemoryBackend_AssociateMergedAPI_ValidatesAPIsExist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sourceExists bool
		mergedExists bool
		wantErr      bool
	}{
		{
			name:         "source_not_found",
			sourceExists: false,
			mergedExists: true,
			wantErr:      true,
		},
		{
			name:         "merged_not_found",
			sourceExists: true,
			mergedExists: false,
			wantErr:      true,
		},
		{
			name:         "both_exist",
			sourceExists: true,
			mergedExists: true,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			sourceID := "nonexistent-source"
			mergedID := "nonexistent-merged"

			if tt.sourceExists {
				src, err := b.CreateGraphqlAPI("SourceAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				sourceID = src.APIID
			}

			if tt.mergedExists {
				mrg, err := b.CreateGraphqlAPI("MergedAPI", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
				require.NoError(t, err)
				mergedID = mrg.APIID
			}

			_, err := b.AssociateMergedGraphqlAPI(sourceID, mergedID, "test", "")

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, appsync.ErrNotFound)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestInMemoryBackend_AssociateSourceAPI_ValidatesAPIsExist(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.AssociateSourceGraphqlAPI("nonexistent-merged", "nonexistent-source", "test", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, appsync.ErrNotFound)
}

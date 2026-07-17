package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestGetResolver_NonExistentAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		apiID     string
		typeName  string
		fieldName string
	}{
		{
			name:      "unknown_api_returns_api_not_found",
			apiID:     "no-such-api",
			typeName:  "Query",
			fieldName: "getUser",
		},
		{
			name:      "empty_backend_returns_api_not_found",
			apiID:     "ghost-api-id",
			typeName:  "Mutation",
			fieldName: "createUser",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.GetResolver(tt.apiID, tt.typeName, tt.fieldName)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			assert.Contains(t, err.Error(), tt.apiID)
		})
	}
}

func TestDeleteResolver_NonExistentAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		apiID     string
		typeName  string
		fieldName string
	}{
		{
			name:      "unknown_api_returns_api_not_found",
			apiID:     "no-such-api",
			typeName:  "Query",
			fieldName: "getUser",
		},
		{
			name:      "resolver_not_reachable_via_bad_api",
			apiID:     "ghost-api",
			typeName:  "Mutation",
			fieldName: "deleteUser",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			err := b.DeleteResolver(tt.apiID, tt.typeName, tt.fieldName)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			assert.Contains(t, err.Error(), tt.apiID)
		})
	}
}

func TestGetResolver_ExistingAPI_ReturnsResolverNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		typeName  string
		fieldName string
	}{
		{
			name:      "resolver_absent_on_valid_api",
			typeName:  "Query",
			fieldName: "missingField",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.GetResolver(api.APIID, tt.typeName, tt.fieldName)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			// Error must mention the resolver, not "api not found".
			assert.NotContains(t, err.Error(), "api "+api.APIID)
		})
	}
}

func TestCreateResolver_UnitResolver_DataSourcePreserved(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "MyDS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	r := &appsync.Resolver{
		FieldName:               "getItem",
		DataSourceName:          "MyDS",
		Kind:                    "UNIT",
		RequestMappingTemplate:  "{\"version\":\"2018-05-29\"}",
		ResponseMappingTemplate: "$util.toJson($context.result)",
		MaxBatchSize:            5,
		CachingConfig: &appsync.CachingConfig{
			TTL: 30,
		},
	}

	created, err := b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)
	assert.Equal(t, "UNIT", created.Kind)
	assert.Equal(t, "MyDS", created.DataSourceName)
	assert.Equal(t, int32(5), created.MaxBatchSize)
	require.NotNil(t, created.CachingConfig)
	assert.Equal(t, int64(30), created.CachingConfig.TTL)
}

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

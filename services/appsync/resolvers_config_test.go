package appsync_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestCreateResolver_CachingConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg     *appsync.CachingConfig
		name    string
		wantNil bool
	}{
		{
			name: "with_caching_config",
			cfg: &appsync.CachingConfig{
				TTL:         60,
				CachingKeys: []string{"$context.identity.sub", "$context.arguments.id"},
			},
		},
		{
			name: "caching_no_keys",
			cfg: &appsync.CachingConfig{
				TTL: 300,
			},
		},
		{
			name:    "no_caching",
			cfg:     nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "DS",
				Type: appsync.DataSourceTypeNone,
			})
			require.NoError(t, err)

			r := &appsync.Resolver{
				FieldName:      "getItem",
				DataSourceName: "DS",
				CachingConfig:  tt.cfg,
			}

			created, err := b.CreateResolver(api.APIID, "Query", r)
			require.NoError(t, err)

			if tt.wantNil {
				assert.Nil(t, created.CachingConfig)
			} else {
				require.NotNil(t, created.CachingConfig)
				assert.Equal(t, tt.cfg.TTL, created.CachingConfig.TTL)
				assert.Equal(t, tt.cfg.CachingKeys, created.CachingConfig.CachingKeys)
			}
		})
	}
}

func TestCreateResolver_CachingConfig_GetRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	cfg := &appsync.CachingConfig{
		TTL:         120,
		CachingKeys: []string{"$context.arguments.id"},
	}

	r := &appsync.Resolver{
		FieldName:      "getUser",
		DataSourceName: "DS",
		CachingConfig:  cfg,
	}

	_, err = b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)

	got, err := b.GetResolver(api.APIID, "Query", "getUser")
	require.NoError(t, err)
	require.NotNil(t, got.CachingConfig)
	assert.Equal(t, int64(120), got.CachingConfig.TTL)
	assert.Equal(t, []string{"$context.arguments.id"}, got.CachingConfig.CachingKeys)
}

func TestCreateResolver_SyncConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg     *appsync.SyncConfig
		name    string
		wantNil bool
	}{
		{
			name: "optimistic_concurrency",
			cfg: &appsync.SyncConfig{
				ConflictDetection: "VERSION",
				ConflictHandler:   "OPTIMISTIC_CONCURRENCY",
			},
		},
		{
			name: "automerge",
			cfg: &appsync.SyncConfig{
				ConflictDetection: "VERSION",
				ConflictHandler:   "AUTOMERGE",
			},
		},
		{
			name: "lambda_conflict_handler",
			cfg: &appsync.SyncConfig{
				ConflictDetection: "VERSION",
				ConflictHandler:   "LAMBDA",
				LambdaConflictHandlerConfig: &appsync.LambdaConflictHandlerConfig{
					LambdaConflictHandlerARN: "arn:aws:lambda:us-east-1:000000000000:function:conflict-handler",
				},
			},
		},
		{
			name: "no_detection",
			cfg: &appsync.SyncConfig{
				ConflictDetection: "NONE",
			},
		},
		{
			name:    "nil_sync_config",
			cfg:     nil,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "DS",
				Type: appsync.DataSourceTypeNone,
			})
			require.NoError(t, err)

			r := &appsync.Resolver{
				FieldName:      "updateItem",
				DataSourceName: "DS",
				SyncConfig:     tt.cfg,
			}

			created, err := b.CreateResolver(api.APIID, "Mutation", r)
			require.NoError(t, err)

			if tt.wantNil {
				assert.Nil(t, created.SyncConfig)
			} else {
				require.NotNil(t, created.SyncConfig)
				assert.Equal(t, tt.cfg.ConflictDetection, created.SyncConfig.ConflictDetection)
				assert.Equal(t, tt.cfg.ConflictHandler, created.SyncConfig.ConflictHandler)
			}
		})
	}
}

func TestCreateResolver_SyncConfig_GetRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:conflict"
	cfg := &appsync.SyncConfig{
		ConflictDetection: "VERSION",
		ConflictHandler:   "LAMBDA",
		LambdaConflictHandlerConfig: &appsync.LambdaConflictHandlerConfig{
			LambdaConflictHandlerARN: lambdaARN,
		},
	}

	r := &appsync.Resolver{
		FieldName:      "mutateItem",
		DataSourceName: "DS",
		SyncConfig:     cfg,
	}

	_, err = b.CreateResolver(api.APIID, "Mutation", r)
	require.NoError(t, err)

	got, err := b.GetResolver(api.APIID, "Mutation", "mutateItem")
	require.NoError(t, err)
	require.NotNil(t, got.SyncConfig)
	assert.Equal(t, "LAMBDA", got.SyncConfig.ConflictHandler)
	require.NotNil(t, got.SyncConfig.LambdaConflictHandlerConfig)
	assert.Equal(t, lambdaARN, got.SyncConfig.LambdaConflictHandlerConfig.LambdaConflictHandlerARN)
}

func TestCreateResolver_MaxBatchSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		maxBatchSize int32
	}{
		{name: "zero_batch_size", maxBatchSize: 0},
		{name: "batch_size_1", maxBatchSize: 1},
		{name: "batch_size_10", maxBatchSize: 10},
		{name: "batch_size_2000", maxBatchSize: 2000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "DS",
				Type: appsync.DataSourceTypeNone,
			})
			require.NoError(t, err)

			r := &appsync.Resolver{
				FieldName:      "batchOp",
				DataSourceName: "DS",
				MaxBatchSize:   tt.maxBatchSize,
			}

			created, err := b.CreateResolver(api.APIID, "Query", r)
			require.NoError(t, err)
			assert.Equal(t, tt.maxBatchSize, created.MaxBatchSize)
		})
	}
}

func TestCreateResolver_MaxBatchSize_GetRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	r := &appsync.Resolver{
		FieldName:      "batchGet",
		DataSourceName: "DS",
		MaxBatchSize:   100,
	}

	_, err = b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)

	got, err := b.GetResolver(api.APIID, "Query", "batchGet")
	require.NoError(t, err)
	assert.Equal(t, int32(100), got.MaxBatchSize)
}

func TestCreateResolver_AllFields(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	r := &appsync.Resolver{
		FieldName:      "complexOp",
		DataSourceName: "DS",
		MaxBatchSize:   50,
		CachingConfig: &appsync.CachingConfig{
			TTL:         60,
			CachingKeys: []string{"$context.arguments.id"},
		},
		SyncConfig: &appsync.SyncConfig{
			ConflictDetection: "VERSION",
			ConflictHandler:   "OPTIMISTIC_CONCURRENCY",
		},
	}

	created, err := b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)
	assert.Equal(t, int32(50), created.MaxBatchSize)
	require.NotNil(t, created.CachingConfig)
	assert.Equal(t, int64(60), created.CachingConfig.TTL)
	require.NotNil(t, created.SyncConfig)
	assert.Equal(t, "OPTIMISTIC_CONCURRENCY", created.SyncConfig.ConflictHandler)
}

func TestUpdateResolver_PreservesCachingAndSyncConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	r := &appsync.Resolver{
		FieldName:      "op",
		DataSourceName: "DS",
		CachingConfig:  &appsync.CachingConfig{TTL: 60},
		SyncConfig:     &appsync.SyncConfig{ConflictDetection: "VERSION", ConflictHandler: "AUTOMERGE"},
		MaxBatchSize:   10,
	}

	_, err = b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)

	updated := &appsync.Resolver{
		FieldName:      "op",
		DataSourceName: "DS",
		MaxBatchSize:   20,
		CachingConfig:  &appsync.CachingConfig{TTL: 120, CachingKeys: []string{"$context.arguments.id"}},
		SyncConfig:     &appsync.SyncConfig{ConflictDetection: "NONE"},
	}

	result, err := b.UpdateResolver(api.APIID, "Query", updated)
	require.NoError(t, err)
	assert.Equal(t, int32(20), result.MaxBatchSize)
	require.NotNil(t, result.CachingConfig)
	assert.Equal(t, int64(120), result.CachingConfig.TTL)
	require.NotNil(t, result.SyncConfig)
	assert.Equal(t, "NONE", result.SyncConfig.ConflictDetection)
}

func TestCreateResolver_SyncConfig_Automerge_NoLambda(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	r := &appsync.Resolver{
		FieldName:      "mergeOp",
		DataSourceName: "DS",
		SyncConfig: &appsync.SyncConfig{
			ConflictDetection: "VERSION",
			ConflictHandler:   "AUTOMERGE",
		},
	}

	created, err := b.CreateResolver(api.APIID, "Mutation", r)
	require.NoError(t, err)
	require.NotNil(t, created.SyncConfig)
	assert.Equal(t, "AUTOMERGE", created.SyncConfig.ConflictHandler)
	assert.Nil(t, created.SyncConfig.LambdaConflictHandlerConfig)
}

func TestCreateResolver_Code_Runtime_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
	require.NoError(t, err)

	ds := &appsync.DataSource{Name: "NONE", Type: appsync.DataSourceTypeNone}
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	r := &appsync.Resolver{
		TypeName:       "Query",
		FieldName:      "hello",
		Kind:           "UNIT",
		DataSourceName: "NONE",
		Code:           `export function request(ctx) { return {}; }`,
		Runtime:        &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"},
	}

	created, err := b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)
	assert.Equal(t, `export function request(ctx) { return {}; }`, created.Code)
	require.NotNil(t, created.Runtime)
	assert.Equal(t, "APPSYNC_JS", created.Runtime.Name)
	assert.Equal(t, "1.0.0", created.Runtime.RuntimeVersion)

	got, err := b.GetResolver(api.APIID, "Query", "hello")
	require.NoError(t, err)
	assert.Equal(t, `export function request(ctx) { return {}; }`, got.Code)
	require.NotNil(t, got.Runtime)
	assert.Equal(t, "APPSYNC_JS", got.Runtime.Name)
}

func TestUpdateResolver_Code_Runtime_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
	require.NoError(t, err)

	ds := &appsync.DataSource{Name: "NONE", Type: appsync.DataSourceTypeNone}
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	r := &appsync.Resolver{TypeName: "Query", FieldName: "hello", Kind: "UNIT", DataSourceName: "NONE"}
	_, err = b.CreateResolver(api.APIID, "Query", r)
	require.NoError(t, err)

	updated, err := b.UpdateResolver(api.APIID, "Query", &appsync.Resolver{
		TypeName:       "Query",
		FieldName:      "hello",
		DataSourceName: "NONE",
		Code:           `export function request(ctx) { return {payload: "hello"}; }`,
		Runtime:        &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"},
	})
	require.NoError(t, err)
	assert.Equal(t, `export function request(ctx) { return {payload: "hello"}; }`, updated.Code)
	require.NotNil(t, updated.Runtime)
	assert.Equal(t, "APPSYNC_JS", updated.Runtime.Name)
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

// TestListResolvers_Pagination verifies maxResults/nextToken on ListResolvers.
func TestListResolvers_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "res-api",
		"authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var apiOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiOut))
	apiID := apiOut["graphqlApi"].(map[string]any)["apiId"].(string)

	rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/datasources", apiID), map[string]any{
		"name": "ds",
		"type": "NONE",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/types", apiID), map[string]any{
		"definition": "type Query { placeholder: String }",
		"format":     "SDL",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	for i := range 4 {
		rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/types/Query/resolvers", apiID), map[string]any{
			"fieldName":      fmt.Sprintf("field%d", i),
			"dataSourceName": "ds",
			"kind":           "UNIT",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		path          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          fmt.Sprintf("/v1/apis/%s/types/Query/resolvers", apiID),
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          fmt.Sprintf("/v1/apis/%s/types/Query/resolvers?maxResults=2", apiID),
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var out struct {
				NextToken string           `json:"nextToken"`
				Resolvers []map[string]any `json:"resolvers"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assert.Len(t, out.Resolvers, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

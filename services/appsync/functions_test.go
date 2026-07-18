package appsync_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestGetFunction_NonExistentAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		apiID      string
		functionID string
	}{
		{
			name:       "unknown_api_returns_api_not_found",
			apiID:      "no-such-api",
			functionID: "fn-abc123",
		},
		{
			name:       "empty_backend_returns_api_not_found",
			apiID:      "ghost-api",
			functionID: "fn-xyz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			_, err := b.GetFunction(tt.apiID, tt.functionID)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			assert.Contains(t, err.Error(), tt.apiID)
		})
	}
}

func TestDeleteFunction_NonExistentAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		apiID      string
		functionID string
	}{
		{
			name:       "unknown_api_returns_api_not_found",
			apiID:      "no-such-api",
			functionID: "fn-abc123",
		},
		{
			name:       "empty_backend_returns_api_not_found",
			apiID:      "ghost-api",
			functionID: "fn-xyz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			err := b.DeleteFunction(tt.apiID, tt.functionID)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			assert.Contains(t, err.Error(), tt.apiID)
		})
	}
}

func TestGetFunction_ExistingAPI_ReturnsFunctionNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		functionID string
	}{
		{
			name:       "function_absent_on_valid_api",
			functionID: "fn-does-not-exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.GetFunction(api.APIID, tt.functionID)
			require.Error(t, err)
			require.ErrorIs(t, err, appsync.ErrNotFound)
			// Error must mention the function, not "api not found".
			assert.NotContains(t, err.Error(), "api "+api.APIID)
		})
	}
}

func TestCreateFunction_PipelineFunctionVersion_Default(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	f := &appsync.Function{
		Name:           "MyFunc",
		DataSourceName: "DS",
	}

	created, err := b.CreateFunction(api.APIID, f)
	require.NoError(t, err)
	assert.Equal(t, "2018-05-29", created.FunctionVersion)
}

func TestCreateFunction_PipelineFunctionVersion_Custom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		version     string
		wantVersion string
	}{
		{
			name:        "custom_version_preserved",
			version:     "2018-05-29",
			wantVersion: "2018-05-29",
		},
		{
			name:        "empty_gets_default",
			version:     "",
			wantVersion: "2018-05-29",
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

			f := &appsync.Function{
				Name:            "TestFunc",
				DataSourceName:  "DS",
				FunctionVersion: tt.version,
			}

			created, err := b.CreateFunction(api.APIID, f)
			require.NoError(t, err)
			assert.Equal(t, tt.wantVersion, created.FunctionVersion)
		})
	}
}

func TestCreateFunction_WithCode(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
		Name: "DS",
		Type: appsync.DataSourceTypeNone,
	})
	require.NoError(t, err)

	code := `export function request(ctx) { return {}; } export function response(ctx) { return ctx.result; }`

	f := &appsync.Function{
		Name:           "JsFunction",
		DataSourceName: "DS",
		Code:           code,
		MaxBatchSize:   5,
	}

	created, err := b.CreateFunction(api.APIID, f)
	require.NoError(t, err)
	assert.Equal(t, code, created.Code)
	assert.Equal(t, int32(5), created.MaxBatchSize)
	assert.Equal(t, "2018-05-29", created.FunctionVersion)

	got, err := b.GetFunction(api.APIID, created.FunctionID)
	require.NoError(t, err)
	assert.Equal(t, code, got.Code)
}

func TestCreateFunction_Runtime_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{Name: "NONE", Type: appsync.DataSourceTypeNone}
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	fn := &appsync.Function{
		Name:           "MyJSFn",
		DataSourceName: "NONE",
		Code:           `export function request(ctx) { return {}; }`,
		Runtime:        &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"},
	}

	created, err := b.CreateFunction(api.APIID, fn)
	require.NoError(t, err)
	require.NotNil(t, created.Runtime)
	assert.Equal(t, "APPSYNC_JS", created.Runtime.Name)
	assert.Equal(t, "1.0.0", created.Runtime.RuntimeVersion)

	got, err := b.GetFunction(api.APIID, created.FunctionID)
	require.NoError(t, err)
	require.NotNil(t, got.Runtime)
	assert.Equal(t, "APPSYNC_JS", got.Runtime.Name)
}

func TestCreateFunction_SyncConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{
		Name: "DYNAMO", Type: appsync.DataSourceTypeDynamoDB,
		DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{TableName: "t", AWSRegion: "us-east-1"},
	}
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	fn := &appsync.Function{
		Name:           "SyncFn",
		DataSourceName: "DYNAMO",
		SyncConfig: &appsync.SyncConfig{
			ConflictDetection: "VERSION",
			ConflictHandler:   "OPTIMISTIC_CONCURRENCY",
		},
	}

	created, err := b.CreateFunction(api.APIID, fn)
	require.NoError(t, err)
	require.NotNil(t, created.SyncConfig)
	assert.Equal(t, "VERSION", created.SyncConfig.ConflictDetection)
	assert.Equal(t, "OPTIMISTIC_CONCURRENCY", created.SyncConfig.ConflictHandler)

	got, err := b.GetFunction(api.APIID, created.FunctionID)
	require.NoError(t, err)
	require.NotNil(t, got.SyncConfig)
	assert.Equal(t, "VERSION", got.SyncConfig.ConflictDetection)
}

func TestUpdateFunction_Runtime_SyncConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	ds := &appsync.DataSource{Name: "NONE", Type: appsync.DataSourceTypeNone}
	_, err = b.CreateDataSource(api.APIID, ds)
	require.NoError(t, err)

	fn := &appsync.Function{Name: "Fn", DataSourceName: "NONE"}
	created, err := b.CreateFunction(api.APIID, fn)
	require.NoError(t, err)

	updated, err := b.UpdateFunction(api.APIID, created.FunctionID, &appsync.Function{
		Name:           "Fn",
		DataSourceName: "NONE",
		Runtime:        &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"},
		SyncConfig: &appsync.SyncConfig{
			ConflictDetection: "NONE",
			ConflictHandler:   "AUTOMERGE",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.Runtime)
	assert.Equal(t, "APPSYNC_JS", updated.Runtime.Name)
	require.NotNil(t, updated.SyncConfig)
	assert.Equal(t, "AUTOMERGE", updated.SyncConfig.ConflictHandler)
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

func TestInMemoryBackend_GetFunction_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.GetFunction(api.APIID, "nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
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

// TestListFunctions_Pagination verifies maxResults/nextToken on ListFunctions.
func TestListFunctions_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "fn-api",
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

	for i := range 4 {
		rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/functions", apiID), map[string]any{
			"name":            fmt.Sprintf("fn-%d", i),
			"dataSourceName":  "ds",
			"functionVersion": "2018-05-29",
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
			path:          fmt.Sprintf("/v1/apis/%s/functions", apiID),
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          fmt.Sprintf("/v1/apis/%s/functions?maxResults=2", apiID),
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
				Functions []map[string]any `json:"functions"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assert.Len(t, out.Functions, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

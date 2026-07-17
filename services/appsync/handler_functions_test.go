package appsync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

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

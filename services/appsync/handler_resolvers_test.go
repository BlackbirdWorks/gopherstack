package appsync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestHandler_Types_ResolverMethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	// CONNECT on individual resolver should return method not allowed (PUT is now UpdateResolver).
	rec := doRequest(t, h, http.MethodConnect, "/v1/apis/"+api.APIID+"/types/Query/resolvers/getItem", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
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

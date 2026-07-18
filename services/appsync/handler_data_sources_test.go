package appsync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestHandler_DataSource_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	// PATCH on named datasource should return method not allowed (PUT is UpdateDataSource).
	rec := doRequest(t, h, http.MethodPatch, "/v1/apis/"+api.APIID+"/datasources/myds", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_DataSourceIntrospections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
		setupAPI   bool
		setupDS    bool
	}{
		{
			name:   "start_introspection_success",
			method: http.MethodPost,
			path:   "/v1/dataSource-introspections",
			body: map[string]any{
				"apiId":          "__APIID__",
				"dataSourceName": "MyDS",
			},
			setupAPI:   true,
			setupDS:    true,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "start_introspection_bad_body",
			method:     http.MethodPost,
			path:       "/v1/dataSource-introspections",
			body:       "not-json-string",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "start_introspection_api_not_found",
			method:     http.MethodPost,
			path:       "/v1/dataSource-introspections",
			body:       map[string]any{"apiId": "noexist", "dataSourceName": "DS"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "get_introspection_success",
			method:     http.MethodGet,
			path:       "/v1/dataSource-introspections/some-id",
			wantStatus: http.StatusOK,
		},
		{
			name:       "method_not_allowed_on_collection",
			method:     http.MethodGet,
			path:       "/v1/dataSource-introspections",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "method_not_allowed_on_item",
			method:     http.MethodPost,
			path:       "/v1/dataSource-introspections/some-id",
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()

			body := tt.body

			if tt.setupAPI {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				if m, ok := body.(map[string]any); ok && m["apiId"] == "__APIID__" {
					m["apiId"] = api.APIID
				}

				if tt.setupDS {
					_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
						Name: "MyDS",
						Type: appsync.DataSourceTypeNone,
					})
					require.NoError(t, err)
				}
			}

			rec := doRequest(t, h, tt.method, tt.path, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
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

func TestHandler_DataSources_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/datasources", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
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

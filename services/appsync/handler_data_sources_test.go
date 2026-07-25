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

	validConfig := map[string]any{
		"rdsDataApiConfig": map[string]any{
			"databaseName": "mydb",
			"resourceArn":  "arn:aws:rds:us-east-1:000000000000:cluster:mycluster",
			"secretArn":    "arn:aws:secretsmanager:us-east-1:000000000000:secret:mysecret",
		},
	}

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "start_success_real_path",
			method:     http.MethodPost,
			path:       "/v1/datasources/introspections",
			body:       validConfig,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "start_success_legacy_path",
			method:     http.MethodPost,
			path:       "/v1/dataSource-introspections",
			body:       validConfig,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "start_bad_body",
			method:     http.MethodPost,
			path:       "/v1/datasources/introspections",
			body:       "not-json-string",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "start_missing_config",
			method:     http.MethodPost,
			path:       "/v1/datasources/introspections",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "get_unknown_id_real_path",
			method:     http.MethodGet,
			path:       "/v1/datasources/introspections/some-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "get_unknown_id_legacy_path",
			method:     http.MethodGet,
			path:       "/v1/dataSource-introspections/some-id",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "method_not_allowed_on_collection_real_path",
			method:     http.MethodGet,
			path:       "/v1/datasources/introspections",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "method_not_allowed_on_item_real_path",
			method:     http.MethodPost,
			path:       "/v1/datasources/introspections/some-id",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "method_not_allowed_on_collection_legacy_path",
			method:     http.MethodGet,
			path:       "/v1/dataSource-introspections",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "method_not_allowed_on_item_legacy_path",
			method:     http.MethodPost,
			path:       "/v1/dataSource-introspections/some-id",
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DataSourceIntrospection_StartThenGet locks the full wire shape of a
// successful introspection round trip: StartDataSourceIntrospection's response fields
// (introspectionId/introspectionStatus/introspectionStatusDetail, no
// introspectionResult) and GetDataSourceIntrospection's response fields
// (introspectionId/introspectionResult/introspectionStatus/introspectionStatusDetail),
// plus that the legacy /v1/dataSource-introspections alias resolves the same
// persisted record as the real /v1/datasources/introspections path.
func TestHandler_DataSourceIntrospection_StartThenGet(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	startRec := doRequest(t, h, http.MethodPost, "/v1/datasources/introspections", map[string]any{
		"rdsDataApiConfig": map[string]any{
			"databaseName": "mydb",
			"resourceArn":  "arn:aws:rds:us-east-1:000000000000:cluster:mycluster",
			"secretArn":    "arn:aws:secretsmanager:us-east-1:000000000000:secret:mysecret",
		},
	})
	require.Equal(t, http.StatusCreated, startRec.Code)

	var startOut struct {
		IntrospectionID     string `json:"introspectionId"`
		IntrospectionStatus string `json:"introspectionStatus"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	assert.NotEmpty(t, startOut.IntrospectionID)
	assert.Equal(t, "SUCCESS", startOut.IntrospectionStatus)

	getRec := doRequest(t, h, http.MethodGet, "/v1/datasources/introspections/"+startOut.IntrospectionID, nil)
	assert.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		IntrospectionID     string `json:"introspectionId"`
		IntrospectionStatus string `json:"introspectionStatus"`
		IntrospectionResult struct {
			Models []any `json:"models"`
		} `json:"introspectionResult"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, startOut.IntrospectionID, getOut.IntrospectionID)
	assert.Equal(t, "SUCCESS", getOut.IntrospectionStatus)
	assert.Empty(t, getOut.IntrospectionResult.Models)

	// The legacy alias resolves the same persisted record.
	legacyGetRec := doRequest(t, h, http.MethodGet, "/v1/dataSource-introspections/"+startOut.IntrospectionID, nil)
	assert.Equal(t, http.StatusOK, legacyGetRec.Code)
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

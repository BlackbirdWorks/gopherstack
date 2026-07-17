package appsync_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestHandler_StartSchemaCreation_Base64Encoded(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	sdl := `type Query { hello: String }`
	encoded := base64.StdEncoding.EncodeToString([]byte(sdl))

	body := map[string]any{"definition": encoded}
	rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/schemacreation", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_SchemaMerge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		createAPI  bool
		wantStatus int
	}{
		{
			name:       "post_success",
			method:     http.MethodPost,
			createAPI:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "api_not_found",
			method:     http.MethodPost,
			createAPI:  false,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "method_not_allowed",
			method:     http.MethodGet,
			createAPI:  true,
			wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := "nonexistent"

			if tt.createAPI {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				apiID = api.APIID
			}

			rec := doRequest(t, h, tt.method, "/v1/apis/"+apiID+"/schemaMerge", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_StartSchemaCreation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sdl         string
		wantStatus2 string
		wantStatus  int
	}{
		{
			name:        "valid_schema_returns_active",
			sdl:         `type Query { hello: String }`,
			wantStatus:  http.StatusOK,
			wantStatus2: string(appsync.SchemaStatusActive),
		},
		{
			name:       "invalid_schema_returns_400",
			sdl:        `type { broken`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

			body := map[string]any{"definition": tt.sdl}
			rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+api.APIID+"/schemacreation", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus2 != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.Equal(t, tt.wantStatus2, resp["status"])
			}
		})
	}
}

func TestHandler_GetSchemaCreationStatus(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/schemacreation", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, string(appsync.SchemaStatusActive), resp["status"])
}

func TestHandler_GetIntrospectionSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		hasSchema  bool
		wantStatus int
	}{
		{
			name:       "returns_schema_sdl",
			hasSchema:  true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "returns_404_when_no_schema",
			hasSchema:  false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

			if tt.hasSchema {
				_, _ = b.StartSchemaCreation(api.APIID, `type Query { hello: String }`)
			}

			rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/schema", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_SchemaCreations_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/schemacreation", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

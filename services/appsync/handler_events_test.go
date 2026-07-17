package appsync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateApi(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "creates_event_api_successfully",
			body: map[string]any{
				"name": "MyEventAPI",
				"tags": map[string]string{"env": "test"},
			},
			wantStatus: http.StatusCreated,
			wantName:   "MyEventAPI",
		},
		{
			name:       "missing_name_returns_400",
			body:       map[string]any{"ownerContact": "owner@example.com"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, http.MethodPost, "/v2/apis", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				api, ok := resp["api"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, api["name"])
				assert.NotEmpty(t, api["apiId"])
			}
		})
	}
}

func TestHandler_EventAPI_CRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// Create event API.
	rec := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{"name": "MyEventAPI"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	apiID := createResp["api"].(map[string]any)["apiId"].(string)

	// List APIs.
	rec2 := doRequest(t, h, http.MethodGet, "/v2/apis", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&listResp))
	items := listResp["apis"].([]any)
	assert.Len(t, items, 1)

	// Get API.
	rec3 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID, nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec3.Body).Decode(&getResp))
	assert.Equal(t, "MyEventAPI", getResp["api"].(map[string]any)["name"])

	// Delete API.
	rec4 := doRequest(t, h, http.MethodDelete, "/v2/apis/"+apiID, nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Get after delete returns 404.
	rec5 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID, nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)

	// List after delete returns empty.
	rec6 := doRequest(t, h, http.MethodGet, "/v2/apis", nil)
	require.Equal(t, http.StatusOK, rec6.Code)

	var listResp2 map[string]any
	require.NoError(t, json.NewDecoder(rec6.Body).Decode(&listResp2))
	assert.Empty(t, listResp2["apis"])
}

func TestHandler_EventAPI_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/v2/apis/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_EventAPI_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// CONNECT is never allowed on any v2 API endpoint.
	rec := doRequest(t, h, http.MethodConnect, "/v2/apis/nonexistent", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_UpdateAPI(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec1 := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{"name": "EventAPI"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&createResp))
	apiID := createResp["api"].(map[string]any)["apiId"].(string)

	rec2 := doRequest(t, h, http.MethodPut, "/v2/apis/"+apiID,
		map[string]any{"name": "UpdatedAPI", "ownerContact": "ops@example.com"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp))
	api := resp["api"].(map[string]any)
	assert.Equal(t, "UpdatedAPI", api["name"])
	assert.Equal(t, "ops@example.com", api["ownerContact"])

	// Update not-found returns 404.
	rec3 := doRequest(t, h, http.MethodPut, "/v2/apis/nonexistent",
		map[string]any{"name": "x"})
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestHandler_V2API_GetAndDeleteNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "get_nonexistent",
			method:   http.MethodGet,
			path:     "/v2/apis/nonexistent",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete_nonexistent",
			method:   http.MethodDelete,
			path:     "/v2/apis/nonexistent",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "update_nonexistent",
			method:   http.MethodPut,
			path:     "/v2/apis/nonexistent",
			body:     map[string]any{"name": "NewName"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

package appsync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestHandler_CreateApiCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*appsync.InMemoryBackend) string
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "creates_api_cache_successfully",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)

				return api.APIID
			},
			body: map[string]any{
				"apiCachingBehavior": "FULL_REQUEST_CACHING",
				"ttl":                300,
				"type":               "SMALL",
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "returns_404_for_missing_api",
			setup: func(_ *appsync.InMemoryBackend) string {
				return "nonexistent"
			},
			body:       map[string]any{"ttl": 300},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := tt.setup(b)

			rec := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/ApiCaches", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateAPICache_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid",
			body: map[string]any{
				"ttl":                int64(60),
				"type":               "SMALL",
				"apiCachingBehavior": "FULL_REQUEST_CACHING",
			},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "ttl_zero_rejected",
			body:       map[string]any{"ttl": 0, "type": "SMALL", "apiCachingBehavior": "FULL_REQUEST_CACHING"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_type_rejected",
			body:       map[string]any{"ttl": 60, "type": "BOGUS", "apiCachingBehavior": "FULL_REQUEST_CACHING"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_caching_behavior_rejected",
			body:       map[string]any{"ttl": 60, "type": "SMALL", "apiCachingBehavior": "BOGUS"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			// Create an API first.
			body := map[string]any{"name": "TestAPI", "authenticationType": "API_KEY"}
			rec := doRequest(t, h, http.MethodPost, "/v1/apis", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			gqlAPI := resp["graphqlApi"].(map[string]any)
			apiID := gqlAPI["apiId"].(string)

			rec2 := doRequest(t, h, http.MethodPost, "/v1/apis/"+apiID+"/ApiCaches", tt.body)
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

func TestHandler_GetApiCache(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(api.APIID, &appsync.APICache{
		TTL:                60,
		Type:               "SMALL",
		APICachingBehavior: "FULL_REQUEST_CACHING",
	})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v1/apis/"+api.APIID+"/ApiCaches", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotNil(t, resp["apiCache"])
}

func TestHandler_DeleteApiCache(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(api.APIID, &appsync.APICache{
		TTL:                60,
		Type:               "SMALL",
		APICachingBehavior: "FULL_REQUEST_CACHING",
	})
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/ApiCaches", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Second delete returns 404.
	rec2 := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/ApiCaches", nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestHandler_UpdateApiCache(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(
		api.APIID,
		&appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
	)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/ApiCaches",
		map[string]any{"ttl": 120, "type": "LARGE"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	cache := resp["apiCache"].(map[string]any)
	assert.Equal(t, "LARGE", cache["type"])
}

func TestHandler_FlushApiCache(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(
		api.APIID,
		&appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
	)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api.APIID+"/ApiCaches/entries", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Flush without cache returns 404.
	api2, err := b.CreateGraphqlAPI("TestAPI2", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	rec2 := doRequest(t, h, http.MethodDelete, "/v1/apis/"+api2.APIID+"/ApiCaches/entries", nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestHandler_UpdateApiCache_NotFound(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodPut, "/v1/apis/"+api.APIID+"/ApiCaches",
		map[string]any{"ttl": 60})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

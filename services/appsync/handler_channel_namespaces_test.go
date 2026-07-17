package appsync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestHandler_CreateChannelNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*appsync.InMemoryBackend) string
		body       map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "creates_channel_namespace_successfully",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateAPI("MyEventAPI", "", nil, nil)

				return api.APIID
			},
			body:       map[string]any{"name": "default"},
			wantStatus: http.StatusCreated,
			wantName:   "default",
		},
		{
			name: "missing_name_returns_400",
			setup: func(b *appsync.InMemoryBackend) string {
				api, _ := b.CreateAPI("MyEventAPI", "", nil, nil)

				return api.APIID
			},
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "returns_404_for_missing_api",
			setup: func(_ *appsync.InMemoryBackend) string {
				return "nonexistent"
			},
			body:       map[string]any{"name": "default"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			apiID := tt.setup(b)

			rec := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/channelNamespaces", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				ns, ok := resp["channelNamespace"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, ns["name"])
			}
		})
	}
}

func TestHandler_ChannelNamespace_CRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// Create event API first.
	rec1 := doRequest(t, h, http.MethodPost, "/v2/apis", map[string]any{"name": "EventAPI"})
	require.Equal(t, http.StatusCreated, rec1.Code)

	var apiResp map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&apiResp))
	apiID := apiResp["api"].(map[string]any)["apiId"].(string)

	// Create namespace.
	rec2 := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/channelNamespaces",
		map[string]any{"name": "ns1"})
	require.Equal(t, http.StatusCreated, rec2.Code)

	// List channel namespaces.
	rec3 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/channelNamespaces", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec3.Body).Decode(&listResp))
	items := listResp["channelNamespaces"].([]any)
	assert.Len(t, items, 1)

	// Get channel namespace.
	rec4 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/channelNamespaces/ns1", nil)
	require.Equal(t, http.StatusOK, rec4.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec4.Body).Decode(&getResp))
	ns := getResp["channelNamespace"].(map[string]any)
	assert.Equal(t, "ns1", ns["name"])

	// Update channel namespace.
	rec5 := doRequest(t, h, http.MethodPut, "/v2/apis/"+apiID+"/channelNamespaces/ns1",
		map[string]any{"codeHandlers": "export const handler = () => {}"})
	require.Equal(t, http.StatusOK, rec5.Code)

	// Delete channel namespace.
	rec6 := doRequest(t, h, http.MethodDelete, "/v2/apis/"+apiID+"/channelNamespaces/ns1", nil)
	assert.Equal(t, http.StatusNoContent, rec6.Code)

	// Get after delete returns 404.
	rec7 := doRequest(t, h, http.MethodGet, "/v2/apis/"+apiID+"/channelNamespaces/ns1", nil)
	assert.Equal(t, http.StatusNotFound, rec7.Code)
}

func TestHandler_ChannelNamespace_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodGet, "/v2/apis/nonexistent/channelNamespaces/ns1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ChannelNamespace_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	// POST is a valid method here (the real AWS SDK sends UpdateChannelNamespace as
	// POST to this exact path, not PUT/PATCH); use a method nothing maps to instead.
	rec := doRequest(t, h, http.MethodOptions, "/v2/apis/x/channelNamespaces/ns1", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_V2API_ChannelNamespaceNotFound(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler()
	api, err := b.CreateAPI("TestAPI", "", nil, nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/v2/apis/"+api.APIID+"/channelNamespaces/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

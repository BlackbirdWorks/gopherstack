package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

func TestSourceLocation_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte)
		body     any
		path     string
		name     string
		wantCode int
	}{
		{
			name: "create returns source location with ARN",
			path: "/sourceLocation/my-location",
			body: map[string]any{
				"HttpConfiguration": map[string]any{"BaseUrl": "https://content.example.com"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "my-location", resp["SourceLocationName"])
				assert.Contains(t, resp["Arn"], "arn:aws:mediatailor:us-east-1:000000000000:sourceLocation/my-location")
				httpCfg, _ := resp["HttpConfiguration"].(map[string]any)
				assert.Equal(t, "https://content.example.com", httpCfg["BaseUrl"])
			},
		},
		{
			name:     "create missing BaseUrl returns 400",
			path:     "/sourceLocation/bad",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestSourceLocation_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/test-sl", map[string]any{
		"HttpConfiguration": map[string]any{"BaseUrl": "https://content.example.com"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, mediatailor.SourceLocationCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/test-sl", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "test-sl", descResp["SourceLocationName"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/sourceLocation/test-sl", map[string]any{
		"HttpConfiguration": map[string]any{"BaseUrl": "https://updated.example.com"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	httpCfg, _ := updateResp["HttpConfiguration"].(map[string]any)
	assert.Equal(t, "https://updated.example.com", httpCfg["BaseUrl"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/sourceLocations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Items"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/sourceLocation/test-sl", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, mediatailor.SourceLocationCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/test-sl", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSourceLocation_DuplicateCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/sourceLocation/sl1", map[string]any{
		"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
	})

	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1", map[string]any{
		"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
	})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestSourceLocation_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe unknown returns 404", http.MethodGet, "/sourceLocation/notexist"},
		{"update unknown returns 404", http.MethodPut, "/sourceLocation/notexist"},
		{"delete unknown returns 404", http.MethodDelete, "/sourceLocation/notexist"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestSourceLocation_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/sourceLocations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Items"])
}

func TestListSourceLocations_WithItems(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 2 {
		name := "sl-" + string(rune('a'+i))
		doRequest(t, h, http.MethodPost, "/sourceLocation/"+name, map[string]any{
			"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
		})
	}

	rec := doRequest(t, h, http.MethodGet, "/sourceLocations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["Items"].([]any)
	assert.Len(t, items, 2)
}

func TestCreateSourceLocation_MissingBaseURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDeleteSourceLocation_WithAttachedSources verifies DeleteSourceLocation
// rejects when vod/live sources are attached.
func TestDeleteSourceLocation_WithAttachedSources(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		setup      func(t *testing.T, h *mediatailor.Handler)
		wantStatus int
	}{
		{
			name:       "no_attached_sources_ok",
			setup:      func(_ *testing.T, _ *mediatailor.Handler) {},
			wantStatus: http.StatusOK,
		},
		{
			name: "attached_vod_source_rejected",
			setup: func(t *testing.T, h *mediatailor.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/vs1", map[string]any{
					"HttpPackageConfigurations": []any{
						map[string]any{"Path": "/", "SourceGroup": "hd", "Type": "HLS"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "attached_live_source_rejected",
			setup: func(t *testing.T, h *mediatailor.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/liveSource/ls1", map[string]any{
					"HttpPackageConfigurations": []any{
						map[string]any{"Path": "/", "SourceGroup": "hd", "Type": "HLS"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPost, "/sourceLocation/sl1", map[string]any{
				"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
			})

			tt.setup(t, h)

			rec := doRequest(t, h, http.MethodDelete, "/sourceLocation/sl1", nil)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

package mediatailor_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

func newTestHandler(t *testing.T) *mediatailor.Handler {
	t.Helper()

	backend := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")

	return mediatailor.NewHandler(backend)
}

func doRequest(t *testing.T, h *mediatailor.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	if body != nil {
		req.ContentLength = int64(len(bodyBytes))
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func createTestSourceLocation(t *testing.T, h *mediatailor.Handler) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1", map[string]any{
		"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// --- PlaybackConfiguration tests ---

func TestAudit1_PlaybackConfiguration_PutAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "put returns config with ARN and endpoint prefixes",
			body: map[string]any{
				"Name":                  "my-config",
				"AdDecisionServerUrl":   "https://ads.example.com",
				"VideoContentSourceUrl": "https://video.example.com",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "my-config", resp["Name"])
				assert.Contains(
					t,
					resp["PlaybackConfigurationArn"],
					"arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/my-config",
				)
				assert.NotEmpty(t, resp["PlaybackEndpointPrefix"])
				assert.NotEmpty(t, resp["SessionInitializationEndpointPrefix"])
				assert.Equal(t, "https://ads.example.com", resp["AdDecisionServerUrl"])
				assert.Equal(t, "https://video.example.com", resp["VideoContentSourceUrl"])
			},
		},
		{
			name:     "put missing Name returns 400",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAudit1_PlaybackConfiguration_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Put
	rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  "test-config",
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, mediatailor.PlaybackConfigurationCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Get
	rec = doRequest(t, h, http.MethodGet, "/playbackConfiguration/test-config", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "test-config", getResp["Name"])
	assert.Equal(t, "https://ads.example.com", getResp["AdDecisionServerUrl"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/playbackConfigurations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Items"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/playbackConfiguration/test-config", nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, 0, mediatailor.PlaybackConfigurationCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Get deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/playbackConfiguration/test-config", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit1_PlaybackConfiguration_PutIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  "cfg1",
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
	})
	doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  "cfg1",
		"AdDecisionServerUrl":   "https://new-ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
	})

	assert.Equal(t, 1, mediatailor.PlaybackConfigurationCount(h.Backend.(*mediatailor.InMemoryBackend)))

	rec := doRequest(t, h, http.MethodGet, "/playbackConfiguration/cfg1", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "https://new-ads.example.com", resp["AdDecisionServerUrl"])
}

func TestAudit1_PlaybackConfiguration_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode int
	}{
		{"get unknown returns 404", http.MethodGet, "/playbackConfiguration/notexist", http.StatusNotFound},
		{
			"delete unknown is idempotent returns 204",
			http.MethodDelete,
			"/playbackConfiguration/notexist",
			http.StatusNoContent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestAudit1_PlaybackConfiguration_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/playbackConfigurations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Items"])
}

// --- Channel tests ---

func TestAudit1_Channel_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, body []byte)
		body     any
		path     string
		name     string
		wantCode int
	}{
		{
			name: "create returns channel with ARN and STOPPED state",
			path: "/channel/my-channel",
			body: map[string]any{
				"PlaybackMode": "LOOP",
				"Outputs": []any{
					map[string]any{"ManifestName": "index", "SourceGroup": "hd"},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "my-channel", resp["ChannelName"])
				assert.Contains(t, resp["Arn"], "arn:aws:mediatailor:us-east-1:000000000000:channel/my-channel")
				assert.Equal(t, "STOPPED", resp["ChannelState"])
				assert.Equal(t, "LOOP", resp["PlaybackMode"])
				assert.Len(t, resp["Outputs"], 1)
			},
		},
		{
			name:     "create missing ChannelName in path still works",
			path:     "/channel/ch2",
			body:     map[string]any{},
			wantCode: http.StatusOK,
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

func TestAudit1_Channel_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/channel/test-channel", map[string]any{
		"PlaybackMode": "LOOP",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, mediatailor.ChannelCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/channel/test-channel", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "test-channel", descResp["ChannelName"])
	assert.Equal(t, "STOPPED", descResp["ChannelState"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/channel/test-channel", map[string]any{
		"Outputs": []any{
			map[string]any{"ManifestName": "index", "SourceGroup": "hd"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Len(t, updateResp["Outputs"], 1)

	// List
	rec = doRequest(t, h, http.MethodGet, "/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Items"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/channel/test-channel", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, mediatailor.ChannelCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/channel/test-channel", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit1_Channel_StartStop(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{})

	// Start
	rec := doRequest(t, h, http.MethodPut, "/channel/ch1/start", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify running
	rec = doRequest(t, h, http.MethodGet, "/channel/ch1", nil)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "RUNNING", descResp["ChannelState"])

	// Start again is idempotent
	rec = doRequest(t, h, http.MethodPut, "/channel/ch1/start", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Stop
	rec = doRequest(t, h, http.MethodPut, "/channel/ch1/stop", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify stopped
	rec = doRequest(t, h, http.MethodGet, "/channel/ch1", nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "STOPPED", descResp["ChannelState"])

	// Stop again is idempotent
	rec = doRequest(t, h, http.MethodPut, "/channel/ch1/stop", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit1_Channel_DeleteRunning(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{})
	doRequest(t, h, http.MethodPut, "/channel/ch1/start", nil)

	rec := doRequest(t, h, http.MethodDelete, "/channel/ch1", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestAudit1_Channel_DuplicateCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{})
	rec := doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestAudit1_Channel_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe unknown returns 404", http.MethodGet, "/channel/notexist"},
		{"update unknown returns 404", http.MethodPut, "/channel/notexist"},
		{"delete unknown returns 404", http.MethodDelete, "/channel/notexist"},
		{"start unknown returns 404", http.MethodPut, "/channel/notexist/start"},
		{"stop unknown returns 404", http.MethodPut, "/channel/notexist/stop"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestAudit1_Channel_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Items"])
}

// --- SourceLocation tests ---

func TestAudit1_SourceLocation_CreateAndDescribe(t *testing.T) {
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
				httpCfg := resp["HttpConfiguration"].(map[string]any)
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

func TestAudit1_SourceLocation_CRUD(t *testing.T) {
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
	httpCfg := updateResp["HttpConfiguration"].(map[string]any)
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

func TestAudit1_SourceLocation_DuplicateCreate(t *testing.T) {
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

func TestAudit1_SourceLocation_NotFound(t *testing.T) {
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

func TestAudit1_SourceLocation_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/sourceLocations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Items"])
}

// --- VodSource tests ---

func TestAudit1_VodSource_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	tests := []struct {
		check    func(t *testing.T, body []byte)
		body     any
		path     string
		name     string
		wantCode int
	}{
		{
			name: "create returns vod source with ARN",
			path: "/sourceLocation/sl1/vodSource/my-vod",
			body: map[string]any{
				"HttpPackageConfigurations": []any{
					map[string]any{"Path": "/hls", "SourceGroup": "hd", "Type": "HLS"},
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "my-vod", resp["VodSourceName"])
				assert.Equal(t, "sl1", resp["SourceLocationName"])
				assert.Contains(
					t,
					resp["Arn"],
					"arn:aws:mediatailor:us-east-1:000000000000:sourceLocation/sl1/vodSource/my-vod",
				)
				assert.Len(t, resp["HttpPackageConfigurations"], 1)
			},
		},
		{
			name:     "create with missing source location returns 404",
			path:     "/sourceLocation/noexist/vodSource/v1",
			body:     map[string]any{},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAudit1_VodSource_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/sourceLocation/sl1/vodSource/vod1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/hls", "SourceGroup": "hd", "Type": "HLS"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, mediatailor.VodSourceCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/vodSource/vod1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "vod1", descResp["VodSourceName"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/sourceLocation/sl1/vodSource/vod1", map[string]any{
		"HttpPackageConfigurations": []any{
			map[string]any{"Path": "/hls", "SourceGroup": "hd", "Type": "HLS"},
			map[string]any{"Path": "/dash", "SourceGroup": "hd", "Type": "DASH_ISO"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Len(t, updateResp["HttpPackageConfigurations"], 2)

	// List
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/vodSources", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Items"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/sourceLocation/sl1/vodSource/vod1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, mediatailor.VodSourceCount(h.Backend.(*mediatailor.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/vodSource/vod1", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit1_VodSource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe unknown returns 404", http.MethodGet, "/sourceLocation/sl1/vodSource/notexist"},
		{"update unknown returns 404", http.MethodPut, "/sourceLocation/sl1/vodSource/notexist"},
		{"delete unknown returns 404", http.MethodDelete, "/sourceLocation/sl1/vodSource/notexist"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestAudit1_VodSource_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestSourceLocation(t, h)

	rec := doRequest(t, h, http.MethodGet, "/sourceLocation/sl1/vodSources", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Items"])
}

// --- Tags tests ---

func TestAudit1_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a playback config to get an ARN
	rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  "tagged-config",
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	resourceARN := putResp["PlaybackConfigurationArn"].(string)

	// TagResource
	rec = doRequest(t, h, http.MethodPost, "/tags/"+resourceARN, map[string]any{
		"Tags": map[string]any{"env": "prod", "team": "media"},
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, http.MethodGet, "/tags/"+resourceARN, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags := listResp["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "media", tags["team"])

	// UntagResource
	req := httptest.NewRequest(http.MethodDelete, "/tags/"+resourceARN+"?tagKeys=env", nil)
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Verify tag removed
	rec = doRequest(t, h, http.MethodGet, "/tags/"+resourceARN, nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags = listResp["Tags"].(map[string]any)
	assert.NotContains(t, tags, "env")
	assert.Equal(t, "media", tags["team"])
}

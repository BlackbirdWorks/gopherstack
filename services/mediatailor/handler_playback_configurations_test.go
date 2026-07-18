package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

func TestPlaybackConfiguration_PutAndGet(t *testing.T) {
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

func TestPlaybackConfiguration_CRUD(t *testing.T) {
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

func TestPlaybackConfiguration_PutIdempotent(t *testing.T) {
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

func TestPlaybackConfiguration_NotFound(t *testing.T) {
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

func TestPlaybackConfiguration_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/playbackConfigurations", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Items"])
}

func TestListPlaybackConfigurations_WithNextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		name := "cfg-" + string(rune('a'+i))
		doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
			"Name":                  name,
			"AdDecisionServerUrl":   "https://ads.com",
			"VideoContentSourceUrl": "https://video.com",
		})
	}

	rec := doRequest(t, h, http.MethodGet, "/playbackConfigurations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, _ := resp["Items"].([]any)
	assert.Len(t, items, 3)
}

// TestPutPlaybackConfiguration_RequiredFields verifies the real model's only
// required member is Name — confirmed against
// aws-sdk-go-v2/service/mediatailor's validators.go and botocore's
// service-2.json ("required": ["Name"]). Requiring AdDecisionServerUrl or
// VideoContentSourceUrl rejects requests a real SDK client is allowed to send.
func TestPutPlaybackConfiguration_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name       string
		body       map[string]any
		wantStatus int
	}{
		{
			name:       "all_fields_present_ok",
			wantStatus: http.StatusOK,
			body: map[string]any{
				"Name":                  "cfg",
				"AdDecisionServerUrl":   "https://ads.example.com",
				"VideoContentSourceUrl": "https://video.example.com",
			},
		},
		{
			name:       "missing_AdDecisionServerUrl_ok",
			wantStatus: http.StatusOK,
			body: map[string]any{
				"Name":                  "cfg",
				"VideoContentSourceUrl": "https://video.example.com",
			},
		},
		{
			name:       "missing_VideoContentSourceUrl_ok",
			wantStatus: http.StatusOK,
			body: map[string]any{
				"Name":                "cfg",
				"AdDecisionServerUrl": "https://ads.example.com",
			},
		},
		{
			name:       "name_only_ok",
			wantStatus: http.StatusOK,
			body: map[string]any{
				"Name": "cfg",
			},
		},
		{
			name:       "missing_Name_rejected",
			wantStatus: http.StatusBadRequest,
			body:       map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// TestPutPlaybackConfiguration_TagsReplacedOnUpdate verifies
// PutPlaybackConfiguration replaces tags on update (not merges).
func TestPutPlaybackConfiguration_TagsReplacedOnUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  "cfg",
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
		"tags":                  map[string]any{"old": "val"},
	})

	doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  "cfg",
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
		"tags":                  map[string]any{"new": "val"},
	})

	rec := doRequest(t, h, http.MethodGet, "/playbackConfiguration/cfg", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	tags, _ := resp["tags"].(map[string]any)

	assert.Contains(t, tags, "new", "new tag must be present after update")
	assert.NotContains(t, tags, "old", "old tag must be removed on full tag replacement")
}

// TestPutPlaybackConfiguration_HlsConfiguration verifies
// PutPlaybackConfiguration returns HlsConfiguration.ManifestEndpointPrefix.
func TestPutPlaybackConfiguration_HlsConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  "hls-cfg",
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	hlsCfg, ok := resp["HlsConfiguration"].(map[string]any)
	require.True(t, ok, "HlsConfiguration must be present in response")
	assert.NotEmpty(t, hlsCfg["ManifestEndpointPrefix"], "ManifestEndpointPrefix must be set")
	assert.Contains(t, hlsCfg["ManifestEndpointPrefix"], "hls-cfg", "ManifestEndpointPrefix must contain config name")
}

// TestDeletePlaybackConfiguration_Idempotent verifies
// DeletePlaybackConfiguration is idempotent.
func TestDeletePlaybackConfiguration_Idempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		exists     bool
	}{
		{name: "delete_existing_returns_204", exists: true, wantStatus: http.StatusNoContent},
		{name: "delete_nonexistent_returns_204", exists: false, wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.exists {
				createTestPlaybackConfig(t, h, "cfg")
			}

			rec := doRequest(t, h, http.MethodDelete, "/playbackConfiguration/cfg", nil)
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

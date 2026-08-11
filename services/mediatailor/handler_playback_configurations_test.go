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

// TestPutPlaybackConfiguration_ExtraConfigRoundTrips verifies
// PutPlaybackConfiguration's optional sub-configs (AvailSuppression, Bumper,
// CdnConfiguration, etc. -- previously entirely unmodeled) are stored and
// echoed back verbatim on Get/List, matching what a real client PUT.
func TestPutPlaybackConfiguration_ExtraConfigRoundTrips(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name": "cfg1",
		"AvailSuppression": map[string]any{
			"Mode":  "BEHIND_LIVE_EDGE",
			"Value": "00:45:00",
		},
		"Bumper": map[string]any{
			"StartUrl": "https://example.com/start.mp4",
			"EndUrl":   "https://example.com/end.mp4",
		},
		"CdnConfiguration": map[string]any{
			"AdSegmentUrlPrefix": "https://cdn.example.com/ads",
		},
		"PersonalizationThresholdSeconds": float64(10),
		"SlateAdUrl":                      "https://example.com/slate.mp4",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	assertPlaybackConfigExtras(t, putResp)

	rec = doRequest(t, h, http.MethodGet, "/playbackConfiguration/cfg1", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assertPlaybackConfigExtras(t, getResp)
}

// TestPutPlaybackConfiguration_AdsPersonalization verifies
// AdsPersonalizationConcurrency/AdsPersonalizationTimeouts (added to the real
// PlaybackConfiguration model after extractExtraConfig's key list was
// written, see gopherstack-gt9o) round-trip when supplied and are absent
// from the raw wire body -- not present as null/empty -- when the caller
// never sent them.
func TestPutPlaybackConfiguration_AdsPersonalization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, resp map[string]any)
		body  map[string]any
		name  string
	}{
		{
			name: "supplied",
			body: map[string]any{
				"Name": "cfg-ads",
				"AdsPersonalizationConcurrency": map[string]any{
					"EnableVodVastParallelization": true,
					"MaxConcurrentAdsRequests":     float64(4),
				},
				"AdsPersonalizationTimeouts": map[string]any{
					"AdsRequestTimeoutMilliseconds":                     float64(2500),
					"LiveMaximumAdsPersonalizationTimeMilliseconds":     float64(9000),
					"PrefetchAdsRequestTimeoutMilliseconds":             float64(1500),
					"PrefetchMaximumAdsPersonalizationTimeMilliseconds": float64(8000),
					"VodMaximumAdsPersonalizationTimeMilliseconds":      float64(7000),
				},
			},
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()

				concurrency, ok := resp["AdsPersonalizationConcurrency"].(map[string]any)
				require.True(t, ok, "AdsPersonalizationConcurrency must round-trip")
				assert.Equal(t, true, concurrency["EnableVodVastParallelization"])
				assert.InDelta(t, float64(4), concurrency["MaxConcurrentAdsRequests"], 0.0001)

				timeouts, ok := resp["AdsPersonalizationTimeouts"].(map[string]any)
				require.True(t, ok, "AdsPersonalizationTimeouts must round-trip")
				assert.InDelta(t, float64(2500), timeouts["AdsRequestTimeoutMilliseconds"], 0.0001)
				assert.InDelta(t, float64(9000), timeouts["LiveMaximumAdsPersonalizationTimeMilliseconds"], 0.0001)
			},
		},
		{
			name: "absent",
			body: map[string]any{
				"Name": "cfg-no-ads",
			},
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()

				_, ok := resp["AdsPersonalizationConcurrency"]
				assert.False(t, ok, "AdsPersonalizationConcurrency must be absent, not null/empty, when unset")

				_, ok = resp["AdsPersonalizationTimeouts"]
				assert.False(t, ok, "AdsPersonalizationTimeouts must be absent, not null/empty, when unset")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", tt.body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
			tt.check(t, putResp)

			name, _ := tt.body["Name"].(string)
			rec = doRequest(t, h, http.MethodGet, "/playbackConfiguration/"+name, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			tt.check(t, getResp)
		})
	}
}

// TestPutPlaybackConfiguration_DualStackFieldsAbsent verifies
// DualStackPlaybackEndpointPrefix/DualStackSessionInitializationEndpointPrefix
// (response-only members with no PutPlaybackConfigurationInput counterpart)
// are absent from the wire rather than a fabricated URL -- gopherstack has
// no dual-stack endpoint to report, and an invented one a client might
// actually dial is worse than an absent field.
func TestPutPlaybackConfiguration_DualStackFieldsAbsent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{"Name": "cfg-dualstack"})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))

	rec = doRequest(t, h, http.MethodGet, "/playbackConfiguration/cfg-dualstack", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))

	for _, resp := range []map[string]any{putResp, getResp} {
		_, ok := resp["DualStackPlaybackEndpointPrefix"]
		assert.False(t, ok, "DualStackPlaybackEndpointPrefix must be absent, not a fabricated URL")

		_, ok = resp["DualStackSessionInitializationEndpointPrefix"]
		assert.False(t, ok, "DualStackSessionInitializationEndpointPrefix must be absent, not a fabricated URL")
	}
}

func assertPlaybackConfigExtras(t *testing.T, resp map[string]any) {
	t.Helper()

	avail, ok := resp["AvailSuppression"].(map[string]any)
	require.True(t, ok, "AvailSuppression must round-trip")
	assert.Equal(t, "BEHIND_LIVE_EDGE", avail["Mode"])

	bumper, ok := resp["Bumper"].(map[string]any)
	require.True(t, ok, "Bumper must round-trip")
	assert.Equal(t, "https://example.com/start.mp4", bumper["StartUrl"])

	cdn, ok := resp["CdnConfiguration"].(map[string]any)
	require.True(t, ok, "CdnConfiguration must round-trip")
	assert.Equal(t, "https://cdn.example.com/ads", cdn["AdSegmentUrlPrefix"])

	assert.InDelta(t, float64(10), resp["PersonalizationThresholdSeconds"], 0.0001)
	assert.Equal(t, "https://example.com/slate.mp4", resp["SlateAdUrl"])
}

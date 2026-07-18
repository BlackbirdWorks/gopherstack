package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigureLogsForChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "configure logs returns channel name and log types",
			body: map[string]any{
				"ChannelName": "ch1",
				"LogTypes":    []any{"AS_RUN"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "ch1", resp["ChannelName"])
				logTypes, _ := resp["LogTypes"].([]any)
				assert.Len(t, logTypes, 1)
			},
		},
		{
			name: "configure logs for missing channel returns 404",
			body: map[string]any{
				"ChannelName": "nope",
				"LogTypes":    []any{},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestChannel(t, h)

			rec := doRequest(t, h, http.MethodPut, "/configureLogs/channel", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}
}

func TestConfigureLogsForPlaybackConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, rec *httptest.ResponseRecorder)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "configure logs returns config name and percent",
			body: map[string]any{
				"PlaybackConfigurationName": "pc1",
				"PercentEnabled":            float64(60),
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "pc1", resp["PlaybackConfigurationName"])
				assert.InDelta(t, float64(60), resp["PercentEnabled"], 0.0001)
			},
		},
		{
			name: "configure logs for missing config returns 404",
			body: map[string]any{
				"PlaybackConfigurationName": "nope",
				"PercentEnabled":            float64(0),
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createTestPlaybackConfig(t, h, "pc1")

			rec := doRequest(t, h, http.MethodPut, "/configureLogs/playbackConfiguration", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec)
			}
		})
	}
}

func TestHandleConfigureLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		path     string
		wantCode int
	}{
		{
			name:     "configure logs for channel that exists",
			path:     "/configureLogs/channel",
			body:     map[string]any{"ChannelName": "ch1", "LogTypes": []any{"AS_RUN"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "configure logs for channel not found",
			path:     "/configureLogs/channel",
			body:     map[string]any{"ChannelName": "missing", "LogTypes": []any{"AS_RUN"}},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "configure logs for playback config that exists",
			path:     "/configureLogs/playbackConfiguration",
			body:     map[string]any{"PlaybackConfigurationName": "pc1", "PercentEnabled": float64(50)},
			wantCode: http.StatusOK,
		},
		{
			name:     "configure logs for playback config not found",
			path:     "/configureLogs/playbackConfiguration",
			body:     map[string]any{"PlaybackConfigurationName": "missing", "PercentEnabled": float64(50)},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Set up dependencies for success cases
			if tt.wantCode == http.StatusOK {
				if tt.path == "/configureLogs/channel" {
					doRequest(t, h, http.MethodPost, "/channel/ch1", map[string]any{
						"PlaybackMode": "LINEAR",
					})
				} else {
					doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
						"Name":                  "pc1",
						"AdDecisionServerUrl":   "https://ads.com",
						"VideoContentSourceUrl": "https://video.com",
					})
				}
			}

			rec := doRequest(t, h, http.MethodPut, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

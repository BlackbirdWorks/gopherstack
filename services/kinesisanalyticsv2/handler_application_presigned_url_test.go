package kinesisanalyticsv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func TestKAV2_CreateApplicationPresignedUrl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *kinesisanalyticsv2.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *kinesisanalyticsv2.Handler) {
				doKAV2Request(t, h, "CreateApplication", map[string]any{
					"ApplicationName":    "flink-app",
					"RuntimeEnvironment": "FLINK-1_18",
				})
			},
			body: map[string]any{
				"ApplicationName": "flink-app",
				"UrlType":         "FLINK_DASHBOARD_URL",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "app_not_found",
			body: map[string]any{
				"ApplicationName": "missing-app",
				"UrlType":         "FLINK_DASHBOARD_URL",
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestKAV2Handler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doKAV2Request(t, h, "CreateApplicationPresignedUrl", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				url, ok := out["AuthorizedUrl"].(string)
				require.True(t, ok)
				assert.Contains(t, url, "FLINK_DASHBOARD_URL")
			}
		})
	}
}

// TestKAV2_CreateApplicationPresignedURL_RequiresURLType verifies URLType is
// a required field.
func TestKAV2_CreateApplicationPresignedURL_RequiresURLType(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "url-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "CreateApplicationPresignedUrl", map[string]any{
		"ApplicationName": "url-app",
		// URLType omitted
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestKAV2_GetSupportedOperations_IncludesPresignedURL verifies
// CreateApplicationPresignedUrl is present in GetSupportedOperations().
func TestKAV2_GetSupportedOperations_IncludesPresignedURL(t *testing.T) {
	t.Parallel()

	h := newTestKAV2Handler(t)
	assert.Contains(t, h.GetSupportedOperations(), "CreateApplicationPresignedUrl")
}

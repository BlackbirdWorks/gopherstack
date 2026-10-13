package mediapackage_test

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

func TestRealism_HarvestJobCompletes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantBefore string
		wantAfter  string
		duration   time.Duration
		wait       time.Duration
	}{
		{name: "default", wait: 2 * time.Second, wantBefore: "IN_PROGRESS", wantAfter: "SUCCEEDED"},
		{
			name: "custom", duration: time.Hour, wait: 30 * time.Minute,
			wantBefore: "IN_PROGRESS", wantAfter: "IN_PROGRESS",
		},
		{name: "immediate", wantBefore: "SUCCEEDED", wantAfter: "SUCCEEDED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := mediapackage.NewInMemoryBackend("000000000000", "us-east-1")
				if tt.name != "default" {
					b.SetHarvestJobDuration(tt.duration)
				}

				h := mediapackage.NewHandler(b)
				chID := createTestChannel(t, h)
				createTestOriginEndpointForHarvest(t, h, chID, "ep")

				code, created := doRequestJSON(t, h, http.MethodPost, "/harvest_jobs", map[string]any{
					"id": "job", "originEndpointId": "ep",
					"startTime": "2024-01-01T00:00:00Z", "endTime": "2024-01-01T01:00:00Z",
					"s3Destination": map[string]any{"bucketName": "b", "manifestKey": "m", "roleArn": "r"},
				})
				require.Equal(t, http.StatusCreated, code)
				assert.Equal(t, tt.wantBefore, created["status"])

				time.Sleep(tt.wait)

				_, got := doRequestJSON(t, h, http.MethodGet, "/harvest_jobs/job", nil)
				assert.Equal(t, tt.wantAfter, got["status"])
			})
		})
	}
}

func TestRealism_OriginEndpointDefaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	chID, _ := createChannelWithTags(t, h, "ch", nil)

	code, resp := doRequestJSON(t, h, http.MethodPost, "/origin_endpoints", map[string]any{
		"channelId": chID, "id": "ep", "hlsPackage": map[string]any{},
	})
	require.Equal(t, http.StatusCreated, code)
	assert.InDelta(t, 0, resp["startoverWindowSeconds"], 0)
	assert.InDelta(t, 0, resp["timeDelaySeconds"], 0)
	assert.Contains(t, resp["url"], "/ep/index.m3u8")

	code, resp = doRequestJSON(t, h, http.MethodPut, "/origin_endpoints/ep", map[string]any{"manifestName": "main"})
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, resp["url"], "/ep/main.m3u8")
}

func TestRealism_ErrorMessageNoCodePrefix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, resp := doRequestJSON(t, h, http.MethodGet, "/channels/nope", nil)
	assert.Equal(t, "NotFoundException", resp["__type"])
	assert.Equal(t, `channel "nope" not found`, resp["message"])
}

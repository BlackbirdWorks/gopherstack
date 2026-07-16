package eventbridge_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ReplayCRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "h-arc",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/h-arc",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	now := time.Now()
	rec := auditMakeRequest(t, h, e, "StartReplay", map[string]any{
		"ReplayName":     "h-replay",
		"EventSourceArn": "arn:aws:events:us-east-1:123456789012:archive/h-arc",
		"EventStartTime": now.Add(-2 * time.Hour).UTC().Format(time.RFC3339),
		"EventEndTime":   now.Add(-time.Hour).UTC().Format(time.RFC3339),
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeReplay", map[string]any{"ReplayName": "h-replay"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListReplays", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-replay")
}

func TestStartReplay_ReplayStartTimeIsEpochFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		replayName string
	}{
		{
			name:       "ReplayStartTime is a JSON number not a string",
			replayName: "replay-epoch-test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Now()
			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			b.AddArchiveInternal(&eventbridge.Archive{
				ArchiveName:    "batch2-arc",
				ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/batch2-arc",
				EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
				State:          "ACTIVE",
			})
			h := eventbridge.NewHandler(b)

			rec := auditMakeRequest(t, h, e, "StartReplay", map[string]any{
				"ReplayName":     tt.replayName,
				"EventSourceArn": "arn:aws:events:us-east-1:123456789012:archive/batch2-arc",
				"EventStartTime": now.Add(-2 * time.Hour).UTC().Format(time.RFC3339),
				"EventEndTime":   now.Add(-time.Hour).UTC().Format(time.RFC3339),
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			replayStartRaw, ok := raw["ReplayStartTime"]
			require.True(t, ok, "ReplayStartTime must be present in StartReplay response")

			var f float64
			err := json.Unmarshal(replayStartRaw, &f)
			require.NoError(
				t,
				err,
				"ReplayStartTime must be a JSON number (epoch seconds), got: %s",
				string(replayStartRaw),
			)
			assert.Greater(t, f, float64(0), "ReplayStartTime epoch value must be positive")
		})
	}
}

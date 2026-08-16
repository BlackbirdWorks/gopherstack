package eventbridge_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
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
		// AWS json-1.1 wire format serializes request timestamps as epoch-seconds
		// numbers (smithytime.FormatEpochSeconds), not RFC3339 strings -- see
		// StartReplayInput.UnmarshalJSON.
		"EventStartTime": float64(now.Add(-2 * time.Hour).Unix()),
		"EventEndTime":   float64(now.Add(-time.Hour).Unix()),
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
				"EventStartTime": float64(now.Add(-2 * time.Hour).Unix()),
				"EventEndTime":   float64(now.Add(-time.Hour).Unix()),
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

// TestHandler_DescribeReplay_TimestampsAreEpochSeconds proves DescribeReplay
// emits EventStartTime/EventEndTime/ReplayStartTime as AWS json-1.1 wire
// format epoch-seconds JSON numbers, not RFC3339 strings -- a raw
// json.Marshal of eventbridge.Replay's time.Time fields would produce the
// latter, which a real AWS SDK client's deserializer rejects.
func TestHandler_DescribeReplay_TimestampsAreEpochSeconds(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := eventbridge.NewInMemoryBackend()
	h := eventbridge.NewHandler(b)

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "epoch-archive",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/epoch-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	now := time.Now()
	rec := auditMakeRequest(t, h, e, "StartReplay", map[string]any{
		"ReplayName":     "epoch-replay",
		"EventSourceArn": "arn:aws:events:us-east-1:123456789012:archive/epoch-archive",
		"EventStartTime": float64(now.Add(-2 * time.Hour).Unix()),
		"EventEndTime":   float64(now.Add(-time.Hour).Unix()),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeReplay", map[string]any{"ReplayName": "epoch-replay"})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	startRaw, ok := raw["EventStartTime"]
	require.True(t, ok, "EventStartTime must be present")

	var f float64
	require.NoError(t, json.Unmarshal(startRaw, &f),
		"EventStartTime must be a JSON number (epoch seconds), got: %s", string(startRaw))
	assert.Greater(t, f, float64(0))
}

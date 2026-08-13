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

// TestDescribeEventSourceAndListEventSources_TimestampsAreEpochFloat proves
// DescribeEventSource and ListEventSources emit CreationTime as an AWS
// json-1.1 wire-format epoch-seconds JSON number, not an RFC3339 string --
// both ops used to json.Marshal the backend's *EventSource/[]EventSource
// directly, unlike DescribeEventBus/ListEndpoints/DescribeReplay which
// already convert through an epoch-seconds response DTO.
func TestDescribeEventSourceAndListEventSources_TimestampsAreEpochFloat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
	}{
		{name: "describe event source", action: "DescribeEventSource"},
		{name: "list event sources", action: "ListEventSources"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			h := eventbridge.NewHandler(b)

			b.AddEventSourceInternal(&eventbridge.EventSource{
				Arn:          "arn:aws:events:us-east-1:123456789012:event-source/aws.partner/epoch.test/app",
				Name:         "aws.partner/epoch.test/app",
				State:        "PENDING",
				CreationTime: time.Now(),
			})

			var input map[string]any
			if tt.action == "DescribeEventSource" {
				input = map[string]any{"Name": "aws.partner/epoch.test/app"}
			} else {
				input = map[string]any{}
			}

			rec := auditMakeRequest(t, h, e, tt.action, input)
			require.Equal(t, http.StatusOK, rec.Code)

			var creationRaw json.RawMessage
			if tt.action == "DescribeEventSource" {
				var raw map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
				creationRaw = raw["CreationTime"]
			} else {
				var raw struct {
					EventSources []map[string]json.RawMessage `json:"EventSources"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
				require.Len(t, raw.EventSources, 1)
				creationRaw = raw.EventSources[0]["CreationTime"]
			}

			require.NotNil(t, creationRaw, "CreationTime must be present")

			var f float64
			err := json.Unmarshal(creationRaw, &f)
			require.NoError(t, err, "CreationTime must be a JSON number (epoch seconds), got: %s", string(creationRaw))
			assert.Greater(t, f, float64(0))
		})
	}
}

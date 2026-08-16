package eventbridge_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestEventEntry_UnmarshalJSON_TimeIsEpochSeconds proves EventEntry parses
// the AWS json-1.1 wire format for PutEventsRequestEntry.Time: an
// epoch-seconds JSON number (aws-sdk-go-v2's serializers.go emits it via
// smithytime.FormatEpochSeconds), not an RFC3339 string. Plain time.Time's
// default UnmarshalJSON only accepts a quoted RFC3339 string, so without
// EventEntry.UnmarshalJSON a real AWS SDK client's PutEvents request with an
// explicit Time field would fail to decode entirely.
func TestEventEntry_UnmarshalJSON_TimeIsEpochSeconds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTime time.Time
		name     string
		body     string
		wantNil  bool
		wantErr  bool
	}{
		{
			name:     "whole-second epoch",
			body:     `{"Time":1700000000,"Source":"s","DetailType":"d","Detail":"{}"}`,
			wantTime: time.Unix(1_700_000_000, 0).UTC(),
		},
		{
			name:     "fractional-second epoch",
			body:     `{"Time":1700000000.5,"Source":"s","DetailType":"d","Detail":"{}"}`,
			wantTime: time.Unix(1_700_000_000, 500_000_000).UTC(),
		},
		{
			name:    "absent Time leaves nil",
			body:    `{"Source":"s","DetailType":"d","Detail":"{}"}`,
			wantNil: true,
		},
		{
			name:    "RFC3339 string is rejected, not silently misparsed",
			body:    `{"Time":"2024-01-01T00:00:00Z","Source":"s","DetailType":"d","Detail":"{}"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var entry eventbridge.EventEntry
			err := json.Unmarshal([]byte(tt.body), &entry)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "s", entry.Source)

			if tt.wantNil {
				assert.Nil(t, entry.Time)

				return
			}

			require.NotNil(t, entry.Time)
			assert.True(t, tt.wantTime.Equal(*entry.Time),
				"got %v, want %v", entry.Time, tt.wantTime)
		})
	}
}

// TestStartReplayInput_UnmarshalJSON_TimesAreEpochSeconds proves
// StartReplayInput parses EventStartTime/EventEndTime as AWS json-1.1
// wire-format epoch-seconds JSON numbers, matching aws-sdk-go-v2's
// serializers.go (smithytime.FormatEpochSeconds) -- see
// EventEntry_UnmarshalJSON's doc comment for why this can't be the default
// struct-tag-driven json.Unmarshal.
func TestStartReplayInput_UnmarshalJSON_TimesAreEpochSeconds(t *testing.T) {
	t.Parallel()

	body := `{
		"ReplayName": "r",
		"EventSourceArn": "arn:aws:events:us-east-1:123456789012:archive/a",
		"EventStartTime": 1700000000,
		"EventEndTime": 1700003600,
		"Destination": {"Arn": "arn:aws:events:us-east-1:123456789012:event-bus/default"}
	}`

	var input eventbridge.StartReplayInput
	require.NoError(t, json.Unmarshal([]byte(body), &input))

	assert.True(t, time.Unix(1_700_000_000, 0).UTC().Equal(input.EventStartTime))
	assert.True(t, time.Unix(1_700_003_600, 0).UTC().Equal(input.EventEndTime))
	require.NotNil(t, input.Destination)
	assert.Equal(t, "arn:aws:events:us-east-1:123456789012:event-bus/default", input.Destination.Arn)
}

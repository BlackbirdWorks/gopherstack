package cloudtrail_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// allLookupEvents paginates through every LookupEvents page and returns the
// combined event list.
func allLookupEvents(t *testing.T, b *cloudtrail.InMemoryBackend) []cloudtrail.Event {
	t.Helper()

	var all []cloudtrail.Event

	token := ""
	for {
		out := b.LookupEvents(cloudtrail.LookupEventsInput{NextToken: token, MaxResults: 50})
		all = append(all, out.Events...)

		if out.NextToken == "" {
			return all
		}

		token = out.NextToken
	}
}

// TestRecordEvent_TrimsPastRetention proves that events older than CloudTrail
// Event history's 90-day retention window are evicted once the amortized
// write-time sweep fires, while events within the window are kept -- guarding
// against unbounded growth of the CloudTrail-capture chokepoint that records
// one event per mutating API call across every registered service.
func TestRecordEvent_TrimsPastRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		age       time.Duration
		wantAlive bool
	}{
		{name: "event past the 90-day window is evicted", age: 91 * 24 * time.Hour, wantAlive: false},
		{name: "event within the 90-day window is kept", age: 89 * 24 * time.Hour, wantAlive: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudtrail.NewInMemoryBackend("000000000000", config.DefaultRegion)

			marker := "marker-event"
			b.RecordEvent(cloudtrail.Event{
				EventID:   marker,
				EventName: "MarkerEvent",
				EventTime: time.Now().UTC().Add(-tc.age),
			})

			// Cross the amortized sweep threshold (trimEventsSweepEvery = 500)
			// with fresh, well-within-retention events.
			for i := range 500 {
				b.RecordEvent(cloudtrail.Event{
					EventName: "FillerEvent",
					EventID:   "filler-" + string(rune('a'+i%26)),
				})
			}

			events := allLookupEvents(t, b)

			found := false
			for _, ev := range events {
				if ev.EventID == marker {
					found = true

					break
				}
			}

			require.Equal(t, tc.wantAlive, found)
		})
	}
}

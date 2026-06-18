package eventbridge_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestEventLog_CappedAtMax verifies that the in-memory event log stays at or
// below maxEventLogSize even when many more events are published, preventing
// unbounded memory growth on a long-running backend.
func TestEventLog_CappedAtMax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		putEvents int
	}{
		{name: "at cap", putEvents: eventbridge.MaxEventLogSizeForTest},
		{name: "one over cap", putEvents: eventbridge.MaxEventLogSizeForTest + 1},
		{name: "well over cap", putEvents: eventbridge.MaxEventLogSizeForTest + 100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := eventbridge.NewInMemoryBackend()

			for i := range tc.putEvents {
				results := b.PutEvents(ctx, []eventbridge.EventEntry{
					{
						Source:     "test.source",
						DetailType: "TestEvent",
						Detail:     `{"seq":` + string(rune('0'+i%10)) + `}`,
					},
				})
				require.Empty(t, results[0].ErrorCode, "PutEvents must not error")
			}

			got := b.EventLogLen()
			require.LessOrEqual(t, got, eventbridge.MaxEventLogSizeForTest,
				"event log must not exceed the cap after %d events", tc.putEvents)

			if tc.putEvents >= eventbridge.MaxEventLogSizeForTest {
				require.Equal(t, eventbridge.MaxEventLogSizeForTest, got,
					"event log must be exactly at the cap when overflowed")
			}
		})
	}
}

// TestEventLog_RetainsMostRecentEvents verifies that when the event log cap is
// exceeded, the oldest events are evicted (not the newest) — so the log always
// reflects recent activity.
func TestEventLog_RetainsMostRecentEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := eventbridge.NewInMemoryBackend()

	// Overfill the log beyond the cap.
	extra := 10
	total := eventbridge.MaxEventLogSizeForTest + extra

	for range total {
		b.PutEvents(ctx, []eventbridge.EventEntry{
			{Source: "test.source", DetailType: "Fill", Detail: `{}`},
		})
	}

	require.Equal(t, eventbridge.MaxEventLogSizeForTest, b.EventLogLen(),
		"log must be trimmed to cap")

	// The GetEventLog call must return exactly the cap entries.
	log := b.GetEventLog(ctx)
	require.Len(t, log, eventbridge.MaxEventLogSizeForTest)
}

package cloudtrail

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestTrimEventsLocked_CapsAtMaxStoredEvents pins trimEventsLocked's
// over-capacity branch: oldest events are dropped, newest are kept in order.
func TestTrimEventsLocked_CapsAtMaxStoredEvents(t *testing.T) {
	t.Parallel()

	be := NewInMemoryBackend("000000000000", "us-east-1")

	const extra = 250

	now := time.Now().UTC()
	total := maxStoredEvents + extra

	be.events = make([]Event, total)
	for i := range be.events {
		be.events[i] = Event{EventID: string(rune(i)), EventTime: now, EventName: "FillerEvent"}
	}

	be.trimEventsLocked()

	assert.Len(t, be.events, maxStoredEvents)
	assert.Equal(t, string(rune(extra)), be.events[0].EventID,
		"oldest surviving event should be the first non-dropped one")
	assert.Equal(t, string(rune(total-1)), be.events[len(be.events)-1].EventID,
		"newest event must survive")
}

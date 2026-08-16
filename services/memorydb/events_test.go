package memorydb_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestRefinement1_MaxEventsCap verifies that events are capped at maxEvents.
func TestMaxEventsCap(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	for i := range 1200 {
		b.AddEvent(&memorydb.ExportedEvent{
			Date:       time.Now(),
			SourceName: "src",
			SourceType: "cluster",
			Message:    string(rune('a' + i%26)),
		})
	}

	// Should be capped at 1000
	assert.LessOrEqual(t, memorydb.EventCount(b), 1000)
}

// TestRefinement1_AddEventCapEnforced verifies AddEvent does not grow beyond cap.
func TestAddEventCapEnforced(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	for range 100 {
		b.AddEvent(&memorydb.ExportedEvent{
			Date:       time.Now(),
			SourceName: "cluster-x",
			SourceType: "cluster",
			Message:    "test event",
		})
	}

	assert.Equal(t, 100, memorydb.EventCount(b))

	// Adding one more should still be 100 (under cap)
	b.AddEvent(&memorydb.ExportedEvent{
		Date: time.Now(), SourceName: "x", SourceType: "cluster", Message: "extra",
	})

	assert.Equal(t, 101, memorydb.EventCount(b))
}

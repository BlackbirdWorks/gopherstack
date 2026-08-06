package dynamodb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// TestStopTableTimers_ConcurrentGSIDelete_NoPanic is a regression test for
// stopTableTimers (store.go) iterating table.GlobalSecondaryIndexes without a
// lock, while applyGSICreate/Update/Delete (table_ops.go) and their async
// AfterFunc timer callbacks mutate that same slice under table.mu, including
// shrinking it on GSI delete. Since `for i := range
// table.GlobalSecondaryIndexes` re-reads the slice each iteration, a concurrent
// GSI delete mid-loop shrinks it out from under the iteration and the next
// index access panics. Fixed by making stopTableTimers acquire table.mu itself
// (matching the db.mu -> table.mu order every call site already uses), which
// serializes it against every GSI mutator.
func TestStopTableTimers_ConcurrentGSIDelete_NoPanic(t *testing.T) {
	t.Parallel()

	const attempts = 200
	const gsiCount = 15

	for attempt := range attempts {
		table := &Table{
			Name: "race-table",
			mu:   lockmetrics.New(fmt.Sprintf("test.stopTableTimers.race.%d", attempt)),
		}
		table.GlobalSecondaryIndexes = make([]models.GlobalSecondaryIndex, gsiCount)
		for i := range gsiCount {
			table.GlobalSecondaryIndexes[i] = models.GlobalSecondaryIndex{
				IndexName: fmt.Sprintf("gsi-%d", i),
				// A live (never-firing) timer is enough for stopTableTimers to
				// touch this entry; the real bug is the unsynchronized slice
				// iteration, not what the timer does.
				IndexStatusTimer: time.AfterFunc(time.Hour, func() {}),
			}
		}

		var wg sync.WaitGroup
		wg.Add(2)

		var panicVal any
		var panicMu sync.Mutex

		// Exactly what DeleteTable / runTableCleaner do: call stopTableTimers
		// with no table.mu held by the caller.
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicMu.Lock()
					panicVal = r
					panicMu.Unlock()
				}
			}()
			stopTableTimers(table)
		}()

		// Exactly what applyGSIDelete's timer callback does: shrink the GSI
		// slice under table.mu.
		go func() {
			defer wg.Done()
			for range gsiCount {
				table.mu.Lock("GSIRemove")
				if len(table.GlobalSecondaryIndexes) > 0 {
					table.GlobalSecondaryIndexes = table.GlobalSecondaryIndexes[:len(table.GlobalSecondaryIndexes)-1]
				}
				table.mu.Unlock()
			}
		}()

		wg.Wait()
		table.mu.Close()

		if panicVal != nil {
			t.Fatalf(
				"attempt %d: stopTableTimers panicked concurrently with a GSI delete: %v "+
					"(regression: stopTableTimers must hold table.mu for its whole body)",
				attempt, panicVal,
			)
		}
	}
}

package dynamodb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// TestStopTableTimers_ConcurrentGSIDelete_NoPanic is a regression test for a
// genuine `panic: runtime error: index out of range` in stopTableTimers
// (store.go), reachable from two call sites that both hold no table.mu at the
// time of the call: DeleteTable (table_ops.go) and the janitor's
// runTableCleaner (janitor.go, run from the real background worker started
// by DynamoDBHandler.StartWorker). stopTableTimers used to iterate
// table.GlobalSecondaryIndexes without any lock, while
// applyGSICreate/applyGSIUpdate/applyGSIDelete (table_ops.go) -- and their
// async GSI-activation/-removal AfterFunc timer callbacks -- mutate that same
// slice under table.mu, including shrinking it (GSI delete). Because the
// `for i := range table.GlobalSecondaryIndexes` loop re-reads the slice on
// every iteration rather than caching it, a concurrent GSI delete landing
// mid-loop shrinks the backing slice out from under the iteration and the
// next index access panics.
//
// Under the real HTTP handler this crashed whichever request happened to be
// running DeleteTable (recovered by the top-level PanicRecovery middleware as
// an HTTP 500) or, when hit from the janitor, aborted the rest of that
// runTableCleaner pass (recovered by pkgs/worker.Group, logged as a
// "TableCleaner"/"panic" task) -- in both cases silently corrupting/losing
// timer cleanup for any tables queued after the one that panicked.
//
// The fix makes stopTableTimers acquire table.mu itself (matching the
// db.mu -> table.mu order already used by every call site, since none of them
// hold table.mu when calling in), which serializes it against every GSI
// mutator. This test exercises exactly the interleaving that used to panic --
// looping enough attempts that, pre-fix, it reproduces on the first handful of
// iterations essentially every run; post-fix it always completes cleanly.
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

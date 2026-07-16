package sns

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
)

// Handler is the Echo HTTP handler for SNS operations.
type snsActionFn func(c *echo.Context) error

// fifoDedupTTL is the deduplication window for FIFO topic messages per AWS specification.
const fifoDedupTTL = 5 * time.Minute

// fifoDedupMaxEntries caps the in-memory deduplication map to bound memory growth
// for high-cardinality dedup IDs. When exceeded, all expired entries are evicted
// before the new one is inserted, regardless of the opportunistic sweep cadence.
const fifoDedupMaxEntries = 100_000

// fifoDeduplication tracks message deduplication IDs with a TTL for FIFO topics.
// insertOrder records keys in insertion order; since all entries share the same TTL,
// the oldest entry is always at insertOrder[insertHead], enabling O(1) amortized eviction.
type fifoDeduplication struct {
	entries     map[string]time.Time // dedupKey → expiry
	insertOrder []string             // keys in insertion order
	insertHead  int                  // index of the first live entry in insertOrder
	mu          sync.Mutex
}

// fifoDedupSweepInterval is the cadence at which the background goroutine
// evicts expired entries from the deduplication map. This supplements the
// opportunistic eviction inside isDuplicate/record and ensures that a
// long-idle FIFO topic does not retain stale entries indefinitely.
const fifoDedupSweepInterval = time.Minute

func newFifoDeduplication() *fifoDeduplication {
	return &fifoDeduplication{
		entries:     make(map[string]time.Time),
		insertOrder: make([]string, 0, fifoDedupMaxEntries),
	}
}

// startPeriodicSweep launches a background goroutine that evicts expired
// entries at fifoDedupSweepInterval. The goroutine stops when ctx is cancelled.
func (d *fifoDeduplication) startPeriodicSweep(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(fifoDedupSweepInterval)
		defer ticker.Stop()

		for {
			select {
			case now := <-ticker.C:
				d.mu.Lock()
				d.sweepExpiredLocked(now)
				d.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
}

// check returns true if the dedup ID has already been seen within the TTL window,
// and records it otherwise.
// isDuplicate returns true if dedupID was already seen within the TTL window.
// It does NOT record the ID — call record() after a successful publish.
// Expired entries are intentionally not swept here; the background goroutine
// and the capacity-triggered sweep in record() maintain memory bounds.
// The correctness check now.Before(exp) already returns false for expired entries,
// so an O(n) sweep on every publish would be pure overhead.
func (d *fifoDeduplication) isDuplicate(topicArn, dedupID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	key := topicArn + "/" + dedupID
	exp, found := d.entries[key]

	return found && now.Before(exp)
}

// record marks dedupID as seen for the given topic ARN.
func (d *fifoDeduplication) record(topicArn, dedupID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()

	// Bound memory: when at the cap, force a sweep before insertion so a workload
	// with many short-lived dedup IDs cannot grow without bound.
	if len(d.entries) >= fifoDedupMaxEntries {
		d.sweepExpiredLocked(now)

		// If a burst of unique-but-still-fresh dedup IDs filled the map, fall
		// back to evicting the entry with the earliest expiration so insertions
		// remain bounded. AWS only guarantees a 5-minute window, so dropping
		// the soonest-to-expire entry is acceptable.
		if len(d.entries) >= fifoDedupMaxEntries {
			d.evictEarliestLocked()
		}
	}

	key := topicArn + "/" + dedupID
	if _, exists := d.entries[key]; !exists {
		d.insertOrder = append(d.insertOrder, key)
	}
	d.entries[key] = now.Add(fifoDedupTTL)
}

// sweepExpiredLocked removes all expired entries. Caller must hold d.mu.
func (d *fifoDeduplication) sweepExpiredLocked(now time.Time) {
	for k, exp := range d.entries {
		if now.After(exp) {
			delete(d.entries, k)
		}
	}
}

// evictEarliestLocked drops the single entry with the earliest expiration from the map.
// Because all entries share the same fifoDedupTTL, insertOrder is effectively sorted
// by expiry time. Scanning from insertHead is therefore O(1) amortised: each key is
// appended once and consumed at most once. Caller must hold d.mu.
func (d *fifoDeduplication) evictEarliestLocked() {
	for d.insertHead < len(d.insertOrder) {
		key := d.insertOrder[d.insertHead]
		d.insertOrder[d.insertHead] = "" // release the string for GC
		d.insertHead++
		if _, exists := d.entries[key]; exists {
			delete(d.entries, key)
			// Compact the slice once the dead prefix exceeds the capacity to
			// prevent unbounded growth of the backing array.
			if d.insertHead > fifoDedupMaxEntries {
				d.insertOrder = append(d.insertOrder[:0], d.insertOrder[d.insertHead:]...)
				d.insertHead = 0
			}

			return
		}
	}
	// insertOrder exhausted (all entries were swept); reset to reclaim memory.
	d.insertOrder = d.insertOrder[:0]
	d.insertHead = 0
}

// nextFIFOSeqNum returns the next 20-digit zero-padded FIFO sequence number for
// the given topic ARN. Sequence numbers are monotonically increasing and unique
// within each topic, matching the shape returned by AWS SNS FIFO topics.
func (h *Handler) nextFIFOSeqNum(topicArn string) string {
	v, _ := h.fifoSeqNums.LoadOrStore(topicArn, new(atomic.Int64))
	counter, ok := v.(*atomic.Int64)
	if !ok {
		return fmt.Sprintf("%020d", 0)
	}
	n := counter.Add(1)

	return fmt.Sprintf("%020d", n)
}

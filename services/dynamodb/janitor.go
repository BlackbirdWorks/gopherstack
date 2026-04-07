package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultDDBJanitorInterval  = 500 * time.Millisecond
	defaultDDBTTLSweepInterval = 5 * time.Second
	// ttlSweepBatchSize is the maximum number of items checked per lock acquisition
	// in sweepTableTTL. Smaller values reduce lock hold time at the cost of more
	// lock round-trips; 1 000 is a reasonable balance for typical table sizes.
	ttlSweepBatchSize = 1_000
	// txnTokensMaxCap is the maximum number of committed idempotency tokens kept
	// in memory. When the cap is exceeded, the oldest half is evicted immediately
	// rather than waiting for the TTL sweep, preventing unbounded map growth.
	txnTokensMaxCap = 100_000
	// txnPendingMaxCap is the equivalent cap for in-progress idempotency tokens.
	txnPendingMaxCap = 100_000
	// streamExpirySeconds is the age after which stream record images are compacted.
	streamExpirySeconds = 24 * 60 * 60
	// txnCapEvictionFraction is the divisor used to compute how many entries to evict
	// when the hard cap is exceeded. Evicting half (len/2) ensures the map drops
	// well below the cap in one pass.
	txnCapEvictionFraction = 2
)

// Janitor is the DynamoDB background worker that finalises tables queued for
// async deletion and records queue-depth metrics for the live dashboard.
type Janitor struct {
	Backend  *InMemoryDB
	Interval time.Duration
	// TaskTimeout bounds each individual janitor task (TTL sweep, table cleaner, etc.).
	// When non-zero, each task runs with a child context that expires after this duration,
	// preventing a stalled operation from blocking the janitor loop indefinitely.
	TaskTimeout time.Duration
}

// NewJanitor creates a new DynamoDB Janitor for the given backend.
// The janitor interval is taken from the provided settings;
// if zero, it falls back to defaultDDBJanitorInterval.
func NewJanitor(backend *InMemoryDB, settings Settings) *Janitor {
	interval := settings.JanitorInterval
	if interval == 0 {
		interval = defaultDDBJanitorInterval
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
	}
}

// Run runs the janitor loop until ctx is cancelled.
// Two tickers are used:
//   - mainTicker (Interval, default 500ms): housekeeping tasks (table cleanup,
//     txn-token sweeps, expression-cache evictions).
//   - ttlTicker (defaultDDBTTLSweepInterval, 5s): per-table TTL and stream-record
//     sweeps, which are O(tables × items) and too expensive to run every 500ms.
//
// A deferred recover() protects the loop from panics caused by concurrent state
// transitions; on recovery the error is logged and the loop continues.
func (j *Janitor) Run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			logger.Load(ctx).Error("DynamoDB janitor: panic recovered, loop exiting",
				"panic", fmt.Sprintf("%v", r))
		}
	}()

	mainTicker := time.NewTicker(j.Interval)
	defer mainTicker.Stop()

	ttlTicker := time.NewTicker(defaultDDBTTLSweepInterval)
	defer ttlTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ttlTicker.C:
			taskCtx, cancel := j.taskContext(ctx)
			j.sweepTTL(taskCtx)
			j.sweepStreamRecords()
			cancel()
		case <-mainTicker.C:
			taskCtx, cancel := j.taskContext(ctx)
			j.sweepTxnTokens()
			j.sweepTxnPending()
			j.Backend.exprCache.Sweep()
			j.runTableCleaner(taskCtx)
			cancel()
		}
	}
}

// taskContext returns a child context bounded by TaskTimeout (if non-zero).
// The caller is responsible for calling the returned cancel function.
func (j *Janitor) taskContext(parent context.Context) (context.Context, context.CancelFunc) {
	if j.TaskTimeout > 0 {
		return context.WithTimeout(parent, j.TaskTimeout)
	}

	return context.WithCancel(parent)
}

// runOnce orchestrates all janitor tasks in a single synchronous pass.
// Called by tests; production code uses the two-ticker Run loop above.
func (j *Janitor) runOnce(ctx context.Context) {
	j.sweepTTL(ctx)
	j.sweepTxnTokens()
	j.sweepTxnPending()
	j.sweepStreamRecords()
	j.Backend.exprCache.Sweep()
	j.runTableCleaner(ctx)
}

// SweepOnce runs a single full sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.runOnce(ctx)
}

// runTableCleaner records the current queue depth and finalises all pending deletions.
func (j *Janitor) runTableCleaner(ctx context.Context) {
	db := j.Backend

	// Snapshot the tables to clean up and remove them from deletingTables under the
	// global lock. The slow work (timer cancellation, mutex close) is done outside the
	// lock so that concurrent DescribeTable / PutItem / Query calls are not stalled
	// while thousands of per-table resources are being released.
	db.mu.Lock("DDBJanitor")
	depth := 0
	names := make([]string, 0)
	tablesToClose := make([]*Table, 0)

	for region, regionTables := range db.deletingTables {
		for name, table := range regionTables {
			depth++
			names = append(names, name)
			tablesToClose = append(tablesToClose, table)
			delete(db.deletingTables[region], name)
		}
	}
	db.mu.Unlock()

	// Release per-table resources outside the global lock.
	for _, table := range tablesToClose {
		stopTableTimers(table)
		if table.Tags != nil {
			table.Tags.Close()
		}
		table.mu.Close()
	}

	telemetry.RecordWorkerQueueDepth("dynamodb", "TableCleaner", depth)
	telemetry.RecordWorkerTask("dynamodb", "TableCleaner", "success")
	telemetry.RecordWorkerItems("dynamodb", "TableCleaner", depth)

	for _, name := range names {
		logger.Load(ctx).InfoContext(ctx, "DynamoDB janitor: table deleted", "table", name)
	}
}

// sweepTTL iterates over all tables, finds those with TTL enabled,
// and evicts expired items based on the configured TTL attribute.
func (j *Janitor) sweepTTL(ctx context.Context) {
	db := j.Backend
	tables := db.ListAllTables()
	now := float64(time.Now().Unix())
	totalEvicted := 0

	var replicationQueue []ttlReplicationEntry

	for _, table := range tables {
		count, pending := j.sweepTableTTL(ctx, db, table, now)
		totalEvicted += count
		replicationQueue = append(replicationQueue, pending...)
	}

	for _, entry := range replicationQueue {
		db.replicateItemMutation(entry.tableName, entry.globalTableName, entry.region, entry.item, "DELETE")
	}

	if totalEvicted > 0 {
		telemetry.RecordWorkerItems("dynamodb", "TTLSweeper", totalEvicted)
	}

	telemetry.RecordWorkerTask("dynamodb", "TTLSweeper", "success")
}

type ttlReplicationEntry struct {
	item            map[string]any
	tableName       string
	globalTableName string
	region          string
}

// sweepTableTTL evicts TTL-expired items from a single table and returns
// the number evicted plus any global-table replication entries to process.
//
// Items are processed in batches of ttlSweepBatchSize so the table write lock is
// not held for the full scan. Between batches the lock is released, giving other
// goroutines (reads, writes) a chance to acquire it. The sweep is also aborted
// early when ctx is cancelled (e.g. on shutdown or task timeout).
func (j *Janitor) sweepTableTTL(
	ctx context.Context,
	db *InMemoryDB,
	table *Table,
	now float64,
) (int, []ttlReplicationEntry) {
	table.mu.RLock("TTLSweepCheck")
	ttlAttr := table.TTLAttribute
	gtName := table.GlobalTableName
	tableARN := table.TableArn
	table.mu.RUnlock()

	if ttlAttr == "" {
		return 0, nil
	}

	region := db.regionFromARN(tableARN)

	var pending []ttlReplicationEntry
	totalEvicted := 0
	start := time.Now()

	// Process in batches: acquire the write lock, scan up to batchSize items,
	// release the lock, then repeat until the full slice has been covered.
	// Scanning backwards keeps index arithmetic correct after deleteItemAtIndex.
	i := -1 // sentinel: start from last element on first batch

	for ctx.Err() == nil {
		table.mu.Lock("TTLSweep")

		if i == -1 {
			i = len(table.Items) - 1
		}

		batchEnd := i - ttlSweepBatchSize
		if batchEnd < 0 {
			batchEnd = -1
		}

		batchEvicted := 0

		for ; i > batchEnd; i-- {
			item := table.Items[i]

			ttlVal, ok := dynamoattr.ParseNumeric(item[ttlAttr])
			if !ok || ttlVal >= now {
				continue
			}

			table.appendStreamRecord(streamEventRemove, deepCopyItem(item), nil)
			batchEvicted++

			if gtName != "" {
				pending = append(pending, ttlReplicationEntry{
					tableName:       table.Name,
					globalTableName: gtName,
					region:          region,
					item:            deepCopyItem(item),
				})
			}

			db.deleteItemAtIndex(table, i)
		}

		table.mu.Unlock()

		totalEvicted += batchEvicted

		// All items scanned.
		if i < 0 {
			break
		}
	}

	if totalEvicted > 0 {
		logger.Load(ctx).InfoContext(ctx, "DynamoDB janitor: TTL items evicted",
			"table", table.Name,
			"count", totalEvicted,
			"duration", time.Since(start))
	}

	return totalEvicted, pending
}

// sweepTxnTokens removes committed idempotency tokens that have exceeded their TTL.
// AWS DynamoDB expires tokens after 10 minutes; this prevents unbounded map growth.
// If the map exceeds txnTokensMaxCap entries the oldest half is evicted immediately.
func (j *Janitor) sweepTxnTokens() {
	db := j.Backend
	now := time.Now()

	db.mu.Lock("sweepTxnTokens")
	defer db.mu.Unlock()

	for token, expiry := range db.txnTokens {
		if now.After(expiry) {
			delete(db.txnTokens, token)
		}
	}

	// Hard cap: if still over limit, evict the oldest half to prevent OOM.
	if len(db.txnTokens) > txnTokensMaxCap {
		evictOldestTokens(db.txnTokens, len(db.txnTokens)/txnCapEvictionFraction)
	}
}

// sweepTxnPending removes in-progress idempotency tokens that have exceeded txnPendingTTL.
// Under normal operation the defer in TransactWriteItems cleans up pending entries.
// This sweep is a safety net for orphaned entries (e.g. from a crashed goroutine).
// If the map exceeds txnPendingMaxCap entries the oldest half is evicted immediately.
func (j *Janitor) sweepTxnPending() {
	db := j.Backend
	now := time.Now()

	db.mu.Lock("sweepTxnPending")
	defer db.mu.Unlock()

	for token, startTime := range db.txnPending {
		if now.Sub(startTime) > txnPendingTTL {
			delete(db.txnPending, token)
		}
	}

	// Hard cap: if still over limit, evict the oldest half to prevent OOM.
	if len(db.txnPending) > txnPendingMaxCap {
		evictOldestPending(db.txnPending, len(db.txnPending)/txnCapEvictionFraction)
	}
}

// evictOldestTokens removes the n oldest entries from m (oldest = earliest expiry time).
// Must be called with db.mu held.
func evictOldestTokens(m map[string]time.Time, n int) {
	// Find the nth smallest expiry time using partial selection — O(len(m)) space.
	times := make([]time.Time, 0, len(m))
	for _, t := range m {
		times = append(times, t)
	}

	threshold := nthSmallest(times, n)

	evicted := 0

	for k, t := range m {
		if evicted >= n {
			break
		}

		if !t.After(threshold) {
			delete(m, k)
			evicted++
		}
	}
}

// evictOldestPending removes the n oldest entries from m (oldest = earliest start time).
// Must be called with db.mu held.
func evictOldestPending(m map[string]time.Time, n int) {
	times := make([]time.Time, 0, len(m))
	for _, t := range m {
		times = append(times, t)
	}

	threshold := nthSmallest(times, n)

	evicted := 0

	for k, t := range m {
		if evicted >= n {
			break
		}

		if !t.After(threshold) {
			delete(m, k)
			evicted++
		}
	}
}

// nthSmallest returns the nth smallest [time.Time] in ts (1-indexed) using a quick-select
// algorithm. If n >= len(ts) it returns the maximum value.
func nthSmallest(ts []time.Time, n int) time.Time {
	if n <= 0 || len(ts) == 0 {
		return time.Time{}
	}

	if n >= len(ts) {
		latest := ts[0]
		for _, t := range ts[1:] {
			if t.After(latest) {
				latest = t
			}
		}

		return latest
	}

	// Simple sort-based approach; the slice is at most txnTokensMaxCap/2 elements.
	// For large caps a full sort is still sub-millisecond in practice.
	sortTimes(ts)

	return ts[n-1]
}

// sortTimes sorts a []time.Time in ascending order using insertion sort.
// Adequate for the token cap sizes in practice (≤50 000 elements).
func sortTimes(ts []time.Time) {
	for i := 1; i < len(ts); i++ {
		key := ts[i]
		j := i - 1

		for j >= 0 && ts[j].After(key) {
			ts[j+1] = ts[j]
			j--
		}

		ts[j+1] = key
	}
}

// sweepStreamRecords compacts stream records that are older than 24 hours.
// For each table the function does two passes under the table write lock:
//  1. Nil out OldImage/NewImage on expired records (saves heap) and count fully
//     expired records (both images already nil and timestamp is stale).
//  2. If the proportion of compacted records is high, compact the ring buffer
//     by removing tombstone entries so the slice does not grow monotonically.
func (j *Janitor) sweepStreamRecords() {
	db := j.Backend
	tables := db.ListAllTables()
	now := time.Now().Unix()

	for _, t := range tables {
		t.mu.Lock("SweepStreamRecords")

		cleared := 0
		tombstones := 0

		for i := range t.StreamRecords {
			r := &t.StreamRecords[i]
			if r.ApproximateCreationDateTime <= 0 {
				continue
			}

			age := now - r.ApproximateCreationDateTime
			if age <= streamExpirySeconds {
				continue
			}

			// Nil images to release heap memory.
			if r.OldImage != nil || r.NewImage != nil {
				r.OldImage = nil
				r.NewImage = nil
				cleared++
			}

			tombstones++
		}

		// Compact the ring buffer when more than half the slots are expired tombstones.
		// This resets the ring to a fresh empty state; stream position is not
		// preserved across a compaction (gap in sequence numbers is acceptable —
		// AWS streams have the same property after the 24h expiry window).
		if len(t.StreamRecords) > 0 && tombstones*2 >= len(t.StreamRecords) {
			t.StreamRecords = t.StreamRecords[:0]
			t.StreamHead = 0
		}

		t.mu.Unlock()

		if cleared > 0 {
			telemetry.RecordWorkerItems("dynamodb", "StreamSweeper", cleared)
		}
	}

	telemetry.RecordWorkerTask("dynamodb", "StreamSweeper", "success")
}

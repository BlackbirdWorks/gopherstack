package dynamodb

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

const (
	defaultDDBJanitorInterval  = 500 * time.Millisecond
	defaultDDBTTLSweepInterval = 5 * time.Second
	// defaultTTLSweepBatchSize is the maximum number of items checked per lock acquisition
	// in sweepTableTTL. Smaller values reduce lock hold time at the cost of more
	// lock round-trips; 1 000 is a reasonable balance for typical table sizes.
	defaultTTLSweepBatchSize = 1_000
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
	// ttlSweepBatchSize is the maximum number of items checked per lock
	// acquisition when sweeping TTL-expired items.
	ttlSweepBatchSize int
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

	ttlSweepBatchSize := settings.TTLSweepBatchSize
	if ttlSweepBatchSize <= 0 {
		ttlSweepBatchSize = defaultTTLSweepBatchSize
	}

	return &Janitor{
		Backend:           backend,
		Interval:          interval,
		ttlSweepBatchSize: ttlSweepBatchSize,
	}
}

// Run runs the janitor loop until ctx is cancelled.
// Two independent tickers are used:
//   - the main ticker (Interval, default 500ms): housekeeping tasks (table
//     cleanup, txn-token sweeps, expression-cache evictions).
//   - the TTL ticker (defaultDDBTTLSweepInterval, 5s): per-table TTL and
//     stream-record sweeps, which are O(tables × items) and too expensive to run
//     every 500ms.
//
// Each sweep is panic-recovered and bounded by TaskTimeout (if non-zero) by the
// worker primitive.
func (j *Janitor) Run(ctx context.Context) {
	worker.RunTickerN(ctx, "dynamodb",
		worker.Sweep{
			Component: "TableCleaner",
			Interval:  j.Interval,
			Timeout:   j.TaskTimeout,
			Fn:        j.sweepMain,
		},
		worker.Sweep{
			Component: "TTLSweeper",
			Interval:  defaultDDBTTLSweepInterval,
			Timeout:   j.TaskTimeout,
			Fn:        j.sweepTTLAndStreams,
		},
	)
}

// sweepMain runs the housekeeping pass: txn-token/pending sweeps, cache
// evictions, and finalising tables queued for deletion.
func (j *Janitor) sweepMain(ctx context.Context) {
	j.sweepTxnTokens(ctx)
	j.sweepTxnPending(ctx)
	j.Backend.exprCache.Sweep()
	j.Backend.iteratorStore.Sweep()
	j.runTableCleaner(ctx)
}

// sweepTTLAndStreams runs the per-table TTL eviction and stream-record
// compaction pass.
func (j *Janitor) sweepTTLAndStreams(ctx context.Context) {
	j.sweepTTL(ctx)
	j.sweepStreamRecords(ctx)
}

// runOnce orchestrates all janitor tasks in a single synchronous pass.
// Called by tests; production code uses the two-ticker Run loop above.
func (j *Janitor) runOnce(ctx context.Context) {
	j.sweepTTL(ctx)
	j.sweepTxnTokens(ctx)
	j.sweepTxnPending(ctx)
	j.sweepStreamRecords(ctx)
	j.Backend.exprCache.Sweep()
	j.Backend.iteratorStore.Sweep()
	j.snapshotPITRTables(ctx)
	j.runTableCleaner(ctx)
}

// snapshotPITRTables captures a point-in-time snapshot of every PITR-enabled
// table's items into its per-table ring buffer. Restore uses these to honour
// RestoreDateTime. Iteration is under db.mu read lock; the per-table read
// happens with table.mu so writers don't see a torn copy.
func (j *Janitor) snapshotPITRTables(_ context.Context) {
	db := j.Backend

	db.mu.RLock("DDBJanitor.snapshotPITR")
	tables := make([]*Table, 0, len(db.Tables))
	for _, regionTables := range db.Tables {
		for _, t := range regionTables {
			tables = append(tables, t)
		}
	}
	db.mu.RUnlock()

	now := time.Now().UTC()

	for _, t := range tables {
		t.mu.Lock("snapshotPITR")
		if !t.PITREnabled {
			t.mu.Unlock()

			continue
		}

		snap := pitrSnapshot{Taken: now, Items: deepCopyItems(t.Items)}
		t.pitrSnapshots = append(t.pitrSnapshots, snap)
		if len(t.pitrSnapshots) > maxPITRSnapshots {
			t.pitrSnapshots = t.pitrSnapshots[len(t.pitrSnapshots)-maxPITRSnapshots:]
		}
		t.mu.Unlock()
	}
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
	tablesToClose := make([]*Table, 0, len(db.deletingTables))

	for region, regionTables := range db.deletingTables {
		for name, table := range regionTables {
			depth++
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

	for _, table := range tablesToClose {
		logger.Load(ctx).InfoContext(ctx, "DynamoDB janitor: table deleted", "table", table.Name)
	}
}

// sweepTTL iterates over all tables, finds those with TTL enabled,
// and evicts expired items based on the configured TTL attribute.
func (j *Janitor) sweepTTL(ctx context.Context) {
	db := j.Backend
	tables := db.ListAllTables()
	now := float64(time.Now().Unix())
	totalEvicted := 0

	replicationQueue := make([]ttlReplicationEntry, 0, len(tables))

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

		// Clamp i in case concurrent deletes shrank table.Items between batches.
		if n := len(table.Items) - 1; i > n {
			i = n
		}

		if i == -1 {
			i = len(table.Items) - 1
		}

		batchEnd := i - j.ttlSweepBatchSize
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

			// Copy the item once; the stream record and replication entry each
			// need their own copy so they can be mutated independently.
			itemCopy := deepCopyItem(item)
			table.appendStreamRecord(streamEventRemove, itemCopy, nil)
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
//
// Optimised two-phase sweep: expired keys are identified under a read lock (allowing
// concurrent reads to proceed), then deleted under a write lock. The write lock is
// only acquired when there is actual work to do, keeping contention minimal on the
// common "nothing expired" path.
func (j *Janitor) sweepTxnTokens(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	db := j.Backend
	now := time.Now()

	// Phase 1: identify expired keys under read lock.
	db.mu.RLock("sweepTxnTokens.scan")
	var expired []string

	for token, expiry := range db.txnTokens {
		if now.After(expiry) {
			expired = append(expired, token)
		}
	}

	capExceeded := len(db.txnTokens) > txnTokensMaxCap
	db.mu.RUnlock()

	// Fast path: nothing to do.
	if len(expired) == 0 && !capExceeded {
		return
	}

	// Phase 2: apply deletions under write lock.
	db.mu.Lock("sweepTxnTokens.delete")
	defer db.mu.Unlock()

	for _, token := range expired {
		delete(db.txnTokens, token)
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
//
// Uses the same two-phase snapshot approach as sweepTxnTokens.
func (j *Janitor) sweepTxnPending(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	db := j.Backend
	now := time.Now()

	// Phase 1: identify stale keys under read lock.
	db.mu.RLock("sweepTxnPending.scan")
	var stale []string

	for token, startTime := range db.txnPending {
		if now.Sub(startTime) > txnPendingTTL {
			stale = append(stale, token)
		}
	}

	capExceeded := len(db.txnPending) > txnPendingMaxCap
	db.mu.RUnlock()

	// Fast path: nothing to do.
	if len(stale) == 0 && !capExceeded {
		return
	}

	// Phase 2: apply deletions under write lock.
	db.mu.Lock("sweepTxnPending.delete")
	defer db.mu.Unlock()

	for _, token := range stale {
		delete(db.txnPending, token)
	}

	// Hard cap: if still over limit, evict the oldest half to prevent OOM.
	if len(db.txnPending) > txnPendingMaxCap {
		evictOldestPending(db.txnPending, len(db.txnPending)/txnCapEvictionFraction)
	}
}

// evictOldestTokens removes the n oldest entries from m (oldest = earliest expiry time).
// Must be called with db.mu held.
func evictOldestTokens(m map[string]time.Time, n int) {
	if n <= 0 {
		return
	}

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
	if n <= 0 {
		return
	}

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

// nthSmallest returns the nth smallest [time.Time] in ts (1-indexed) using a
// Floyd-Rivest / introselect-style quickselect algorithm — O(n) average, O(n²)
// worst case. For the token-eviction call site n is always len(ts)/2, so the
// average complexity is the relevant bound.
//
// If n >= len(ts) it returns the maximum value. ts is mutated in place; callers
// must not rely on its order after the call.
func nthSmallest(ts []time.Time, n int) time.Time {
	if n <= 0 || len(ts) == 0 {
		return time.Time{}
	}

	if n >= len(ts) {
		return sliceMax(ts)
	}

	// Quickselect: partition around a pivot until the kth element is in place.
	// k is 0-indexed here (n is 1-indexed from the caller).
	k := n - 1
	lo, hi := 0, len(ts)-1

	for lo < hi {
		p := partition(ts, lo, hi)

		if p <= k {
			lo = p + 1
		}

		if p >= k {
			hi = p - 1
		}
	}

	return ts[k]
}

// sliceMax returns the largest time.Time in ts. ts must not be empty.
func sliceMax(ts []time.Time) time.Time {
	latest := ts[0]

	for _, t := range ts[1:] {
		if t.After(latest) {
			latest = t
		}
	}

	return latest
}

// partition runs a Hoare-variant partition on ts[lo..hi] using a median-of-three
// pivot. Returns the final pivot index after partitioning.
// Extracted from nthSmallest to keep per-function cognitive complexity below 20.
func partition(ts []time.Time, lo, hi int) int {
	// minPartitionWindow is the window size below which sortThree has already fully
	// sorted the elements and further pivot placement is unnecessary.
	const minPartitionWindow = 2

	mid := lo + (hi-lo)/minPartitionWindow
	sortThree(ts, lo, mid, hi)

	// Window of 2 or fewer elements is already sorted by sortThree; return mid.
	if hi-lo < minPartitionWindow {
		return mid
	}

	pivot := ts[mid]

	// Place pivot at hi-1 so both scan directions can proceed inward.
	ts[mid], ts[hi-1] = ts[hi-1], ts[mid]

	i, j := lo, hi-1

	for {
		i++
		for ts[i].Before(pivot) {
			i++
		}

		j--
		for pivot.Before(ts[j]) {
			j--
		}

		if i >= j {
			break
		}

		ts[i], ts[j] = ts[j], ts[i]
	}

	ts[i], ts[hi-1] = ts[hi-1], ts[i]

	return i
}

// sortThree sorts the three elements at positions a, b, c in ts into ascending order.
func sortThree(ts []time.Time, a, b, c int) {
	if ts[c].Before(ts[a]) {
		ts[a], ts[c] = ts[c], ts[a]
	}

	if ts[b].Before(ts[a]) {
		ts[a], ts[b] = ts[b], ts[a]
	}

	if ts[c].Before(ts[b]) {
		ts[b], ts[c] = ts[c], ts[b]
	}
}

// sweepStreamRecords compacts stream records that are older than 24 hours.
// For each table the function does two passes under the table write lock:
//  1. Nil out OldImage/NewImage on expired records (saves heap) and count fully
//     expired records (both images already nil and timestamp is stale).
//  2. If the proportion of compacted records is high, compact the ring buffer
//     by removing tombstone entries so the slice does not grow monotonically.
func (j *Janitor) sweepStreamRecords(ctx context.Context) {
	db := j.Backend
	tables := db.ListAllTables()
	now := time.Now().Unix()

	for _, t := range tables {
		if ctx.Err() != nil {
			return
		}

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
		// Allocate a fresh slice so the GC can reclaim the old backing array immediately
		// (unlike [:0] which retains the backing array).
		if len(t.StreamRecords) > 0 && tombstones*2 >= len(t.StreamRecords) {
			t.StreamRecords = make([]models.StreamRecord, 0, maxStreamRecords)
			t.StreamHead = 0
		}

		t.mu.Unlock()

		if cleared > 0 {
			telemetry.RecordWorkerItems("dynamodb", "StreamSweeper", cleared)
		}
	}

	telemetry.RecordWorkerTask("dynamodb", "StreamSweeper", "success")
}

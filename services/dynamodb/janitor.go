package dynamodb

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultDDBJanitorInterval  = 500 * time.Millisecond
	defaultDDBTTLSweepInterval = 5 * time.Second
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
func (j *Janitor) Run(ctx context.Context) {
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

	table.mu.Lock("TTLSweep")
	defer table.mu.Unlock()

	var pending []ttlReplicationEntry
	evicted := 0

	for i := len(table.Items) - 1; i >= 0; i-- {
		item := table.Items[i]

		ttlVal, ok := dynamoattr.ParseNumeric(item[ttlAttr])
		if !ok || ttlVal >= now {
			continue
		}

		table.appendStreamRecord(streamEventRemove, deepCopyItem(item), nil)
		evicted++

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

	if evicted > 0 {
		logger.Load(ctx).InfoContext(ctx, "DynamoDB janitor: TTL items evicted",
			"table", table.Name,
			"count", evicted)
	}

	return evicted, pending
}

// sweepTxnTokens removes committed idempotency tokens that have exceeded their TTL.
// AWS DynamoDB expires tokens after 10 minutes; this prevents unbounded map growth.
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
}

// sweepTxnPending removes in-progress idempotency tokens that have exceeded txnPendingTTL.
// Under normal operation the defer in TransactWriteItems cleans up pending entries.
// This sweep is a safety net for orphaned entries (e.g. from a crashed goroutine).
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
}

func (j *Janitor) sweepStreamRecords() {
	db := j.Backend
	tables := db.ListAllTables()
	now := time.Now().Unix()
	const streamExpirySeconds = 24 * 60 * 60

	for _, t := range tables {
		t.mu.Lock("SweepStreamRecords")
		var cleared int
		for i := range t.StreamRecords {
			r := &t.StreamRecords[i]
			// If record is older than 24h, we can nil out its images to save space.
			// We don't remove it from the ring buffer slice to maintain ring buffer indices.
			if r.ApproximateCreationDateTime > 0 && now-r.ApproximateCreationDateTime > streamExpirySeconds {
				if r.OldImage != nil || r.NewImage != nil {
					r.OldImage = nil
					r.NewImage = nil
					cleared++
				}
			}
		}
		t.mu.Unlock()

		if cleared > 0 {
			telemetry.RecordWorkerItems("dynamodb", "StreamSweeper", cleared)
		}
	}
	telemetry.RecordWorkerTask("dynamodb", "StreamSweeper", "success")
}

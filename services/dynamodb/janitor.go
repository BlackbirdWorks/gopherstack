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
func (j *Janitor) Run(ctx context.Context) {
	mainTicker := time.NewTicker(j.Interval)
	defer mainTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-mainTicker.C:
			j.runOnce(ctx)
		}
	}
}

// runOnce orchestrates the various janitor tasks.
func (j *Janitor) runOnce(ctx context.Context) {
	j.sweepTTL(ctx)
	j.sweepTxnTokens()
	j.sweepTxnPending()
	j.sweepStreamRecords()
	j.Backend.exprCache.Sweep()
	j.runTableCleaner(ctx) // The original runOnce logic
}

// runTableCleaner records the current queue depth and finalises all pending deletions.
func (j *Janitor) runTableCleaner(ctx context.Context) {
	db := j.Backend

	// Snapshot pending tables under the lock across all regions, record depth before processing.
	db.mu.Lock("DDBJanitor")
	depth := 0
	names := make([]string, 0)

	for region, regionTables := range db.deletingTables {
		for name, table := range regionTables {
			depth++
			names = append(names, name)
			delete(db.deletingTables[region], name)
			if table.Tags != nil {
				table.Tags.Close()
			}
			table.mu.Close()
		}
	}
	db.mu.Unlock()

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

	for _, table := range tables {
		table.mu.RLock("TTLSweepCheck")
		ttlAttr := table.TTLAttribute
		table.mu.RUnlock()

		if ttlAttr == "" {
			continue
		}

		table.mu.Lock("TTLSweep")
		evictedCount := 0
		// Iterate backwards to safely remove items without affecting index of remaining items
		for i := len(table.Items) - 1; i >= 0; i-- {
			item := table.Items[i]
			expired := false
			if ttlVal, ok := dynamoattr.ParseNumeric(item[ttlAttr]); ok {
				if ttlVal < now {
					expired = true
				}
			}

			if expired {
				// Capture stream REMOVE event? AWS TTL evictions DO capture stream events
				// but mark them as "userIdentity": {"type": "Service", "principalId": "dynamodb.amazonaws.com"}
				// We'll capture it as a normal remove for now.
				table.appendStreamRecord(streamEventRemove, deepCopyItem(item), nil)
				evictedCount++

				// Evict item using optimized deleteItemAtIndex
				// This handles O(1) swap and index updates.
				// We no longer need to call t.rebuildIndexes() here.
				db.deleteItemAtIndex(table, i)
			}
		}

		if evictedCount > 0 {
			totalEvicted += evictedCount
			logger.Load(ctx).InfoContext(ctx, "DynamoDB janitor: TTL items evicted",
				"table", table.Name,
				"count", evictedCount)
		}
		table.mu.Unlock()
	}

	if totalEvicted > 0 {
		telemetry.RecordWorkerItems("dynamodb", "TTLSweeper", totalEvicted)
	}
	telemetry.RecordWorkerTask("dynamodb", "TTLSweeper", "success")
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

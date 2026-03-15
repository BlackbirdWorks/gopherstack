package dynamodb

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const defaultDDBJanitorInterval = 500 * time.Millisecond

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
	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.runOnce(ctx)
			j.sweepTTL(ctx)
			j.sweepTxnTokens()
			j.sweepTxnPending()
			j.Backend.exprCache.Sweep()
		}
	}
}

// runOnce records the current queue depth and finalises all pending deletions.
func (j *Janitor) runOnce(ctx context.Context) {
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
		newItems := make([]map[string]any, 0, len(table.Items))

		for _, item := range table.Items {
			expired := false
			if ttlVal, ok := dynamoattr.ParseNumeric(item[ttlAttr]); ok {
				// DynamoDB TTL: Item is expired if its TTL value < current time (Unix epoch seconds)
				if ttlVal < now {
					expired = true
				}
			}

			if expired {
				// Emit a REMOVE stream record before discarding the item so that
				// stream consumers can react to TTL-driven deletions.
				table.appendStreamRecord(streamEventRemove, deepCopyItem(item), nil)
				evictedCount++
			} else {
				newItems = append(newItems, item)
			}
		}

		if evictedCount > 0 {
			table.Items = newItems
			table.rebuildIndexes()
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

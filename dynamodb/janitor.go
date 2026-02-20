package dynamodb

import (
	"context"
	"log/slog"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const defaultDDBJanitorInterval = 500 * time.Millisecond

// Janitor is the DynamoDB background worker that finalises tables queued for
// async deletion and records queue-depth metrics for the live dashboard.
type Janitor struct {
	Backend  *InMemoryDB
	Log      *slog.Logger
	Interval time.Duration
}

// NewJanitor creates a new DynamoDB Janitor for the given backend.
func NewJanitor(backend *InMemoryDB, log *slog.Logger) *Janitor {
	return &Janitor{
		Backend:  backend,
		Log:      log,
		Interval: defaultDDBJanitorInterval,
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
		}
	}
}

// runOnce records the current queue depth and finalises all pending deletions.
func (j *Janitor) runOnce(ctx context.Context) {
	db := j.Backend

	// Snapshot pending tables under the lock, record depth before processing.
	db.mu.Lock("DDBJanitor")
	depth := len(db.deletingTables)
	names := make([]string, 0, depth)

	for name, table := range db.deletingTables {
		names = append(names, name)
		delete(db.deletingTables, name)
		table.mu.Close()
	}
	db.mu.Unlock()

	telemetry.RecordDeleteQueueDepth("dynamodb", depth)

	for _, name := range names {
		j.Log.InfoContext(ctx, "DynamoDB janitor: table deleted", "table", name)
	}
}

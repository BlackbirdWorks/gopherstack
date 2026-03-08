package dynamodb

import (
	"context"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// FISActions returns the FIS action definitions that the DynamoDB service supports.
func (h *DynamoDBHandler) FISActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:dynamodb:global-table-pause-replication",
			Description: "Pause global table replication for the target DynamoDB table",
			TargetType:  "aws:dynamodb:global-table",
			Parameters: []service.FISParamDef{
				{Name: "duration", Description: "ISO 8601 duration (e.g. PT5M)", Required: false},
			},
		},
	}
}

// ExecuteFISAction executes a FIS action against resolved DynamoDB targets.
func (h *DynamoDBHandler) ExecuteFISAction(ctx context.Context, action service.FISActionExecution) error {
	if action.ActionID != "aws:dynamodb:global-table-pause-replication" {
		return nil
	}

	db, ok := h.Backend.(*InMemoryDB)
	if !ok {
		return nil
	}

	return db.activateReplicationPause(ctx, action.Targets, action.Duration)
}

// activateReplicationPause marks the given table ARNs as replication-paused.
// It always registers a goroutine that clears the pause when ctx is cancelled
// (experiment stopped), and also schedules time-based expiry when dur > 0.
func (db *InMemoryDB) activateReplicationPause(ctx context.Context, tableARNs []string, dur time.Duration) error {
	var expiry time.Time
	if dur > 0 {
		expiry = time.Now().Add(dur)
	}

	db.mu.Lock("FISPauseReplication")

	for _, tableARN := range tableARNs {
		db.fisReplicationPaused[tableARN] = expiry
	}

	db.mu.Unlock()

	if dur > 0 {
		// Time-limited: clear after duration or on cancellation.
		go db.scheduleReplicationPauseCleanup(ctx, tableARNs, dur)
	} else {
		// Indefinite fault (dur==0): the goroutine blocks on ctx.Done().
		// It terminates when StopExperiment cancels the experiment context,
		// or when the server shuts down (root context is cancelled).
		// This is not a goroutine leak — the goroutine is intentionally
		// bound to the experiment lifetime via ctx.
		go func() {
			<-ctx.Done()

			db.mu.Lock("FISPauseReplication-ctxcancel")
			defer db.mu.Unlock()

			for _, tableARN := range tableARNs {
				delete(db.fisReplicationPaused, tableARN)
			}
		}()
	}

	return nil
}

// scheduleReplicationPauseCleanup removes replication-pause entries after the
// given duration or when ctx is cancelled (whichever comes first).
// On ctx cancellation, entries are removed unconditionally so that StopExperiment
// always clears active pauses regardless of remaining time.
func (db *InMemoryDB) scheduleReplicationPauseCleanup(ctx context.Context, tableARNs []string, dur time.Duration) {
	ctxCancelled := false

	select {
	case <-ctx.Done():
		ctxCancelled = true
	case <-time.After(dur):
	}

	db.mu.Lock("FISPauseReplication-cleanup")
	defer db.mu.Unlock()

	now := time.Now()

	for _, tableARN := range tableARNs {
		exp, exists := db.fisReplicationPaused[tableARN]
		if !exists {
			continue
		}

		// On ctx cancellation always remove; on timeout only remove if expired.
		if ctxCancelled || (!exp.IsZero() && now.After(exp)) {
			delete(db.fisReplicationPaused, tableARN)
		}
	}
}

// IsReplicationPaused reports whether FIS global-table-pause-replication is
// currently active for the given table ARN or name.
// Expired entries are lazily evicted to prevent unbounded map growth.
func (db *InMemoryDB) IsReplicationPaused(tableARNOrName string) bool {
	db.mu.Lock("IsReplicationPaused")
	defer db.mu.Unlock()

	exp, foundKey, ok := db.lookupReplicationPauseKey(tableARNOrName)
	if !ok {
		return false
	}

	if !exp.IsZero() && time.Now().After(exp) {
		// Lazily evict expired entry.
		delete(db.fisReplicationPaused, foundKey)

		return false
	}

	return true
}

// lookupReplicationPauseKey finds the map key and expiry for the given table ARN
// or name. The caller must hold the mutex.
func (db *InMemoryDB) lookupReplicationPauseKey(tableARNOrName string) (time.Time, string, bool) {
	if exp, ok := db.fisReplicationPaused[tableARNOrName]; ok {
		return exp, tableARNOrName, true
	}

	// Also try matching by bare table name suffix.
	for key, e := range db.fisReplicationPaused {
		if strings.HasSuffix(key, "/"+tableARNOrName) || key == tableARNOrName {
			return e, key, true
		}
	}

	return time.Time{}, "", false
}

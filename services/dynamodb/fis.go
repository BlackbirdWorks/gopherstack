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

// activateReplicationPause marks the given table ARNs as replication-paused
// and schedules automatic cleanup after dur (if non-zero).
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
		go db.scheduleReplicationPauseCleanup(ctx, tableARNs, dur)
	}

	return nil
}

// scheduleReplicationPauseCleanup removes expired replication-pause entries after
// the given duration or when ctx is cancelled.
func (db *InMemoryDB) scheduleReplicationPauseCleanup(ctx context.Context, tableARNs []string, dur time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(dur):
	}

	db.mu.Lock("FISPauseReplication-cleanup")
	defer db.mu.Unlock()

	now := time.Now()

	for _, tableARN := range tableARNs {
		if exp, exists := db.fisReplicationPaused[tableARN]; exists {
			if !exp.IsZero() && now.After(exp) {
				delete(db.fisReplicationPaused, tableARN)
			}
		}
	}
}

// IsReplicationPaused reports whether FIS global-table-pause-replication is
// currently active for the given table ARN or name.
func (db *InMemoryDB) IsReplicationPaused(tableARNOrName string) bool {
	db.mu.RLock("IsReplicationPaused")
	defer db.mu.RUnlock()

	exp, ok := db.fisReplicationPaused[tableARNOrName]
	if !ok {
		// Also try matching by bare table name suffix in case the caller passes just the name.
		for key, e := range db.fisReplicationPaused {
			if strings.HasSuffix(key, "/"+tableARNOrName) || key == tableARNOrName {
				exp = e
				ok = true

				break
			}
		}
	}

	if !ok {
		return false
	}

	if !exp.IsZero() && time.Now().After(exp) {
		return false
	}

	return true
}

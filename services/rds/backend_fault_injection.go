package rds

import (
	"context"
	"strings"
	"time"
)

// scheduleFailoverFaultCleanup removes failover faults after the given duration
// or when ctx is cancelled (whichever comes first).
// On ctx cancellation, entries are removed unconditionally so that StopExperiment
// always clears active faults regardless of remaining time.
func (b *InMemoryBackend) scheduleFailoverFaultCleanup(ctx context.Context, ids []string, dur time.Duration) {
	ctxCancelled := false

	timer := time.NewTimer(dur)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		ctxCancelled = true
	case <-timer.C:
	}

	b.mu.Lock("FISFailoverDBClusters-cleanup")
	defer b.mu.Unlock()

	now := time.Now()

	for _, id := range ids {
		exp, ok := b.fisFailoverFaults[id]
		if !ok {
			continue
		}

		// On ctx cancellation always remove; on timeout only remove if expired.
		if ctxCancelled || (!exp.IsZero() && now.After(exp)) {
			delete(b.fisFailoverFaults, id)
		}
	}
}

// rdsIDFromARN extracts the resource identifier from an RDS ARN or returns the
// input unchanged when it is already a bare identifier.
// Handles the two common forms:
//   - arn:aws:rds:{region}:{account}:{type}/{id}  → returns {id}
//   - arn:aws:rds:{region}:{account}:{type}:{id}  → returns {id}
func rdsIDFromARN(arnOrID string) string {
	// Slash-delimited ARN: arn:aws:rds:…/{id}
	if idx := strings.LastIndex(arnOrID, "/"); idx >= 0 {
		return arnOrID[idx+1:]
	}

	// Colon-delimited RDS ARN: arn:aws:rds:…:db:my-id
	if strings.HasPrefix(arnOrID, "arn:") {
		parts := strings.Split(arnOrID, ":")
		if len(parts) > 0 {
			return parts[len(parts)-1]
		}
	}

	return arnOrID
}

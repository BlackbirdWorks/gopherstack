package rds

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrRebootFailed is returned when one or more FIS reboot-instance actions fail.
var ErrRebootFailed = errors.New("aws:rds:reboot-db-instances failed")

// ----------------------------------------
// FIS interface implementation
// ----------------------------------------

// FISActions returns the FIS action definitions that the RDS service supports.
func (h *Handler) FISActions() []service.FISActionDefinition {
	return []service.FISActionDefinition{
		{
			ActionID:    "aws:rds:reboot-db-instances",
			Description: "Reboot target RDS DB instances",
			TargetType:  "aws:rds:db",
		},
		{
			ActionID:    "aws:rds:failover-db-cluster",
			Description: "Trigger a failover for the target RDS Aurora DB cluster",
			TargetType:  "aws:rds:cluster",
		},
	}
}

// ExecuteFISAction executes a FIS action against resolved RDS targets.
func (h *Handler) ExecuteFISAction(ctx context.Context, action service.FISActionExecution) error {
	switch action.ActionID {
	case "aws:rds:reboot-db-instances":
		return h.fisRebootDBInstances(action.Targets)
	case "aws:rds:failover-db-cluster":
		return h.fisFailoverDBClusters(ctx, action.Targets, action.Duration)
	}

	return nil
}

// fisRebootDBInstances reboots the given DB instances identified by ARN or bare identifier.
func (h *Handler) fisRebootDBInstances(targets []string) error {
	var errs []string

	for _, t := range targets {
		id := rdsIDFromARN(t)

		if _, err := h.Backend.RebootDBInstance(id); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %s", ErrRebootFailed, strings.Join(errs, "; "))
	}

	return nil
}

// fisFailoverDBClusters simulates a failover for the given DB clusters.
// In the in-memory backend there is no real replication, so this records a
// timed failover event on the backend for observability and automatically
// clears it after the given duration (if non-zero).
func (h *Handler) fisFailoverDBClusters(ctx context.Context, targets []string, dur time.Duration) error {
	var expiry time.Time
	if dur > 0 {
		expiry = time.Now().Add(dur)
	}

	h.Backend.mu.Lock("FISFailoverDBClusters")
	for _, t := range targets {
		id := rdsIDFromARN(t)
		h.Backend.fisFailoverFaults[id] = expiry
	}
	h.Backend.mu.Unlock()

	// Schedule automatic cleanup when a duration is specified.
	if dur > 0 {
		go h.Backend.scheduleFailoverFaultCleanup(ctx, targets, dur)
	}

	return nil
}

// IsClusterFailoverActive reports whether a FIS failover simulation is currently
// active for the cluster with the given identifier.
func (b *InMemoryBackend) IsClusterFailoverActive(clusterID string) bool {
	b.mu.RLock("IsClusterFailoverActive")
	defer b.mu.RUnlock()

	exp, ok := b.fisFailoverFaults[clusterID]
	if !ok {
		return false
	}

	if !exp.IsZero() && time.Now().After(exp) {
		return false
	}

	return true
}

// scheduleFailoverFaultCleanup removes expired failover faults after the given
// duration or when ctx is cancelled.
func (b *InMemoryBackend) scheduleFailoverFaultCleanup(ctx context.Context, targets []string, dur time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(dur):
	}

	b.mu.Lock("FISFailoverDBClusters-cleanup")
	defer b.mu.Unlock()

	now := time.Now()

	for _, t := range targets {
		id := rdsIDFromARN(t)

		if exp, ok := b.fisFailoverFaults[id]; ok {
			if !exp.IsZero() && now.After(exp) {
				delete(b.fisFailoverFaults, id)
			}
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

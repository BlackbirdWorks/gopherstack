package docdb

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// This file backs the "no pending-maintenance-action queue" gap identified
// in PARITY.md: ApplyPendingMaintenanceAction validated its inputs and
// DescribePendingMaintenanceActions always returned an empty list, so
// nothing was ever really "pending" -- mirroring the already-completed
// neptune service's identical fix (services/neptune/maintenance.go). Real
// AWS populates pending maintenance actions itself from system-side
// upgrade/security-patch availability data this backend has no equivalent
// of, so AddPendingMaintenanceActionInternal exists to seed the queue for
// tests -- mirroring the AddDBClusterInternal/AddDBClusterSnapshotInternal/
// AddDBClusterParameterGroupInternal seeding pattern used elsewhere in this
// backend -- after which Apply/Describe operate on real, persisted queue
// state instead of a disguised no-op.

// pendingActionsFor returns (creating if necessary) the pending-action map
// for resourceARN. Callers must hold the backend write lock.
func (b *InMemoryBackend) pendingActionsFor(resourceARN string) map[string]PendingMaintenanceAction {
	if b.pendingMaintenanceActions[resourceARN] == nil {
		b.pendingMaintenanceActions[resourceARN] = make(map[string]PendingMaintenanceAction)
	}

	return b.pendingMaintenanceActions[resourceARN]
}

// AddPendingMaintenanceActionInternal queues a maintenance action for a
// resource, bypassing normal validation. Used for seeding tests.
func (b *InMemoryBackend) AddPendingMaintenanceActionInternal(
	resourceARN, action, description string,
) *PendingMaintenanceAction {
	b.mu.Lock("AddPendingMaintenanceActionInternal")
	defer b.mu.Unlock()
	pa := PendingMaintenanceAction{
		Action:               action,
		Description:          description,
		AutoAppliedAfterDate: time.Now().UTC().Format(time.RFC3339),
	}
	b.pendingActionsFor(resourceARN)[action] = pa

	return &pa
}

// ApplyPendingMaintenanceAction applies (or opts in/out of) a queued
// maintenance action for a resource, returning that resource's current full
// set of pending actions -- matching AWS's ApplyPendingMaintenanceActionOutput,
// which always echoes ResourcePendingMaintenanceActions rather than just the
// one action touched. Calling Apply for a resource/action combination that
// was never queued (nothing was ever seeded/became eligible) is not an error
// -- it mirrors AWS's own opt-in semantics, where opting into a maintenance
// action that doesn't currently apply to the resource is a harmless no-op --
// so it simply returns whatever (possibly empty) pending-action set the
// resource already has.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(
	_ context.Context, resourceARN, action, optInType string,
) (*ResourcePendingMaintenanceActions, error) {
	if resourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceIdentifier is required", ErrInvalidParameter)
	}
	if action == "" {
		return nil, fmt.Errorf("%w: ApplyAction is required", ErrInvalidParameter)
	}
	switch optInType {
	case optInTypeImmediate, optInTypeNextMaintenance, optInTypeUndoOptIn:
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: OptInType must be one of %s, %s, %s",
			ErrInvalidParameter,
			optInTypeImmediate, optInTypeNextMaintenance, optInTypeUndoOptIn,
		)
	}
	b.mu.Lock("ApplyPendingMaintenanceAction")
	defer b.mu.Unlock()
	actions := b.pendingActionsFor(resourceARN)
	if pa, exists := actions[action]; exists {
		applyOptIn(&pa, optInType)
		actions[action] = pa
	}

	return &ResourcePendingMaintenanceActions{
		ResourceIdentifier: resourceARN,
		Actions:            sortedPendingActions(actions),
	}, nil
}

// applyOptIn mutates pa's CurrentApplyDate/OptInStatus per AWS's three
// OptInType semantics: immediate applies right away, next-maintenance defers
// to the resource's next maintenance window, and undo-opt-in cancels a
// previously-registered next-maintenance opt-in.
func applyOptIn(pa *PendingMaintenanceAction, optInType string) {
	switch optInType {
	case optInTypeImmediate:
		pa.CurrentApplyDate = time.Now().UTC().Format(time.RFC3339)
		pa.OptInStatus = optInTypeImmediate
	case optInTypeNextMaintenance:
		pa.CurrentApplyDate = ""
		pa.OptInStatus = optInTypeNextMaintenance
	case optInTypeUndoOptIn:
		pa.CurrentApplyDate = ""
		pa.OptInStatus = ""
	}
}

// DescribePendingMaintenanceActions returns the pending maintenance actions
// for every resource that has at least one queued (resourceARN, when
// non-empty, restricts results to a single resource ARN, matching
// DescribePendingMaintenanceActionsInput.ResourceIdentifier) -- AWS never
// includes a ResourcePendingMaintenanceActions entry with an empty
// PendingMaintenanceActionDetails list.
func (b *InMemoryBackend) DescribePendingMaintenanceActions(
	_ context.Context, resourceARN string,
) []ResourcePendingMaintenanceActions {
	b.mu.RLock("DescribePendingMaintenanceActions")
	defer b.mu.RUnlock()
	result := make([]ResourcePendingMaintenanceActions, 0, len(b.pendingMaintenanceActions))
	for arn, actions := range b.pendingMaintenanceActions {
		if resourceARN != "" && arn != resourceARN {
			continue
		}
		details := sortedPendingActions(actions)
		if len(details) == 0 {
			continue
		}
		result = append(result, ResourcePendingMaintenanceActions{
			ResourceIdentifier: arn,
			Actions:            details,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ResourceIdentifier < result[j].ResourceIdentifier
	})

	return result
}

// sortedPendingActions renders actions (a map, for O(1) per-action lookup)
// as a deterministically-ordered slice for wire responses.
func sortedPendingActions(actions map[string]PendingMaintenanceAction) []PendingMaintenanceAction {
	names := make([]string, 0, len(actions))
	for name := range actions {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]PendingMaintenanceAction, 0, len(names))
	for _, name := range names {
		result = append(result, actions[name])
	}

	return result
}

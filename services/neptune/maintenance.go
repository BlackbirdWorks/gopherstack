package neptune

import (
	"context"
	"fmt"
	"slices"
	"sort"
)

// This file backs the "no pending-maintenance-action queue" gap identified in
// PARITY.md: ApplyPendingMaintenanceAction validated its inputs and
// DescribePendingMaintenanceActions always returned an empty list, so nothing
// was ever really "pending". Real AWS populates pending maintenance actions
// itself from system-side upgrade/security-patch availability data this
// backend has no equivalent of, so AddPendingMaintenanceActionInternal exists
// to seed the queue for tests -- mirroring the AddClusterInternal/
// AddSnapshotInternal/AddParameterGroupInternal seeding pattern used
// elsewhere in this backend -- after which Apply/Describe operate on real,
// persisted queue state instead of a disguised no-op.

const (
	optInImmediate       = "immediate"
	optInNextMaintenance = "next-maintenance"
	optInUndo            = "undo-opt-in"
)

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
		AutoAppliedAfterDate: nowISO8601(),
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
	_ context.Context, resourceID, applyAction, optInType string,
) (*ResourcePendingMaintenanceActions, error) {
	if resourceID == "" {
		return nil, fmt.Errorf("%w: ResourceIdentifier is required", ErrInvalidParameter)
	}
	if applyAction == "" {
		return nil, fmt.Errorf("%w: ApplyAction is required", ErrInvalidParameter)
	}
	switch optInType {
	case optInImmediate, optInNextMaintenance, optInUndo:
	default:
		return nil, fmt.Errorf(
			"%w: OptInType must be one of immediate, next-maintenance, undo-opt-in",
			ErrInvalidParameter,
		)
	}
	b.mu.Lock("ApplyPendingMaintenanceAction")
	defer b.mu.Unlock()
	actions := b.pendingActionsFor(resourceID)
	if pa, exists := actions[applyAction]; exists {
		applyOptIn(&pa, optInType)
		actions[applyAction] = pa
	}

	return &ResourcePendingMaintenanceActions{
		ResourceIdentifier:              resourceID,
		PendingMaintenanceActionDetails: sortedPendingActions(actions),
	}, nil
}

// applyOptIn mutates pa's CurrentApplyDate/OptInStatus per AWS's three
// OptInType semantics: immediate applies right away, next-maintenance defers
// to the resource's next maintenance window, and undo-opt-in cancels a
// previously-registered next-maintenance opt-in.
func applyOptIn(pa *PendingMaintenanceAction, optInType string) {
	switch optInType {
	case optInImmediate:
		pa.CurrentApplyDate = nowISO8601()
		pa.OptInStatus = optInImmediate
	case optInNextMaintenance:
		pa.CurrentApplyDate = ""
		pa.OptInStatus = optInNextMaintenance
	case optInUndo:
		pa.CurrentApplyDate = ""
		pa.OptInStatus = ""
	}
}

// DescribePendingMaintenanceActions returns the pending maintenance actions
// for every resource that has at least one queued (resourceFilter, when
// non-empty, restricts results to a single resource ARN) -- AWS never
// includes a ResourcePendingMaintenanceActions entry with an empty
// PendingMaintenanceActionDetails list.
func (b *InMemoryBackend) DescribePendingMaintenanceActions(
	_ context.Context, resourceFilter []string,
) []ResourcePendingMaintenanceActions {
	b.mu.RLock("DescribePendingMaintenanceActions")
	defer b.mu.RUnlock()
	result := make([]ResourcePendingMaintenanceActions, 0, len(b.pendingMaintenanceActions))
	for arn, actions := range b.pendingMaintenanceActions {
		if len(resourceFilter) > 0 && !slices.Contains(resourceFilter, arn) {
			continue
		}
		details := sortedPendingActions(actions)
		if len(details) == 0 {
			continue
		}
		result = append(result, ResourcePendingMaintenanceActions{
			ResourceIdentifier:              arn,
			PendingMaintenanceActionDetails: details,
		})
	}

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

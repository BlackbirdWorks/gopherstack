package rds

import (
	"fmt"
	"net/url"
	"slices"
)

// isValidOptInType reports whether optInType is one of
// ApplyPendingMaintenanceActionInput.OptInType's legal values, taken from
// its own doc comment (rds@v1.124.1 api_op_ApplyPendingMaintenanceAction.go:53-63)
// since OptInType is an untyped *string on the real SDK, not a generated
// enum type with a Values() method. A function (not a package var) to stay
// lint-clean without a global, matching this repo's builtInConnectionTypes()
// convention.
func isValidOptInType(optInType string) bool {
	switch optInType {
	case "immediate", "next-maintenance", "undo-opt-in":
		return true
	default:
		return false
	}
}

// registerDBUpgradeActionLocked records a pending db-upgrade maintenance
// action for inst, if one isn't already pending. Callers must hold b.mu for
// writing. Mirrors the minimal pending-action lifecycle gopherstack-qpxye
// models: a deferred (ApplyImmediately=false) EngineVersion change queues
// this the same way real AWS surfaces an available engine upgrade.
func (b *InMemoryBackend) registerDBUpgradeActionLocked(inst *DBInstance) {
	id := normalizeID(inst.DBInstanceIdentifier)
	for _, a := range b.pendingMaintenanceActions[id] {
		if a.Action == pendingActionDBUpgrade {
			return
		}
	}
	b.pendingMaintenanceActions[id] = append(b.pendingMaintenanceActions[id], &PendingMaintenanceAction{
		ResourceIdentifier: b.rdsARN("db", inst.DBInstanceIdentifier),
		Action:             pendingActionDBUpgrade,
		Description:        "New DB engine version is available",
	})
}

// clearDBUpgradeActionLocked removes any pending db-upgrade action for
// instanceID. Callers must hold b.mu for writing.
func (b *InMemoryBackend) clearDBUpgradeActionLocked(instanceID string) {
	id := normalizeID(instanceID)
	actions := b.pendingMaintenanceActions[id]
	idx := slices.IndexFunc(actions, func(a *PendingMaintenanceAction) bool {
		return a.Action == pendingActionDBUpgrade
	})
	if idx < 0 {
		return
	}
	b.pendingMaintenanceActions[id] = slices.Delete(actions, idx, idx+1)
}

// ApplyPendingMaintenanceAction applies a pending maintenance action to a resource.
// The resource is identified by its ARN. OptInType is required and validated
// against its documented enum. OptInType=immediate on the db-upgrade action
// this backend models (see registerDBUpgradeActionLocked) applies the
// instance's deferred EngineVersion change now and clears the action;
// next-maintenance and undo-opt-in are validated but otherwise have no
// further modelled effect.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(
	resourceID, applyAction, optInType string,
) (string, error) {
	if resourceID == "" {
		return "", fmt.Errorf("%w: ResourceIdentifier must not be empty", ErrInvalidParameter)
	}
	if applyAction == "" {
		return "", fmt.Errorf("%w: ApplyAction must not be empty", ErrInvalidParameter)
	}
	if optInType == "" {
		return "", fmt.Errorf("%w: OptInType must not be empty", ErrInvalidParameter)
	}
	if !isValidOptInType(optInType) {
		return "", fmt.Errorf(
			"%w: OptInType must be one of immediate, next-maintenance, undo-opt-in; got %q",
			ErrInvalidParameter, optInType,
		)
	}

	b.mu.Lock("ApplyPendingMaintenanceAction")
	defer b.mu.Unlock()

	id := normalizeID(rdsIDFromARN(resourceID))

	inst, instExists := b.instances.Get(id)
	if !instExists {
		if _, clusterExists := b.clusters.Get(id); !clusterExists {
			return "", fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
		}
	}

	if optInType == "immediate" {
		actions := b.pendingMaintenanceActions[id]
		idx := slices.IndexFunc(actions, func(a *PendingMaintenanceAction) bool {
			return a.Action == applyAction
		})
		if idx >= 0 {
			b.pendingMaintenanceActions[id] = slices.Delete(actions, idx, idx+1)
			if instExists && applyAction == pendingActionDBUpgrade && inst.PendingModifiedValues != nil {
				applyPendingModifications(inst)
				inst.DBInstanceStatus = instanceStatusAvailable
				delete(b.instanceReadyAt, inst.DBInstanceIdentifier)
			}
		}
	}

	return resourceID, nil
}

// DescribePendingMaintenanceActions returns pending maintenance actions,
// optionally narrowed to a single resource ARN or identifier.
func (b *InMemoryBackend) DescribePendingMaintenanceActions(resourceID string) []PendingMaintenanceAction {
	b.mu.RLock("DescribePendingMaintenanceActions")
	defer b.mu.RUnlock()

	if resourceID != "" {
		id := normalizeID(rdsIDFromARN(resourceID))
		result := make([]PendingMaintenanceAction, 0, len(b.pendingMaintenanceActions[id]))
		for _, a := range b.pendingMaintenanceActions[id] {
			result = append(result, *a)
		}

		return result
	}

	ids := make([]string, 0, len(b.pendingMaintenanceActions))
	for id := range b.pendingMaintenanceActions {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	result := make([]PendingMaintenanceAction, 0, len(b.pendingMaintenanceActions))
	for _, id := range ids {
		for _, a := range b.pendingMaintenanceActions[id] {
			result = append(result, *a)
		}
	}

	return result
}

// isKnownPendingMaintenanceActionFilterName reports whether name is a
// Filters.Filter.N.Name value AWS recognizes for
// DescribePendingMaintenanceActions (rds@v1.124.1
// api_op_DescribePendingMaintenanceActions.go:38-50).
func isKnownPendingMaintenanceActionFilterName(name string) bool {
	switch name {
	case filterNameDBClusterID, filterNameDBInstanceID:
		return true
	default:
		return false
	}
}

// applyPendingMaintenanceActionFilters narrows actions per the AWS
// DescribePendingMaintenanceActions Filters contract: each filter ANDs
// together, and a filter's Values list (identifiers or ARNs, per the op's
// own doc comment) is OR-matched against the action's own ResourceIdentifier
// (which real AWS -- and ApplyPendingMaintenanceAction's echo of it above --
// stores as either a DB cluster or a DB instance ARN, so the filter's
// db-cluster-id/db-instance-id distinction isn't separable from the
// resource's ARN type on this backend's own data; both filter names match
// against the same resolved identifier). An unrecognized filter name returns
// InvalidParameterValue, matching real AWS.
func applyPendingMaintenanceActionFilters(
	vals url.Values, actions []PendingMaintenanceAction,
) ([]PendingMaintenanceAction, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return actions, nil
	}

	for name := range filters {
		if !isKnownPendingMaintenanceActionFilterName(name) {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	filtered := make([]PendingMaintenanceAction, 0, len(actions))
	for _, a := range actions {
		if matchesAllPendingMaintenanceActionFilters(a, filters) {
			filtered = append(filtered, a)
		}
	}

	return filtered, nil
}

func matchesAllPendingMaintenanceActionFilters(a PendingMaintenanceAction, filters map[string][]string) bool {
	ident := rdsIDFromARN(a.ResourceIdentifier)
	for name, values := range filters {
		switch name {
		case filterNameDBClusterID, filterNameDBInstanceID:
			if !containsFoldIDOrARN(values, ident) {
				return false
			}
		}
	}

	return true
}

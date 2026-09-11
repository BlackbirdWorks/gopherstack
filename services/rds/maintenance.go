package rds

import (
	"fmt"
	"net/url"
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

// ApplyPendingMaintenanceAction applies a pending maintenance action to a resource.
// The resource is identified by its ARN. This implementation validates the resource exists
// and returns a stub response. OptInType is required and validated against
// its documented enum, but this backend has no mechanism anywhere that ever
// generates a real pending maintenance action for a resource (see
// DescribePendingMaintenanceActions, hardcoded to return none), so
// OptInType's immediate/next-window/undo semantics have no state to act on
// -- validated and rejected if invalid, not silently accepted, but not
// wired to any further effect.
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

	b.mu.RLock("ApplyPendingMaintenanceAction")
	defer b.mu.RUnlock()

	id := rdsIDFromARN(resourceID)

	// Validate that the referenced resource exists (instance or cluster).
	if _, ok := b.instances.Get(normalizeID(id)); !ok {
		if _, ok2 := b.clusters.Get(normalizeID(id)); !ok2 {
			return "", fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
		}
	}

	return resourceID, nil
}

// DescribePendingMaintenanceActions returns pending maintenance actions.
//
// This backend never generates a real pending maintenance action for any
// resource (see ApplyPendingMaintenanceAction's own doc comment above), so
// this always returns an empty slice -- filed as gopherstack-vl4m's
// follow-up for the structural gap. applyPendingMaintenanceActionFilters
// below still validates and narrows the Filters contract for wire
// correctness (and so it's ready the moment that gap is fixed), but with no
// data ever populated, only its unrecognized-filter-name rejection is
// observable through the real API today.
func (b *InMemoryBackend) DescribePendingMaintenanceActions(_ string) []PendingMaintenanceAction {
	return []PendingMaintenanceAction{}
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

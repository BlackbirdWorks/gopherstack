package docdb

import (
	"context"
	"fmt"
)

// ApplyPendingMaintenanceAction applies a pending maintenance action to a resource.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(
	_ context.Context,
	resourceARN, action, optInType string,
) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceIdentifier is required", ErrInvalidParameter)
	}
	if action == "" {
		return fmt.Errorf("%w: ApplyAction is required", ErrInvalidParameter)
	}
	if optInType == "" {
		return fmt.Errorf("%w: OptInType is required", ErrInvalidParameter)
	}
	switch optInType {
	case optInTypeImmediate, optInTypeNextMaintenance, optInTypeUndoOptIn:
		// valid
	default:
		return fmt.Errorf(
			"%w: OptInType must be one of %s, %s, %s",
			ErrInvalidParameter,
			optInTypeImmediate, optInTypeNextMaintenance, optInTypeUndoOptIn,
		)
	}

	return nil
}

// DescribePendingMaintenanceActions returns pending maintenance actions for resources.
// This implementation returns an empty list (in-memory emulation has no real pending actions).
func (b *InMemoryBackend) DescribePendingMaintenanceActions(
	_ context.Context,
	_ string,
) []ResourcePendingMaintenanceActions {
	return []ResourcePendingMaintenanceActions{}
}

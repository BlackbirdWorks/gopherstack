package rds

import "fmt"

// ApplyPendingMaintenanceAction applies a pending maintenance action to a resource.
// The resource is identified by its ARN. This implementation validates the resource exists
// and returns a stub response.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(
	resourceID, applyAction string,
) (string, error) {
	if resourceID == "" {
		return "", fmt.Errorf("%w: ResourceIdentifier must not be empty", ErrInvalidParameter)
	}
	if applyAction == "" {
		return "", fmt.Errorf("%w: ApplyAction must not be empty", ErrInvalidParameter)
	}

	b.mu.RLock("ApplyPendingMaintenanceAction")
	defer b.mu.RUnlock()

	id := rdsIDFromARN(resourceID)

	// Validate that the referenced resource exists (instance or cluster).
	if _, ok := b.instances.Get(normalizeID(id)); !ok {
		if _, ok2 := b.clusters.Get(normalizeID(id)); !ok2 {
			return "", fmt.Errorf("%w: resource %s not found", ErrInstanceNotFound, resourceID)
		}
	}

	return resourceID, nil
}

// DescribePendingMaintenanceActions returns pending maintenance actions.
func (b *InMemoryBackend) DescribePendingMaintenanceActions(_ string) []PendingMaintenanceAction {
	return []PendingMaintenanceAction{}
}

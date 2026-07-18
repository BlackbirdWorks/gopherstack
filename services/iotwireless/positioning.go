package iotwireless

import (
	"cmp"
	"maps"
	"slices"
)

// GetPosition returns the position data for a resource.
func (b *InMemoryBackend) GetPosition(resourceID string) map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if pos, ok := b.positions[resourceID]; ok {
		result := make(map[string]any, len(pos))
		maps.Copy(result, pos)

		return result
	}

	return map[string]any{}
}

// UpdatePosition updates the position data for a resource.
func (b *InMemoryBackend) UpdatePosition(resourceID string, position map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	pos := make(map[string]any, len(position))
	maps.Copy(pos, position)
	b.positions[resourceID] = pos

	return nil
}

// annotateSemtechGnssSolver returns a copy of solvers with the constant
// Provider/Type fields ("Semtech"/"GNSS") injected into the SemtechGnss
// sub-object, matching AWS's behaviour of always reporting these values since
// Semtech GNSS is currently the only supported position solver.
func annotateSemtechGnssSolver(solvers map[string]any) map[string]any {
	if solvers == nil {
		return nil
	}

	out := make(map[string]any, len(solvers))
	maps.Copy(out, solvers)

	if sg, ok := out["SemtechGnss"].(map[string]any); ok {
		const injectedSolverFields = 2 // Provider + Type

		sgCopy := make(map[string]any, len(sg)+injectedSolverFields)
		maps.Copy(sgCopy, sg)
		sgCopy["Provider"] = "Semtech"
		sgCopy["Type"] = "GNSS"
		out["SemtechGnss"] = sgCopy
	}

	return out
}

// PutPositionConfiguration stores the position solver configuration for a
// resource.
func (b *InMemoryBackend) PutPositionConfiguration(
	resourceID, resourceType, destination string, solvers map[string]any,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.positionConfigs.Put(&PositionConfigEntry{
		ResourceIdentifier: resourceID,
		ResourceType:       resourceType,
		Destination:        destination,
		Solvers:            annotateSemtechGnssSolver(solvers),
	})

	return nil
}

// GetPositionConfiguration returns the stored position configuration for a
// resource, if any.
func (b *InMemoryBackend) GetPositionConfiguration(resourceID string) (*PositionConfigEntry, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	e, ok := b.positionConfigs.Get(resourceID)
	if !ok {
		return nil, false
	}

	cp := *e

	return &cp, true
}

// ListPositionConfigurations returns all stored position configurations,
// optionally filtered by resource type.
func (b *InMemoryBackend) ListPositionConfigurations(resourceType string) []*PositionConfigEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := b.positionConfigs.All()
	result := make([]*PositionConfigEntry, 0, len(all))

	for _, e := range all {
		if resourceType != "" && e.ResourceType != resourceType {
			continue
		}

		cp := *e
		result = append(result, &cp)
	}

	slices.SortFunc(result, func(a, b *PositionConfigEntry) int {
		return cmp.Compare(a.ResourceIdentifier, b.ResourceIdentifier)
	})

	return result
}

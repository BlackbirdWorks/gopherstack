package ocicontainerinstances

import (
	"maps"
	"slices"
)

// Compartments lists the distinct compartments that hold container instances.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}
	for _, in := range b.instances {
		seen[in.Compartment] = struct{}{}
	}

	return slices.Sorted(maps.Keys(seen))
}

// EngineMode reports the active engine: EngineDocker or EngineState.
func (b *InMemoryBackend) EngineMode() string {
	b.mu.RLock("EngineMode")
	defer b.mu.RUnlock()

	if b.engine != nil {
		return EngineDocker
	}

	return EngineState
}

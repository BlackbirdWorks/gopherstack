package ocifilestorage

import (
	"maps"
	"slices"
)

// Compartments lists the distinct compartments that hold File Storage resources.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}

	for _, e := range b.fss {
		seen[e.W.CompartmentID] = struct{}{}
	}

	for _, e := range b.mts {
		seen[e.W.CompartmentID] = struct{}{}
	}

	for _, e := range b.sets {
		seen[e.W.CompartmentID] = struct{}{}
	}

	for _, e := range b.pols {
		seen[e.W.CompartmentID] = struct{}{}
	}

	for _, e := range b.reps {
		seen[e.W.CompartmentID] = struct{}{}
	}

	return slices.Sorted(maps.Keys(seen))
}

// NFSEnabled reports whether the NFS data plane is running.
func (b *InMemoryBackend) NFSEnabled() bool {
	b.mu.RLock("NFSEnabled")
	defer b.mu.RUnlock()

	return b.dp != nil
}

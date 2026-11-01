package ocidatabase

import (
	"maps"
	"slices"
)

// Compartments lists the distinct compartments that hold Autonomous Databases.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}
	for _, db := range b.dbs {
		seen[db.CompartmentID] = struct{}{}
	}

	return slices.Sorted(maps.Keys(seen))
}

package ocicontainerengine

import "slices"

// Compartments lists the distinct compartments that hold clusters.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}

	for _, c := range b.clusters {
		if c.State != stateDeleted {
			seen[c.CompartmentID] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	slices.Sort(out)

	return out
}

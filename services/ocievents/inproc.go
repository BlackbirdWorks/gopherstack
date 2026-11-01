package ocievents

import "slices"

// Compartments lists the distinct compartments that hold rules.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}

	for _, r := range b.rules {
		if r.State != stateDeleted {
			seen[r.CompartmentID] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	slices.Sort(out)

	return out
}

package ocicore

import "slices"

// Compartments lists the distinct compartments that hold live resources.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}

	for _, r := range b.res {
		if live(r) {
			seen[r.base().CompartmentID] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	slices.Sort(out)

	return out
}

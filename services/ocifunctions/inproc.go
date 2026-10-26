package ocifunctions

import "sort"

// Compartments lists the distinct compartments that hold applications or work requests.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]bool{}

	for _, a := range b.apps {
		if a.State != stateDeleted {
			seen[a.CompartmentID] = true
		}
	}

	for _, wr := range b.workReqs {
		seen[wr.CompartmentID] = true
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	sort.Strings(out)

	return out
}

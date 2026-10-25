package ocivault

import "sort"

// Compartments lists the distinct compartments that hold vaults, keys or secrets.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]bool{}

	for _, v := range b.vaults {
		seen[v.CompartmentID] = true
	}

	for _, k := range b.keys {
		seen[k.CompartmentID] = true
	}

	for _, s := range b.secrets {
		seen[s.CompartmentID] = true
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	sort.Strings(out)

	return out
}

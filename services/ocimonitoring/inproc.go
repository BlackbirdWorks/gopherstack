package ocimonitoring

import "slices"

// Compartments lists the distinct compartments that hold alarms or metric streams.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}

	for _, a := range b.alarms {
		seen[a.CompartmentID] = struct{}{}
		seen[a.MetricCompartmentID] = struct{}{}
	}

	for _, s := range b.streams {
		seen[s.CompartmentID] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	slices.Sort(out)

	return out
}

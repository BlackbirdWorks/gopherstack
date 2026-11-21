package ocistreaming

import "slices"

// HoldsInCompartment names a stream pool, stream or connect harness held in the compartment, or returns "".
func (b *InMemoryBackend) HoldsInCompartment(compartmentID string) string {
	b.mu.RLock("HoldsInCompartment")
	defer b.mu.RUnlock()

	var found []string

	for _, p := range b.pools {
		if p.CompartmentID == compartmentID && p.State != stateDeleted {
			found = append(found, "Streaming pool "+p.ID)
		}
	}

	for _, s := range b.streams {
		if s.CompartmentID == compartmentID && s.State != stateDeleted {
			found = append(found, "Streaming stream "+s.ID)
		}
	}

	for _, h := range b.harnesses {
		if h.CompartmentID == compartmentID && h.State != stateDeleted {
			found = append(found, "Streaming connect harness "+h.ID)
		}
	}

	if len(found) == 0 {
		return ""
	}

	return slices.Min(found)
}

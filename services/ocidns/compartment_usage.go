package ocidns

import "slices"

// HoldsInCompartment names a zone, view, TSIG key or steering policy held in the compartment, or returns "".
func (b *InMemoryBackend) HoldsInCompartment(compartmentID string) string {
	b.mu.RLock("HoldsInCompartment")
	defer b.mu.RUnlock()

	var found []string

	for _, z := range b.zones {
		if z.CompartmentID == compartmentID {
			found = append(found, "DNS zone "+z.Name)
		}
	}

	for _, v := range b.views {
		if v.CompartmentID == compartmentID {
			found = append(found, "DNS view "+v.ID)
		}
	}

	for _, k := range b.tsigs {
		if k.CompartmentID == compartmentID {
			found = append(found, "DNS TSIG key "+k.ID)
		}
	}

	for _, p := range b.policies {
		if p.CompartmentID == compartmentID {
			found = append(found, "DNS steering policy "+p.ID)
		}
	}

	if len(found) == 0 {
		return ""
	}

	return slices.Min(found)
}

package ociidentity

// CompartmentActive reports whether id is the tenancy or an ACTIVE compartment.
func (b *InMemoryBackend) CompartmentActive(id string) bool {
	b.mu.RLock("CompartmentActive")
	defer b.mu.RUnlock()

	if id == TenancyID {
		return true
	}

	c, ok := b.compartment[id]

	return ok && c.LifecycleState == stateActive
}

// CompartmentWithin reports whether id equals ancestor or is nested below it; the tenancy contains everything.
func (b *InMemoryBackend) CompartmentWithin(id, ancestor string) bool {
	b.mu.RLock("CompartmentWithin")
	defer b.mu.RUnlock()

	if ancestor == TenancyID {
		return true
	}

	for hops := 0; id != "" && hops <= maxCompartmentHops; hops++ {
		if id == ancestor {
			return true
		}

		c, ok := b.compartment[id]
		if !ok {
			return false
		}

		id = c.CompartmentID
	}

	return false
}

const maxCompartmentHops = 64

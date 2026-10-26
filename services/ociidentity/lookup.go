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

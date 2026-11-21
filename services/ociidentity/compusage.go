package ociidentity

// CompartmentUsage names a resource another service still holds in a compartment, or returns "".
type CompartmentUsage func(compartmentID string) string

// AddCompartmentUsage registers a service whose resources block compartment deletion.
func (b *InMemoryBackend) AddCompartmentUsage(fn CompartmentUsage) {
	b.mu.Lock("AddCompartmentUsage")
	b.usage = append(b.usage, fn)
	b.mu.Unlock()
}

func (b *InMemoryBackend) usageFuncs() []CompartmentUsage {
	b.mu.RLock("usageFuncs")
	defer b.mu.RUnlock()

	return append([]CompartmentUsage(nil), b.usage...)
}

// foreignUsage asks the other services without holding the backend lock.
func (b *InMemoryBackend) foreignUsage(id string) string {
	for _, fn := range b.usageFuncs() {
		if what := fn(id); what != "" {
			return what
		}
	}

	return ""
}

// blockedCompartments finds the DELETING compartments that are due and now hold foreign resources.
func (b *InMemoryBackend) blockedCompartments() map[string]string {
	fns := b.usageFuncs()
	if len(fns) == 0 {
		return nil
	}

	b.mu.RLock("blockedCompartments")

	var due []string

	now := b.nowUTC()

	for _, c := range b.compartment {
		if c.LifecycleState == stateDeleting && c.Internal.TransitionAt != nil && !c.Internal.TransitionAt.After(now) {
			due = append(due, c.ID)
		}
	}

	b.mu.RUnlock()

	blocked := map[string]string{}

	for _, id := range due {
		if what := b.foreignUsage(id); what != "" {
			blocked[id] = what
		}
	}

	return blocked
}

// abortCompartmentDelete returns a compartment to ACTIVE and fails its work request; callers hold the write lock.
func (b *InMemoryBackend) abortCompartmentDelete(c *Compartment, what string) {
	c.LifecycleState, c.Internal.TransitionAt, c.Internal.Etag = stateActive, nil, newEtag()

	for _, wr := range b.workReqs {
		if wr.Operation == wrDeleteCompartment && wr.EntityID == c.ID && wr.FailCode == "" {
			wr.FailCode, wr.FailMessage = "Conflict", "Compartment "+c.ID+" is not empty: it still contains "+what
			wr.FailedAt = b.nowUTC()
		}
	}
}

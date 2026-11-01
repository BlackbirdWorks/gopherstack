package ociapigateway

import "slices"

// Compartments lists the distinct compartments that hold gateways, deployments or certificates.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}

	for _, g := range b.gateways {
		if g.State != stateDeleted {
			seen[g.CompartmentID] = struct{}{}
		}
	}

	for _, d := range b.deployments {
		if d.State != stateDeleted {
			seen[d.CompartmentID] = struct{}{}
		}
	}

	for _, c := range b.certificates {
		if c.State != stateDeleted {
			seen[c.CompartmentID] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	slices.Sort(out)

	return out
}

// GatewayState returns the lifecycle state of a gateway.
func (b *InMemoryBackend) GatewayState(id string) (string, bool) {
	b.mu.RLock("GatewayState")
	defer b.mu.RUnlock()

	g, ok := b.gateways[id]
	if !ok || g.State == stateDeleted {
		return "", false
	}

	return g.State, true
}

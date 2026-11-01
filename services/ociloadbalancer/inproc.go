package ociloadbalancer

import "slices"

// Compartments lists the distinct compartments that hold load balancers.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}

	for _, lb := range b.lbs {
		if lb.State != stateDeleted {
			seen[lb.CompartmentID] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	slices.Sort(out)

	return out
}

// LoadBalancerState returns the lifecycle state of a load balancer.
func (b *InMemoryBackend) LoadBalancerState(id string) (string, bool) {
	b.mu.RLock("LoadBalancerState")
	defer b.mu.RUnlock()

	lb, ok := b.lbs[id]
	if !ok || lb.State == stateDeleted {
		return "", false
	}

	return lb.State, true
}

// Target is the data-plane endpoint of a serving listener.
type Target struct {
	Address  string
	Protocol string
	TLS      bool
}

// ListenerTarget returns the loopback data-plane address of a serving listener of an ACTIVE load balancer.
func (b *InMemoryBackend) ListenerTarget(lbID, listener string) (Target, bool) {
	b.mu.RLock("ListenerTarget")
	defer b.mu.RUnlock()

	lb, ok := b.lbs[lbID]
	if !ok || lb.State != stateActive {
		return Target{}, false
	}

	l, ok := lb.Listeners[listener]
	if !ok {
		return Target{}, false
	}

	addr := b.dp.address(lb.ID, l.Port)
	if addr == "" {
		return Target{}, false
	}

	return Target{Address: addr, Protocol: l.Protocol, TLS: l.SSL != nil}, true
}

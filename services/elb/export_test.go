package elb

// LoadBalancerCount returns the number of load balancers. Used only in tests.
func (b *InMemoryBackend) LoadBalancerCount() int {
	b.mu.RLock("LoadBalancerCount")
	defer b.mu.RUnlock()

	return len(b.lbs)
}

// PolicyCount returns the total number of policies across all load balancers. Used only in tests.
func (b *InMemoryBackend) PolicyCount() int {
	b.mu.RLock("PolicyCount")
	defer b.mu.RUnlock()

	return len(b.policies)
}

// HandlerOpsLen returns the number of registered operations in the handler dispatch table.
// Used only in tests to verify the dispatch table is pre-built.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}

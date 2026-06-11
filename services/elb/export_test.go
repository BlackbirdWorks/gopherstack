package elb

import "context"

// LoadBalancerCount returns the total number of load balancers across all
// regions. Used only in tests.
func (b *InMemoryBackend) LoadBalancerCount() int {
	b.mu.RLock("LoadBalancerCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionLBs := range b.lbs {
		total += len(regionLBs)
	}

	return total
}

// PolicyCount returns the total number of policies across all load balancers
// and regions. Used only in tests.
func (b *InMemoryBackend) PolicyCount() int {
	b.mu.RLock("PolicyCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionPolicies := range b.policies {
		total += len(regionPolicies)
	}

	return total
}

// RegionContextForTest returns a context carrying the given region under the
// unexported regionContextKey, for use by tests in this package.
func RegionContextForTest(ctx context.Context, region string) context.Context {
	return context.WithValue(ctx, regionContextKey{}, region)
}

// HandlerOpsLen returns the number of registered operations in the handler dispatch table.
// Used only in tests to verify the dispatch table is pre-built.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}

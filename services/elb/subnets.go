package elb

import (
	"context"
	"fmt"
	"sort"
)

// AttachLoadBalancerToSubnets adds subnets to an existing load balancer.
func (b *InMemoryBackend) AttachLoadBalancerToSubnets(
	ctx context.Context, name string, subnets []string,
) ([]string, error) {
	b.mu.Lock("AttachLoadBalancerToSubnets")
	defer b.mu.Unlock()

	lb, ok := b.lbs.Get(lbTableKey(getRegion(ctx, b.region), name))
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	if !lb.IsVPC {
		return nil, fmt.Errorf("%w: cannot attach subnets to an EC2-Classic load balancer", ErrInvalidConfiguration)
	}

	existing := make(map[string]bool, len(lb.Subnets))
	for _, s := range lb.Subnets {
		existing[s] = true
	}

	for _, s := range subnets {
		if !existing[s] {
			lb.Subnets = append(lb.Subnets, s)
			existing[s] = true
		}
	}

	result := make([]string, len(lb.Subnets))
	copy(result, lb.Subnets)
	sort.Strings(result)

	return result, nil
}

// DetachLoadBalancerFromSubnets removes subnets from an existing load balancer.
func (b *InMemoryBackend) DetachLoadBalancerFromSubnets(
	ctx context.Context, name string, subnets []string,
) ([]string, error) {
	b.mu.Lock("DetachLoadBalancerFromSubnets")
	defer b.mu.Unlock()

	lb, ok := b.lbs.Get(lbTableKey(getRegion(ctx, b.region), name))
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	remove := make(map[string]bool, len(subnets))
	for _, s := range subnets {
		remove[s] = true
	}

	kept := lb.Subnets[:0]
	for _, s := range lb.Subnets {
		if !remove[s] {
			kept = append(kept, s)
		}
	}

	lb.Subnets = kept

	result := make([]string, len(lb.Subnets))
	copy(result, lb.Subnets)
	sort.Strings(result)

	return result, nil
}

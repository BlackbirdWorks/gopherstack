package elb

import (
	"context"
	"fmt"
)

// ModifyLoadBalancerAttributes updates the tunable attributes for a load balancer.
func (b *InMemoryBackend) ModifyLoadBalancerAttributes(
	ctx context.Context,
	name string,
	attrs LoadBalancerAttributes,
) (*LoadBalancerAttributes, error) {
	b.mu.Lock("ModifyLoadBalancerAttributes")
	defer b.mu.Unlock()

	lb, ok := b.lbs.Get(lbTableKey(getRegion(ctx, b.region), name))
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	lb.Attributes = attrs
	cp := attrs

	return &cp, nil
}

// DescribeLoadBalancerAttributes returns the tunable attributes for a load balancer.
func (b *InMemoryBackend) DescribeLoadBalancerAttributes(
	ctx context.Context, name string,
) (*LoadBalancerAttributes, error) {
	b.mu.RLock("DescribeLoadBalancerAttributes")
	defer b.mu.RUnlock()

	lb, ok := b.lbs.Get(lbTableKey(getRegion(ctx, b.region), name))
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	cp := lb.Attributes

	return &cp, nil
}

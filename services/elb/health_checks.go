package elb

import (
	"context"
	"fmt"
)

// ConfigureHealthCheck sets the health-check configuration on a load balancer.
func (b *InMemoryBackend) ConfigureHealthCheck(
	ctx context.Context, name string, hc HealthCheck,
) (*HealthCheck, error) {
	b.mu.Lock("ConfigureHealthCheck")
	defer b.mu.Unlock()

	lb, ok := b.lbs.Get(lbTableKey(getRegion(ctx, b.region), name))
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	lb.HealthCheck = &hc
	cp := hc

	return &cp, nil
}

package elb

import (
	"context"
	"fmt"
	"sort"
)

// EnableAvailabilityZonesForLoadBalancer adds availability zones to an existing load balancer.
func (b *InMemoryBackend) EnableAvailabilityZonesForLoadBalancer(
	ctx context.Context, name string, azs []string,
) ([]string, error) {
	b.mu.Lock("EnableAvailabilityZonesForLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs.Get(lbTableKey(getRegion(ctx, b.region), name))
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	if lb.IsVPC {
		return nil, fmt.Errorf(
			"%w: cannot enable availability zones on a VPC load balancer; use AttachLoadBalancerToSubnets instead",
			ErrInvalidConfiguration,
		)
	}

	existing := make(map[string]bool, len(lb.AvailabilityZones))
	for _, az := range lb.AvailabilityZones {
		existing[az] = true
	}

	for _, az := range azs {
		if !existing[az] {
			lb.AvailabilityZones = append(lb.AvailabilityZones, az)
			existing[az] = true
		}
	}

	result := make([]string, len(lb.AvailabilityZones))
	copy(result, lb.AvailabilityZones)
	sort.Strings(result)

	return result, nil
}

// DisableAvailabilityZonesForLoadBalancer removes availability zones from an existing load balancer.
func (b *InMemoryBackend) DisableAvailabilityZonesForLoadBalancer(
	ctx context.Context, name string, azs []string,
) ([]string, error) {
	b.mu.Lock("DisableAvailabilityZonesForLoadBalancer")
	defer b.mu.Unlock()

	lb, ok := b.lbs.Get(lbTableKey(getRegion(ctx, b.region), name))
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrLoadBalancerNotFound, name)
	}

	// No-op when no AZs provided.
	if len(azs) == 0 {
		result := make([]string, len(lb.AvailabilityZones))
		copy(result, lb.AvailabilityZones)
		sort.Strings(result)

		return result, nil
	}

	remove := make(map[string]bool, len(azs))
	for _, az := range azs {
		remove[az] = true
	}

	kept := make([]string, 0, len(lb.AvailabilityZones))
	for _, az := range lb.AvailabilityZones {
		if !remove[az] {
			kept = append(kept, az)
		}
	}

	if len(kept) == 0 {
		return nil, fmt.Errorf(
			"%w: cannot remove all availability zones; at least one must remain",
			ErrInvalidParameter,
		)
	}

	lb.AvailabilityZones = kept

	result := make([]string, len(lb.AvailabilityZones))
	copy(result, lb.AvailabilityZones)
	sort.Strings(result)

	return result, nil
}

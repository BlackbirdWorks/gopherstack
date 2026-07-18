package elbv2

import (
	"time"
)

const defaultDecreaseRequestsRemaining = 5

// ModifyCapacityReservation persists capacity reservation state on a load balancer.
func (b *InMemoryBackend) ModifyCapacityReservation(
	lbArn string, minimumCapacityUnits *int32, reset bool,
) (*CapacityReservation, error) {
	b.mu.Lock("ModifyCapacityReservation")
	defer b.mu.Unlock()

	lb, ok := b.loadBalancers.Get(lbArn)
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	if reset {
		lb.CapacityReservation = nil

		return &CapacityReservation{
			DecreaseRequestsRemaining: defaultDecreaseRequestsRemaining,
			LastModifiedTime:          time.Now().UTC(),
		}, nil
	}

	cr := lb.CapacityReservation
	if cr == nil {
		cr = &CapacityReservation{DecreaseRequestsRemaining: defaultDecreaseRequestsRemaining}
	}

	if minimumCapacityUnits != nil {
		// A decrease consumes one of the daily decrease requests.
		if *minimumCapacityUnits < cr.MinimumCapacityUnits && cr.DecreaseRequestsRemaining > 0 {
			cr.DecreaseRequestsRemaining--
		}

		cr.MinimumCapacityUnits = *minimumCapacityUnits
	}

	cr.LastModifiedTime = time.Now().UTC()
	lb.CapacityReservation = cr

	cp := *cr

	return &cp, nil
}

// DescribeCapacityReservation returns the capacity reservation state for a load balancer.
func (b *InMemoryBackend) DescribeCapacityReservation(lbArn string) (*CapacityReservation, error) {
	b.mu.RLock("DescribeCapacityReservation")
	defer b.mu.RUnlock()

	lb, ok := b.loadBalancers.Get(lbArn)
	if !ok {
		return nil, ErrLoadBalancerNotFound
	}

	if lb.CapacityReservation == nil {
		return &CapacityReservation{
			DecreaseRequestsRemaining: defaultDecreaseRequestsRemaining,
		}, nil
	}

	cp := *lb.CapacityReservation

	return &cp, nil
}

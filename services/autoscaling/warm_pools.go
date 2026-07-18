package autoscaling

import "fmt"

// PutWarmPool creates or updates a warm pool configuration for the ASG.
func (b *InMemoryBackend) PutWarmPool(input WarmPoolInput) error {
	b.mu.Lock("PutWarmPool")
	defer b.mu.Unlock()

	if !b.groups.Has(input.AutoScalingGroupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	poolState := input.PoolState
	if poolState == "" {
		poolState = "Stopped"
	} else if poolState != "Stopped" && poolState != "Running" && poolState != "Hibernated" {
		return fmt.Errorf("%w: PoolState must be one of Stopped, Running, Hibernated; got %q",
			ErrInvalidParameter, poolState)
	}

	b.warmPools.Put(&WarmPool{
		AutoScalingGroupName:     input.AutoScalingGroupName,
		PoolState:                poolState,
		MinSize:                  input.MinSize,
		MaxGroupPreparedCapacity: input.MaxGroupPreparedCapacity,
		InstanceReusePolicy:      input.InstanceReusePolicy,
	})

	return nil
}

// DeleteWarmPool removes the warm pool for the ASG.
func (b *InMemoryBackend) DeleteWarmPool(groupName string) error {
	b.mu.Lock("DeleteWarmPool")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	b.warmPools.Delete(groupName)

	return nil
}

// DescribeWarmPool returns the warm pool configuration for the ASG.
func (b *InMemoryBackend) DescribeWarmPool(groupName string) (*WarmPool, error) {
	b.mu.RLock("DescribeWarmPool")
	defer b.mu.RUnlock()

	if !b.groups.Has(groupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	wp, ok := b.warmPools.Get(groupName)
	if !ok {
		return nil, fmt.Errorf("%w: no warm pool found for group %q", ErrWarmPoolNotFound, groupName)
	}

	cp := *wp

	return &cp, nil
}

package opsworks

// SetTimeBasedAutoScaling sets the time-based auto-scaling schedule for an instance.
func (b *InMemoryBackend) SetTimeBasedAutoScaling(instanceID string, schedule *AutoScalingSchedule) error {
	b.mu.Lock("SetTimeBasedAutoScaling")
	defer b.mu.Unlock()

	if !b.instances.Has(instanceID) {
		return ErrInstanceNotFound
	}

	b.timeBasedAutoScale.Put(&storedTimeBasedAutoScaling{
		AutoScalingSchedule: schedule,
		InstanceID:          instanceID,
	})

	return nil
}

// DescribeTimeBasedAutoScaling returns time-based auto-scaling config for instances.
func (b *InMemoryBackend) DescribeTimeBasedAutoScaling(instanceIDs []string) ([]*TimeBasedAutoScaling, error) {
	b.mu.RLock("DescribeTimeBasedAutoScaling")
	defer b.mu.RUnlock()

	result := make([]*TimeBasedAutoScaling, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		t, ok := b.timeBasedAutoScale.Get(id)
		if !ok {
			// Return a record with empty schedule if not configured.
			result = append(result, &TimeBasedAutoScaling{
				AutoScalingSchedule: &AutoScalingSchedule{},
				InstanceID:          id,
			})

			continue
		}
		result = append(result, t.toTimeBasedAutoScaling())
	}

	return result, nil
}

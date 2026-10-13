package ecs

import "fmt"

// AutoScalingGroupResolver reports whether an Auto Scaling group (by ARN or name) exists.
type AutoScalingGroupResolver interface {
	AutoScalingGroupExists(arnOrName string) bool
}

// SetAutoScalingGroupResolver wires the lookup CreateCapacityProvider uses to validate AutoScalingGroupArn.
func (b *InMemoryBackend) SetAutoScalingGroupResolver(r AutoScalingGroupResolver) {
	b.mu.Lock("SetAutoScalingGroupResolver")
	defer b.mu.Unlock()

	b.asgResolver = r
}

func (b *InMemoryBackend) validateAutoScalingGroupLocked(p *AutoScalingGroupProvider) error {
	if p == nil || b.asgResolver == nil || p.AutoScalingGroupArn == "" {
		return nil
	}

	if !b.asgResolver.AutoScalingGroupExists(p.AutoScalingGroupArn) {
		return fmt.Errorf("%w: Auto Scaling group %s does not exist", ErrInvalidParameter, p.AutoScalingGroupArn)
	}

	return nil
}

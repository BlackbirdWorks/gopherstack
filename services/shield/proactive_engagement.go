package shield

import "fmt"

// GetProactiveEngagementStatus returns the current proactive engagement status.
func (b *InMemoryBackend) GetProactiveEngagementStatus() string {
	b.mu.RLock("GetProactiveEngagementStatus")
	defer b.mu.RUnlock()

	return b.proactiveEngagementStatus
}

// EnableProactiveEngagement enables proactive engagement for the subscription.
// Requires at least one emergency contact to be configured.
func (b *InMemoryBackend) EnableProactiveEngagement() error {
	b.mu.Lock("EnableProactiveEngagement")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	if len(b.emergencyContacts) == 0 {
		return fmt.Errorf(
			"%w: EmergencyContactList must be populated before enabling proactive engagement",
			ErrValidation,
		)
	}

	b.proactiveEngagementStatus = ProactiveEngagementEnabled

	return nil
}

// DisableProactiveEngagement disables proactive engagement for the subscription.
func (b *InMemoryBackend) DisableProactiveEngagement() error {
	b.mu.Lock("DisableProactiveEngagement")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	b.proactiveEngagementStatus = ProactiveEngagementDisabled

	return nil
}

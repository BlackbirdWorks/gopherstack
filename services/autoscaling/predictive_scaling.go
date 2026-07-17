package autoscaling

import "fmt"

// GetPredictiveScalingForecast validates the group exists (stub implementation).
func (b *InMemoryBackend) GetPredictiveScalingForecast(groupName string) error {
	b.mu.RLock("GetPredictiveScalingForecast")
	defer b.mu.RUnlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	return nil
}

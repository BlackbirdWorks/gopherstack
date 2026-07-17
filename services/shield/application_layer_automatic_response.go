package shield

import "fmt"

// GetALARConfig returns the ALAR config for a resource ARN, or nil if none.
func (b *InMemoryBackend) GetALARConfig(resourceARN string) *ALARConfig {
	b.mu.RLock("GetALARConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.alarConfigs.Get(resourceARN)
	if !ok {
		return nil
	}

	cp := *cfg

	return &cp
}

// EnableApplicationLayerAutomaticResponse enables ALAR for the given resource ARN.
// action must be "BLOCK" or "COUNT".
func (b *InMemoryBackend) EnableApplicationLayerAutomaticResponse(resourceARN, action string) error {
	b.mu.Lock("EnableApplicationLayerAutomaticResponse")
	defer b.mu.Unlock()

	if b.subscription == nil {
		return fmt.Errorf("%w: Shield Advanced subscription is required", ErrSubscriptionRequired)
	}

	if matches := b.protectionsByResourceARN.Get(resourceARN); len(matches) == 0 {
		return fmt.Errorf("%w: no protection found for resource %q", ErrProtectionNotFound, resourceARN)
	}

	b.alarConfigs.Put(&ALARConfig{ResourceARN: resourceARN, Enabled: true, Action: action})

	return nil
}

// DisableApplicationLayerAutomaticResponse disables ALAR for the given resource ARN.
func (b *InMemoryBackend) DisableApplicationLayerAutomaticResponse(resourceARN string) error {
	b.mu.Lock("DisableApplicationLayerAutomaticResponse")
	defer b.mu.Unlock()

	if matches := b.protectionsByResourceARN.Get(resourceARN); len(matches) == 0 {
		return fmt.Errorf("%w: no protection found for resource %q", ErrProtectionNotFound, resourceARN)
	}

	b.alarConfigs.Delete(resourceARN)

	return nil
}

// UpdateApplicationLayerAutomaticResponse updates the ALAR action for a resource ARN.
func (b *InMemoryBackend) UpdateApplicationLayerAutomaticResponse(resourceARN, action string) error {
	b.mu.Lock("UpdateApplicationLayerAutomaticResponse")
	defer b.mu.Unlock()

	if matches := b.protectionsByResourceARN.Get(resourceARN); len(matches) == 0 {
		return fmt.Errorf("%w: no protection found for resource %q", ErrProtectionNotFound, resourceARN)
	}

	cfg, ok := b.alarConfigs.Get(resourceARN)
	if !ok || !cfg.Enabled {
		return fmt.Errorf(
			"%w: ALAR is not enabled for resource %q; enable it first",
			ErrValidation,
			resourceARN,
		)
	}

	cfg.Action = action

	return nil
}

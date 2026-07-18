package glue

// CreateGlueIdentityCenterConfiguration creates the configuration.
func (b *InMemoryBackend) CreateGlueIdentityCenterConfiguration(instanceARN string) error {
	b.mu.Lock("CreateGlueIdentityCenterConfiguration")
	defer b.mu.Unlock()

	b.glueIdentityCenterConfig = &IdentityCenterConfig{
		InstanceARN: instanceARN,
		Status:      "ENABLED",
	}

	return nil
}

// GetGlueIdentityCenterConfiguration returns the configuration.
func (b *InMemoryBackend) GetGlueIdentityCenterConfiguration() (*IdentityCenterConfig, error) {
	b.mu.RLock("GetGlueIdentityCenterConfiguration")
	defer b.mu.RUnlock()

	if b.glueIdentityCenterConfig == nil {
		return &IdentityCenterConfig{Status: "DISABLED"}, nil
	}

	cp := *b.glueIdentityCenterConfig

	return &cp, nil
}

// UpdateGlueIdentityCenterConfiguration updates the configuration.
func (b *InMemoryBackend) UpdateGlueIdentityCenterConfiguration(instanceARN string) error {
	b.mu.Lock("UpdateGlueIdentityCenterConfiguration")
	defer b.mu.Unlock()

	if b.glueIdentityCenterConfig == nil {
		b.glueIdentityCenterConfig = &IdentityCenterConfig{}
	}

	b.glueIdentityCenterConfig.InstanceARN = instanceARN

	return nil
}

// DeleteGlueIdentityCenterConfiguration removes the configuration.
func (b *InMemoryBackend) DeleteGlueIdentityCenterConfiguration() error {
	b.mu.Lock("DeleteGlueIdentityCenterConfiguration")
	defer b.mu.Unlock()

	b.glueIdentityCenterConfig = nil

	return nil
}

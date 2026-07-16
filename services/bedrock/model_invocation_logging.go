package bedrock

// GetModelInvocationLoggingConfiguration returns the current logging configuration.
func (b *InMemoryBackend) GetModelInvocationLoggingConfiguration() *ModelInvocationLoggingConfiguration {
	b.mu.RLock("GetModelInvocationLoggingConfiguration")
	defer b.mu.RUnlock()

	if b.loggingConfig == nil {
		return &ModelInvocationLoggingConfiguration{}
	}

	cp := *b.loggingConfig

	return &cp
}

// PutModelInvocationLoggingConfiguration sets the logging configuration.
func (b *InMemoryBackend) PutModelInvocationLoggingConfiguration(
	cfg *ModelInvocationLoggingConfiguration,
) {
	b.mu.Lock("PutModelInvocationLoggingConfiguration")
	defer b.mu.Unlock()

	cp := *cfg
	b.loggingConfig = &cp
}

// DeleteModelInvocationLoggingConfiguration removes the logging configuration.
func (b *InMemoryBackend) DeleteModelInvocationLoggingConfiguration() {
	b.mu.Lock("DeleteModelInvocationLoggingConfiguration")
	defer b.mu.Unlock()

	b.loggingConfig = nil
}

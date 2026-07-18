package iotwireless

// GetLogLevelsByResourceTypes returns the default log level settings.
func (b *InMemoryBackend) GetLogLevelsByResourceTypes() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if level, ok := b.logLevels["default"]; ok {
		return level
	}

	return "INFO"
}

// UpdateLogLevelsByResourceTypes updates the default log level.
func (b *InMemoryBackend) UpdateLogLevelsByResourceTypes(defaultLogLevel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.logLevels["default"] = defaultLogLevel

	return nil
}

// ResetAllResourceLogLevels clears all resource-level log level overrides.
func (b *InMemoryBackend) ResetAllResourceLogLevels() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resourceLogLevels = make(map[string]string)

	return nil
}

// GetResourceLogLevel returns the log level for a specific resource.
func (b *InMemoryBackend) GetResourceLogLevel(resourceID string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if level, ok := b.resourceLogLevels[resourceID]; ok {
		return level
	}

	return "INFO"
}

// PutResourceLogLevel sets the log level for a specific resource.
func (b *InMemoryBackend) PutResourceLogLevel(resourceID, logLevel string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resourceLogLevels[resourceID] = logLevel

	return nil
}

// ResetResourceLogLevel clears the log level override for a specific resource.
func (b *InMemoryBackend) ResetResourceLogLevel(resourceID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.resourceLogLevels, resourceID)

	return nil
}

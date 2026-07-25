package iotwireless

// LogLevelsConfig is the account-wide default log-level configuration set
// via UpdateLogLevelsByResourceTypes. FuotaTaskLogOptions/
// WirelessDeviceLogOptions/WirelessGatewayLogOptions hold the request's
// nested log-option-list objects verbatim (see the WirelessDevice.LoRaWAN
// doc comment in models.go for why opaque storage is used for these
// client-submitted nested configs) -- previously these three fields were
// silently accepted by UpdateLogLevelsByResourceTypesInput and dropped,
// always echoing back empty arrays regardless of what was set.
type LogLevelsConfig struct {
	DefaultLogLevel           string
	FuotaTaskLogOptions       []map[string]any
	WirelessDeviceLogOptions  []map[string]any
	WirelessGatewayLogOptions []map[string]any
}

// GetLogLevelsByResourceTypes returns the account-wide default log-level
// configuration. Defaults to DefaultLogLevel "INFO" with empty option lists
// when never explicitly configured.
func (b *InMemoryBackend) GetLogLevelsByResourceTypes() LogLevelsConfig {
	b.mu.RLock("GetLogLevelsByResourceTypes")
	defer b.mu.RUnlock()

	if b.logLevelsConfig == nil {
		return LogLevelsConfig{DefaultLogLevel: "INFO"}
	}

	return *b.logLevelsConfig
}

// UpdateLogLevelsByResourceTypes replaces the account-wide default
// log-level configuration.
func (b *InMemoryBackend) UpdateLogLevelsByResourceTypes(cfg LogLevelsConfig) error {
	b.mu.Lock("UpdateLogLevelsByResourceTypes")
	defer b.mu.Unlock()

	cp := cfg
	b.logLevelsConfig = &cp

	return nil
}

// ResetAllResourceLogLevels clears all resource-level log level overrides.
func (b *InMemoryBackend) ResetAllResourceLogLevels() error {
	b.mu.Lock("ResetAllResourceLogLevels")
	defer b.mu.Unlock()

	b.resourceLogLevels = make(map[string]string)

	return nil
}

// GetResourceLogLevel returns the log level for a specific resource.
func (b *InMemoryBackend) GetResourceLogLevel(resourceID string) string {
	b.mu.RLock("GetResourceLogLevel")
	defer b.mu.RUnlock()

	if level, ok := b.resourceLogLevels[resourceID]; ok {
		return level
	}

	return "INFO"
}

// PutResourceLogLevel sets the log level for a specific resource.
func (b *InMemoryBackend) PutResourceLogLevel(resourceID, logLevel string) error {
	b.mu.Lock("PutResourceLogLevel")
	defer b.mu.Unlock()

	b.resourceLogLevels[resourceID] = logLevel

	return nil
}

// ResetResourceLogLevel clears the log level override for a specific resource.
func (b *InMemoryBackend) ResetResourceLogLevel(resourceID string) error {
	b.mu.Lock("ResetResourceLogLevel")
	defer b.mu.Unlock()

	delete(b.resourceLogLevels, resourceID)

	return nil
}

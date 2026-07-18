package waf

import "sort"

// PutLoggingConfiguration stores a logging configuration for a WebACL.
func (b *InMemoryBackend) PutLoggingConfiguration(config LoggingConfiguration) (*LoggingConfiguration, error) {
	b.mu.Lock("PutLoggingConfiguration")
	defer b.mu.Unlock()

	stored := config
	b.loggingConfigs.Put(&stored)

	return &stored, nil
}

// GetLoggingConfiguration retrieves the logging configuration for a WebACL ARN.
func (b *InMemoryBackend) GetLoggingConfiguration(resourceArn string) (*LoggingConfiguration, error) {
	b.mu.RLock("GetLoggingConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.loggingConfigs.Get(resourceArn)
	if !ok {
		return nil, ErrNotFound
	}

	return cfg, nil
}

// DeleteLoggingConfiguration removes the logging configuration for a WebACL ARN.
func (b *InMemoryBackend) DeleteLoggingConfiguration(resourceArn string) error {
	b.mu.Lock("DeleteLoggingConfiguration")
	defer b.mu.Unlock()

	if !b.loggingConfigs.Has(resourceArn) {
		return ErrNotFound
	}

	b.loggingConfigs.Delete(resourceArn)

	return nil
}

// ListLoggingConfigurations returns all logging configurations.
func (b *InMemoryBackend) ListLoggingConfigurations() []LoggingConfiguration {
	b.mu.RLock("ListLoggingConfigurations")
	defer b.mu.RUnlock()

	all := b.loggingConfigs.All()
	result := make([]LoggingConfiguration, 0, len(all))
	for _, cfg := range all {
		result = append(result, *cfg)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ResourceArn < result[j].ResourceArn })

	return result
}

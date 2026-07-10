package mq

// BrokerCount returns the number of brokers in the backend.
func BrokerCount(b *InMemoryBackend) int {
	b.mu.RLock("BrokerCount")
	defer b.mu.RUnlock()

	return b.brokers.Len()
}

// ConfigurationCount returns the number of configurations in the backend.
func ConfigurationCount(b *InMemoryBackend) int {
	b.mu.RLock("ConfigurationCount")
	defer b.mu.RUnlock()

	return b.configurations.Len()
}

// TagCount returns the number of tag-tracked ARNs in the backend.
func TagCount(b *InMemoryBackend) int {
	b.mu.RLock("TagCount")
	defer b.mu.RUnlock()

	return len(b.tags)
}

// HandlerOpsLen returns the number of supported operations in the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// AddBrokerInternal inserts a broker directly into the backend, bypassing
// validation. Useful for seeding test state.
func AddBrokerInternal(b *InMemoryBackend, br *Broker) {
	b.mu.Lock("AddBrokerInternal")
	defer b.mu.Unlock()

	if br.Tags == nil {
		br.Tags = make(map[string]string)
	}

	if br.Users == nil {
		br.Users = make(map[string]*User)
	}

	b.brokers.Put(br)
	b.tags[br.BrokerArn] = br.Tags
}

// AddConfigurationInternal inserts a configuration directly into the backend,
// bypassing validation. Useful for seeding test state.
func AddConfigurationInternal(b *InMemoryBackend, cfg *Configuration) {
	b.mu.Lock("AddConfigurationInternal")
	defer b.mu.Unlock()

	if cfg.Tags == nil {
		cfg.Tags = make(map[string]string)
	}

	if cfg.Data == nil {
		cfg.Data = make(map[int32]string)
	}

	b.configurations.Put(cfg)
	b.tags[cfg.Arn] = cfg.Tags
}

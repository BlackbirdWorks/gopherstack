package iotdataplane

// ShadowCount returns the total number of shadow entries across all things (for white-box testing).
func ShadowCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	total := 0
	for _, thingShadows := range b.shadows {
		total += len(thingShadows)
	}

	return total
}

// ThingCount returns the number of distinct things with at least one shadow (for white-box testing).
func ThingCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.shadows)
}

// RetainedMessageCount returns the number of retained messages (for white-box testing).
func RetainedMessageCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.retainedMessages)
}

// ConnectionCount returns the number of tracked connections (for white-box testing).
func ConnectionCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.connections)
}

package mediapackage

// ChannelCount returns the number of stored channels.
func ChannelCount(b *InMemoryBackend) int {
	b.mu.RLock("ChannelCount")
	defer b.mu.RUnlock()

	return len(b.channels)
}

// OriginEndpointCount returns the number of stored origin endpoints.
func OriginEndpointCount(b *InMemoryBackend) int {
	b.mu.RLock("OriginEndpointCount")
	defer b.mu.RUnlock()

	return len(b.originEndpoints)
}

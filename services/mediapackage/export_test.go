package mediapackage

// ChannelCount returns the number of stored channels.
func ChannelCount(b *InMemoryBackend) int {
	b.mu.RLock("ChannelCount")
	defer b.mu.RUnlock()

	return b.channels.Len()
}

// OriginEndpointCount returns the number of stored origin endpoints.
func OriginEndpointCount(b *InMemoryBackend) int {
	b.mu.RLock("OriginEndpointCount")
	defer b.mu.RUnlock()

	return b.originEndpoints.Len()
}

// HarvestJobCount returns the number of stored harvest jobs.
func HarvestJobCount(b *InMemoryBackend) int {
	b.mu.RLock("HarvestJobCount")
	defer b.mu.RUnlock()

	return b.harvestJobs.Len()
}

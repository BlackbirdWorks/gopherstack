package mediatailor

// PlaybackConfigurationCount returns the number of stored playback configurations.
func PlaybackConfigurationCount(b *InMemoryBackend) int {
	b.mu.RLock("PlaybackConfigurationCount")
	defer b.mu.RUnlock()

	return len(b.playbackConfigurations)
}

// ChannelCount returns the number of stored channels.
func ChannelCount(b *InMemoryBackend) int {
	b.mu.RLock("ChannelCount")
	defer b.mu.RUnlock()

	return len(b.channels)
}

// SourceLocationCount returns the number of stored source locations.
func SourceLocationCount(b *InMemoryBackend) int {
	b.mu.RLock("SourceLocationCount")
	defer b.mu.RUnlock()

	return len(b.sourceLocations)
}

// VodSourceCount returns the number of stored VOD sources.
func VodSourceCount(b *InMemoryBackend) int {
	b.mu.RLock("VodSourceCount")
	defer b.mu.RUnlock()

	return len(b.vodSources)
}

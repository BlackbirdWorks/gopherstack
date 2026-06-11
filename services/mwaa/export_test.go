package mwaa

// EnvironmentCount returns the total number of environments in the backend across
// all regions.
func EnvironmentCount(b *InMemoryBackend) int {
	b.mu.RLock("EnvironmentCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionEnvs := range b.environments {
		total += len(regionEnvs)
	}

	return total
}

// EnvironmentCountRegion returns the number of environments in the given region.
func EnvironmentCountRegion(b *InMemoryBackend, region string) int {
	b.mu.RLock("EnvironmentCountRegion")
	defer b.mu.RUnlock()

	return len(b.environments[region])
}

// MetricsCount returns the number of metric data points for an environment in the
// backend's default region.
func MetricsCount(b *InMemoryBackend, envName string) int {
	b.mu.RLock("MetricsCount")
	defer b.mu.RUnlock()

	return len(b.metrics[b.region][envName])
}

// MetricsCapacity returns the capacity of the backing slice for an
// environment's metrics in the backend's default region. Used to verify trimming
// does not retain an oversized backing array (a memory leak even though len() is
// capped).
func MetricsCapacity(b *InMemoryBackend, envName string) int {
	b.mu.RLock("MetricsCapacity")
	defer b.mu.RUnlock()

	return cap(b.metrics[b.region][envName])
}

// ARNIndexSize returns the total number of entries in the ARN index across all regions.
func ARNIndexSize(b *InMemoryBackend) int {
	b.mu.RLock("ARNIndexSize")
	defer b.mu.RUnlock()

	total := 0
	for _, regionIndex := range b.arnIndex {
		total += len(regionIndex)
	}

	return total
}

// HandlerOpsLen returns the number of operations returned by GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

package mwaa

// EnvironmentCount returns the total number of environments in the backend across
// all regions.
func EnvironmentCount(b *InMemoryBackend) int {
	b.mu.RLock("EnvironmentCount")
	defer b.mu.RUnlock()

	return b.environments.Len()
}

// EnvironmentCountRegion returns the number of environments in the given region.
func EnvironmentCountRegion(b *InMemoryBackend, region string) int {
	b.mu.RLock("EnvironmentCountRegion")
	defer b.mu.RUnlock()

	return len(b.environmentsByRegion.Get(region))
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
// The ARN index is now a derived secondary index over the environments table (see
// store_setup.go); since every environment has exactly one ARN, this is equivalent
// to the total environment count.
func ARNIndexSize(b *InMemoryBackend) int {
	b.mu.RLock("ARNIndexSize")
	defer b.mu.RUnlock()

	return b.environments.Len()
}

// HandlerOpsLen returns the number of operations returned by GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

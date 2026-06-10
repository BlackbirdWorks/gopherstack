package mwaa

// EnvironmentCount returns the number of environments in the backend.
func EnvironmentCount(b *InMemoryBackend) int {
	b.mu.RLock("EnvironmentCount")
	defer b.mu.RUnlock()

	return len(b.environments)
}

// MetricsCount returns the number of metric data points for an environment.
func MetricsCount(b *InMemoryBackend, envName string) int {
	b.mu.RLock("MetricsCount")
	defer b.mu.RUnlock()

	return len(b.metrics[envName])
}

// MetricsCapacity returns the capacity of the backing slice for an
// environment's metrics. Used to verify trimming does not retain an oversized
// backing array (a memory leak even though len() is capped).
func MetricsCapacity(b *InMemoryBackend, envName string) int {
	b.mu.RLock("MetricsCapacity")
	defer b.mu.RUnlock()

	return cap(b.metrics[envName])
}

// ARNIndexSize returns the number of entries in the ARN index.
func ARNIndexSize(b *InMemoryBackend) int {
	b.mu.RLock("ARNIndexSize")
	defer b.mu.RUnlock()

	return len(b.arnIndex)
}

// HandlerOpsLen returns the number of operations returned by GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

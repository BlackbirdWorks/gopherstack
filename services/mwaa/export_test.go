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

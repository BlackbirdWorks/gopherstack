package apprunner

// ServiceCount returns the number of stored services.
func ServiceCount(b *InMemoryBackend) int {
	b.mu.RLock("ServiceCount")
	defer b.mu.RUnlock()

	return len(b.services)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// ServiceOperationStats returns the length and capacity of a service's operation
// history slice, for leak-detection tests.
func ServiceOperationStats(b *InMemoryBackend, serviceArn string) (int, int) {
	b.mu.RLock("ServiceOperationStats")
	defer b.mu.RUnlock()

	svc, ok := b.services[serviceArn]
	if !ok {
		return 0, 0
	}

	return len(svc.Operations), cap(svc.Operations)
}

// MaxOperationsPerService exposes the per-service operation cap for tests.
const MaxOperationsPerService = maxOperationsPerService

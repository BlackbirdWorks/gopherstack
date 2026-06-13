package omics

// ReferenceStoreCount returns the number of reference stores in the backend.
func ReferenceStoreCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.region(b.defaultRegion).referenceStores)
}

// SequenceStoreCount returns the number of sequence stores in the backend.
func SequenceStoreCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.region(b.defaultRegion).sequenceStores)
}

// WorkflowCount returns the number of workflows in the backend.
func WorkflowCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.region(b.defaultRegion).workflows)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

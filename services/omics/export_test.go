package omics

// ReferenceStoreCount returns the number of reference stores in the backend.
func ReferenceStoreCount(b *InMemoryBackend) int {
	b.mu.RLock("ReferenceStoreCount")
	defer b.mu.RUnlock()

	return b.referenceStores.Len()
}

// SequenceStoreCount returns the number of sequence stores in the backend.
func SequenceStoreCount(b *InMemoryBackend) int {
	b.mu.RLock("SequenceStoreCount")
	defer b.mu.RUnlock()

	return b.sequenceStores.Len()
}

// WorkflowCount returns the number of workflows in the backend.
func WorkflowCount(b *InMemoryBackend) int {
	b.mu.RLock("WorkflowCount")
	defer b.mu.RUnlock()

	return b.workflows.Len()
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

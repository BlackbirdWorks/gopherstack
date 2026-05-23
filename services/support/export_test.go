package support

// CaseCount returns the number of cases in the backend.
func CaseCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.cases)
}

// AttachmentCount returns the number of attachments in the backend.
func AttachmentCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.attachments)
}

// CheckRefreshStatusCount returns the number of tracked refresh statuses.
func CheckRefreshStatusCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.checkRefreshStatuses)
}

// CommunicationCount returns the total number of communications across all cases.
func CommunicationCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	total := 0

	for _, comms := range b.communications {
		total += len(comms)
	}

	return total
}

// HandlerOpsLen returns the number of supported operations in the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

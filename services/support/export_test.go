package support

// CaseCount returns the number of cases in the backend.
func CaseCount(b *InMemoryBackend) int {
	b.mu.RLock("CaseCount")
	defer b.mu.RUnlock()

	return b.cases.Len()
}

// AttachmentCount returns the number of attachments in the backend.
func AttachmentCount(b *InMemoryBackend) int {
	b.mu.RLock("AttachmentCount")
	defer b.mu.RUnlock()

	return b.attachments.Len()
}

// CheckRefreshStatusCount returns the number of tracked refresh statuses.
func CheckRefreshStatusCount(b *InMemoryBackend) int {
	b.mu.RLock("CheckRefreshStatusCount")
	defer b.mu.RUnlock()

	return b.checkRefreshStatuses.Len()
}

// CommunicationCount returns the total number of communications across all cases.
func CommunicationCount(b *InMemoryBackend) int {
	b.mu.RLock("CommunicationCount")
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

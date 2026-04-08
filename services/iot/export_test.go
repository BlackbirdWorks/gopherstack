package iot

// ThingCount returns the number of Things in the backend.
// Used only in tests to verify backend state.
func (b *InMemoryBackend) ThingCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.things)
}

// PolicyCount returns the number of Policies in the backend.
// Used only in tests to verify backend state.
func (b *InMemoryBackend) PolicyCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.policies)
}

// RuleCount returns the number of TopicRules in the backend.
// Used only in tests to verify backend state.
func (b *InMemoryBackend) RuleCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.rules)
}

// CertTransferCount returns the number of certificate transfer records.
// Used only in tests to verify backend state.
func (b *InMemoryBackend) CertTransferCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.certificateTransfers)
}

// HandlerOpsLen returns the number of operations in GetSupportedOperations.
// Used only in tests to assert that the dispatch table is fully populated.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

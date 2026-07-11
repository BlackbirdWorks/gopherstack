package organizations

// AccountCount returns the number of accounts in the backend (including management account).
func AccountCount(b *InMemoryBackend) int {
	b.mu.RLock("AccountCount")
	defer b.mu.RUnlock()

	return b.accounts.Len()
}

// PolicyCount returns the number of policies in the backend.
func PolicyCount(b *InMemoryBackend) int {
	b.mu.RLock("PolicyCount")
	defer b.mu.RUnlock()

	return b.policies.Len()
}

// OUCount returns the number of OUs in the backend.
func OUCount(b *InMemoryBackend) int {
	b.mu.RLock("OUCount")
	defer b.mu.RUnlock()

	return b.ous.Len()
}

// HandshakeCount returns the number of handshakes in the backend.
func HandshakeCount(b *InMemoryBackend) int {
	b.mu.RLock("HandshakeCount")
	defer b.mu.RUnlock()

	return b.handshakes.Len()
}

// TagCount returns the number of tag-tracked resources in the backend.
func TagCount(b *InMemoryBackend) int {
	b.mu.RLock("TagCount")
	defer b.mu.RUnlock()

	return len(b.tags)
}

// HandlerOpsLen returns the number of supported operations in the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// ResolveTargetSummaryExported exposes resolveTargetSummary for white-box testing.
func ResolveTargetSummaryExported(b *InMemoryBackend, targetID string) PolicyTargetSummary {
	b.mu.RLock("ResolveTargetSummaryExported")
	defer b.mu.RUnlock()

	return b.resolveTargetSummary(targetID)
}

// EnsureNonNilMapsExported exposes ensureNonNilMaps for white-box testing.
func EnsureNonNilMapsExported(snap *backendSnapshot) {
	ensureNonNilMaps(snap)
}

// NewBackendSnapshot returns an empty backendSnapshot for testing.
func NewBackendSnapshot() *backendSnapshot {
	return &backendSnapshot{}
}

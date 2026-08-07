package verifiedpermissions

// PolicyStoreCount returns the number of policy stores in b (for test use only).
func PolicyStoreCount(b *InMemoryBackend) int {
	b.mu.RLock("PolicyStoreCount")
	defer b.mu.RUnlock()

	return b.policyStores.Len()
}

// PolicyCount returns the total number of policies across all policy stores (for test use only).
func PolicyCount(b *InMemoryBackend) int {
	b.mu.RLock("PolicyCount")
	defer b.mu.RUnlock()

	return b.policies.Len()
}

// PolicyTemplateCount returns the total number of policy templates across all policy stores (for test use only).
func PolicyTemplateCount(b *InMemoryBackend) int {
	b.mu.RLock("PolicyTemplateCount")
	defer b.mu.RUnlock()

	return b.policyTemplates.Len()
}

// IdentitySourceCount returns the total number of identity sources across all policy stores (for test use only).
func IdentitySourceCount(b *InMemoryBackend) int {
	b.mu.RLock("IdentitySourceCount")
	defer b.mu.RUnlock()

	return b.identitySources.Len()
}

// SchemaCount returns the number of schemas stored in b (for test use only).
func SchemaCount(b *InMemoryBackend) int {
	b.mu.RLock("SchemaCount")
	defer b.mu.RUnlock()

	return b.schemas.Len()
}

// ARNIndexSize returns the number of entries in the ARN index (for test use only).
func ARNIndexSize(b *InMemoryBackend) int {
	b.mu.RLock("ARNIndexSize")
	defer b.mu.RUnlock()

	return len(b.arnIndex)
}

// HandlerOpsLen returns the number of supported operations for h (for test use only).
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

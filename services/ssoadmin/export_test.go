package ssoadmin

// InstanceCount returns the number of instances in the backend.
func InstanceCount(b *InMemoryBackend) int {
	b.mu.RLock("InstanceCount")
	defer b.mu.RUnlock()

	return len(b.instances)
}

// PermissionSetCount returns the number of permission sets in the backend.
func PermissionSetCount(b *InMemoryBackend) int {
	b.mu.RLock("PermissionSetCount")
	defer b.mu.RUnlock()

	return len(b.permissionSets)
}

// ApplicationCount returns the number of applications in the backend.
func ApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return len(b.applications)
}

// TrustedTokenIssuerCount returns the number of trusted token issuers in the backend.
func TrustedTokenIssuerCount(b *InMemoryBackend) int {
	b.mu.RLock("TrustedTokenIssuerCount")
	defer b.mu.RUnlock()

	return len(b.trustedTokenIssuers)
}

// HandlerOpsLen returns the number of operations in GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

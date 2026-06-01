package workmail

// OrgCount returns the number of stored organizations.
func OrgCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.organizations)
}

// UserCount returns the number of users in an organization.
func UserCount(b *InMemoryBackend, orgID string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.users[orgID])
}

// GroupCount returns the number of groups in an organization.
func GroupCount(b *InMemoryBackend, orgID string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.groups[orgID])
}

// ResourceCount returns the number of resources in an organization.
func ResourceCount(b *InMemoryBackend, orgID string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.resources[orgID])
}

// HandlerOpsLen returns the count of supported operations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

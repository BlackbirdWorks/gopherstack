package sesv2

// IdentityCount returns the number of stored email identities.
func IdentityCount(b *InMemoryBackend) int {
	b.mu.RLock("IdentityCount")
	defer b.mu.RUnlock()

	return len(b.identities)
}

// ConfigSetCount returns the number of stored configuration sets.
func ConfigSetCount(b *InMemoryBackend) int {
	b.mu.RLock("ConfigSetCount")
	defer b.mu.RUnlock()

	return len(b.configurationSets)
}

// ContactListCount returns the number of stored contact lists.
func ContactListCount(b *InMemoryBackend) int {
	b.mu.RLock("ContactListCount")
	defer b.mu.RUnlock()

	return len(b.contactLists)
}

// ContactCount returns the number of contacts in a given contact list.
func ContactCount(b *InMemoryBackend, listName string) int {
	b.mu.RLock("ContactCount")
	defer b.mu.RUnlock()

	return len(b.contacts[listName])
}

// EmailTemplateCount returns the number of stored email templates.
func EmailTemplateCount(b *InMemoryBackend) int {
	b.mu.RLock("EmailTemplateCount")
	defer b.mu.RUnlock()

	return len(b.emailTemplates)
}

// DedicatedIPPoolCount returns the number of stored dedicated IP pools.
func DedicatedIPPoolCount(b *InMemoryBackend) int {
	b.mu.RLock("DedicatedIPPoolCount")
	defer b.mu.RUnlock()

	return len(b.dedicatedIPPools)
}

// ExportJobCount returns the number of stored export jobs.
func ExportJobCount(b *InMemoryBackend) int {
	b.mu.RLock("ExportJobCount")
	defer b.mu.RUnlock()

	return len(b.exportJobs)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

package sesv2

// IdentityCount returns the number of stored email identities.
func IdentityCount(b *InMemoryBackend) int {
	b.mu.RLock("IdentityCount")
	defer b.mu.RUnlock()

	return b.identities.Len()
}

// ConfigSetCount returns the number of stored configuration sets.
func ConfigSetCount(b *InMemoryBackend) int {
	b.mu.RLock("ConfigSetCount")
	defer b.mu.RUnlock()

	return b.configurationSets.Len()
}

// ContactListCount returns the number of stored contact lists.
func ContactListCount(b *InMemoryBackend) int {
	b.mu.RLock("ContactListCount")
	defer b.mu.RUnlock()

	return b.contactLists.Len()
}

// ContactCount returns the number of contacts in a given contact list.
func ContactCount(b *InMemoryBackend, listName string) int {
	b.mu.RLock("ContactCount")
	defer b.mu.RUnlock()

	return len(b.contactsByList.Get(listName))
}

// EmailTemplateCount returns the number of stored email templates.
func EmailTemplateCount(b *InMemoryBackend) int {
	b.mu.RLock("EmailTemplateCount")
	defer b.mu.RUnlock()

	return b.emailTemplates.Len()
}

// DedicatedIPPoolCount returns the number of stored dedicated IP pools.
func DedicatedIPPoolCount(b *InMemoryBackend) int {
	b.mu.RLock("DedicatedIPPoolCount")
	defer b.mu.RUnlock()

	return b.dedicatedIPPools.Len()
}

// ExportJobCount returns the number of stored export jobs.
func ExportJobCount(b *InMemoryBackend) int {
	b.mu.RLock("ExportJobCount")
	defer b.mu.RUnlock()

	return b.exportJobs.Len()
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// EmailCount returns the number of retained sent emails.
func EmailCount(b *InMemoryBackend) int {
	b.mu.RLock("EmailCount")
	defer b.mu.RUnlock()

	return len(b.emails)
}

// MaxRetainedEmails exposes the email retention cap for tests.
const MaxRetainedEmails = maxRetainedEmails

// EmailCompactionHighWater exposes the compaction trigger for tests.
const EmailCompactionHighWater = emailCompactionHighWater

// ParseSESv2Path exposes parseSESv2Path -- the method+path parser backing
// RouteMatcher/ExtractOperation/Handler -- for external route-matrix tests.
func ParseSESv2Path(method, path string) (string, string) {
	return parseSESv2Path(method, path)
}

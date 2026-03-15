package ses

// MaxRetainedEmails exposes the email retention cap for testing.
const MaxRetainedEmails = maxRetainedEmails

// EmailCount returns the number of stored emails.
func (b *InMemoryBackend) EmailCount() int {
	b.mu.RLock("EmailCount")
	defer b.mu.RUnlock()

	return len(b.emails)
}

// IdentityCount returns the number of verified identities.
func (b *InMemoryBackend) IdentityCount() int {
	b.mu.RLock("IdentityCount")
	defer b.mu.RUnlock()

	return len(b.identities)
}

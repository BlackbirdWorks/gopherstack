package cognitoidentity

// PoolCount returns the number of identity pools in the backend.
// Used only in tests.
func (b *InMemoryBackend) PoolCount() int {
	b.mu.RLock("PoolCount")
	defer b.mu.RUnlock()

	return len(b.pools)
}

// IdentityCount returns the total number of identities in the backend.
// Used only in tests.
func (b *InMemoryBackend) IdentityCount() int {
	b.mu.RLock("IdentityCount")
	defer b.mu.RUnlock()

	return len(b.identities)
}

// PrincipalTagCount returns the number of principal-tag mappings in the backend.
// Used only in tests.
func (b *InMemoryBackend) PrincipalTagCount() int {
	b.mu.RLock("PrincipalTagCount")
	defer b.mu.RUnlock()

	return len(b.principalTags)
}

// ExportedRandomAlphanumeric exposes randomAlphanumeric for test coverage.
func ExportedRandomAlphanumeric(n int) (string, error) {
	return randomAlphanumeric(n)
}

// SetIdentityEnabled directly sets the Enabled flag on an identity for testing.
func (b *InMemoryBackend) SetIdentityEnabled(identityID string, enabled bool) {
	b.mu.Lock("SetIdentityEnabled")
	defer b.mu.Unlock()

	if id, ok := b.identities[identityID]; ok {
		id.Enabled = enabled
	}
}

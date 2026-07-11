package cognitoidentity

// PoolCount returns the total number of identity pools across all regions.
// Used only in tests.
func (b *InMemoryBackend) PoolCount() int {
	b.mu.RLock("PoolCount")
	defer b.mu.RUnlock()

	return b.pools.Len()
}

// IdentityCount returns the total number of identities across all regions.
// Used only in tests.
func (b *InMemoryBackend) IdentityCount() int {
	b.mu.RLock("IdentityCount")
	defer b.mu.RUnlock()

	return b.identities.Len()
}

// PrincipalTagCount returns the total number of principal-tag mappings across all regions.
// Used only in tests.
func (b *InMemoryBackend) PrincipalTagCount() int {
	b.mu.RLock("PrincipalTagCount")
	defer b.mu.RUnlock()

	return b.principalTags.Len()
}

// ExportedRandomAlphanumeric exposes randomAlphanumeric for test coverage.
func ExportedRandomAlphanumeric(n int) (string, error) {
	return randomAlphanumeric(n)
}

// SetIdentityEnabled directly sets the Enabled flag on an identity for testing.
// Searches across all regions.
func (b *InMemoryBackend) SetIdentityEnabled(identityID string, enabled bool) {
	b.mu.Lock("SetIdentityEnabled")
	defer b.mu.Unlock()

	b.identities.Range(func(identity *Identity) bool {
		if identity.IdentityID != identityID {
			return true
		}

		identity.Enabled = enabled

		return false
	})
}

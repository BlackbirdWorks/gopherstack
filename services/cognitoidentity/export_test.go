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

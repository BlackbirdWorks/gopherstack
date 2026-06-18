package cognitoidentity

// PoolCount returns the total number of identity pools across all regions.
// Used only in tests.
func (b *InMemoryBackend) PoolCount() int {
	b.mu.RLock("PoolCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionPools := range b.pools {
		total += len(regionPools)
	}

	return total
}

// IdentityCount returns the total number of identities across all regions.
// Used only in tests.
func (b *InMemoryBackend) IdentityCount() int {
	b.mu.RLock("IdentityCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionIdentities := range b.identities {
		total += len(regionIdentities)
	}

	return total
}

// PrincipalTagCount returns the total number of principal-tag mappings across all regions.
// Used only in tests.
func (b *InMemoryBackend) PrincipalTagCount() int {
	b.mu.RLock("PrincipalTagCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionTags := range b.principalTags {
		total += len(regionTags)
	}

	return total
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

	for _, regionIdentities := range b.identities {
		if id, ok := regionIdentities[identityID]; ok {
			id.Enabled = enabled

			return
		}
	}
}

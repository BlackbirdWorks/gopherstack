package vpclattice

// ------- Auth Policy operations -------

// PutAuthPolicy sets an auth policy on a resource.
func (b *InMemoryBackend) PutAuthPolicy(resourceID, policy string) (*AuthPolicy, error) {
	b.mu.Lock("PutAuthPolicy")
	defer b.mu.Unlock()

	b.authPolicies[resourceID] = policy

	return &AuthPolicy{Policy: policy, State: authPolicyStateActive}, nil
}

// GetAuthPolicy returns the auth policy for a resource.
func (b *InMemoryBackend) GetAuthPolicy(resourceID string) (*AuthPolicy, error) {
	b.mu.RLock("GetAuthPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.authPolicies[resourceID]
	if !ok {
		return nil, ErrNotFound
	}

	return &AuthPolicy{Policy: policy, State: authPolicyStateActive}, nil
}

// DeleteAuthPolicy deletes the auth policy for a resource.
func (b *InMemoryBackend) DeleteAuthPolicy(resourceID string) error {
	b.mu.Lock("DeleteAuthPolicy")
	defer b.mu.Unlock()

	delete(b.authPolicies, resourceID)

	return nil
}

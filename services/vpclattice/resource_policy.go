package vpclattice

// ------- Resource Policy operations -------

// PutResourcePolicy sets a resource policy.
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policy string) error {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	b.resourcePolicies[resourceArn] = policy

	return nil
}

// GetResourcePolicy returns a resource policy.
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) (string, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	policy, ok := b.resourcePolicies[resourceArn]
	if !ok {
		return "", ErrNotFound
	}

	return policy, nil
}

// DeleteResourcePolicy deletes a resource policy.
func (b *InMemoryBackend) DeleteResourcePolicy(resourceArn string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if _, ok := b.resourcePolicies[resourceArn]; !ok {
		return ErrNotFound
	}

	delete(b.resourcePolicies, resourceArn)

	return nil
}

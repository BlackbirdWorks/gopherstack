package vpclattice

// ------- Resource Policy operations -------

// PutResourcePolicy sets a resource policy. resourceArn accepts an ID or ARN
// (see resolvePolicyResourceARN in auth_policy.go for why it's normalized).
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policy string) error {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	b.resourcePolicies[b.resolvePolicyResourceARN(resourceArn)] = policy

	return nil
}

// GetResourcePolicy returns a resource policy.
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) (string, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	policy, ok := b.resourcePolicies[b.resolvePolicyResourceARN(resourceArn)]
	if !ok {
		return "", ErrNotFound
	}

	return policy, nil
}

// DeleteResourcePolicy deletes a resource policy.
func (b *InMemoryBackend) DeleteResourcePolicy(resourceArn string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	key := b.resolvePolicyResourceARN(resourceArn)

	if _, ok := b.resourcePolicies[key]; !ok {
		return ErrNotFound
	}

	delete(b.resourcePolicies, key)

	return nil
}

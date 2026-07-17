package codebuild

// PutResourcePolicy stores a resource policy for the given ARN.
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policy string) error {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	b.resourcePolicies[resourceArn] = policy

	return nil
}

// GetResourcePolicy returns the resource policy for the given ARN, or ErrNotFound if none set.
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) (string, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	if p, ok := b.resourcePolicies[resourceArn]; ok {
		return p, nil
	}

	return "", ErrNotFound
}

// DeleteResourcePolicy removes the resource policy for the given ARN (idempotent).
func (b *InMemoryBackend) DeleteResourcePolicy(resourceArn string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	delete(b.resourcePolicies, resourceArn)

	return nil
}

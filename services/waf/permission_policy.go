package waf

// PutPermissionPolicy stores a permission policy for a resource ARN.
func (b *InMemoryBackend) PutPermissionPolicy(resourceArn, policy string) error {
	b.mu.Lock("PutPermissionPolicy")
	defer b.mu.Unlock()

	b.permissionPolicies[resourceArn] = policy

	return nil
}

// GetPermissionPolicy retrieves the permission policy for a resource ARN.
func (b *InMemoryBackend) GetPermissionPolicy(resourceArn string) (string, error) {
	b.mu.RLock("GetPermissionPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.permissionPolicies[resourceArn]
	if !ok {
		return "", ErrNotFound
	}

	return policy, nil
}

// DeletePermissionPolicy removes the permission policy for a resource ARN.
func (b *InMemoryBackend) DeletePermissionPolicy(resourceArn string) error {
	b.mu.Lock("DeletePermissionPolicy")
	defer b.mu.Unlock()

	if _, ok := b.permissionPolicies[resourceArn]; !ok {
		return ErrNotFound
	}

	delete(b.permissionPolicies, resourceArn)

	return nil
}

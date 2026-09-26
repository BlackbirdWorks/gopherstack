package dsql

// GetClusterPolicy returns a cluster's resource-based policy.
func (b *InMemoryBackend) GetClusterPolicy(identifier string) (*ClusterPolicy, error) {
	b.mu.Lock("GetClusterPolicy")
	defer b.mu.Unlock()

	c, err := b.resolveClusterLocked(identifier)
	if err != nil {
		return nil, err
	}

	if c.Policy == nil {
		return nil, ErrPolicyNotFound
	}

	p := *c.Policy

	return &p, nil
}

// PutClusterPolicy creates or replaces a cluster's resource-based policy. If
// expectedVersion is non-empty it must match the current policy version
// (optimistic concurrency), matching PutClusterPolicyInput's
// expectedPolicyVersion semantics.
func (b *InMemoryBackend) PutClusterPolicy(identifier, policy, expectedVersion string) (*ClusterPolicy, error) {
	if policy == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("PutClusterPolicy")
	defer b.mu.Unlock()

	c, err := b.resolveClusterLocked(identifier)
	if err != nil {
		return nil, err
	}

	if expectedVersion != "" && (c.Policy == nil || c.Policy.Version != expectedVersion) {
		return nil, ErrPolicyVersionMismatch
	}

	c.Policy = &ClusterPolicy{Policy: policy, Version: newVersionToken()}

	p := *c.Policy

	return &p, nil
}

// DeleteClusterPolicy removes a cluster's resource-based policy. If
// expectedVersion is non-empty it must match the current policy version.
func (b *InMemoryBackend) DeleteClusterPolicy(identifier, expectedVersion string) (*ClusterPolicy, error) {
	b.mu.Lock("DeleteClusterPolicy")
	defer b.mu.Unlock()

	c, err := b.resolveClusterLocked(identifier)
	if err != nil {
		return nil, err
	}

	if c.Policy == nil {
		return nil, ErrPolicyNotFound
	}

	if expectedVersion != "" && c.Policy.Version != expectedVersion {
		return nil, ErrPolicyVersionMismatch
	}

	p := *c.Policy
	c.Policy = nil

	return &p, nil
}

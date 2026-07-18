package organizations

// DeleteResourcePolicy removes the organization resource policy.
func (b *InMemoryBackend) DeleteResourcePolicy() error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	if b.resourcePolicy == nil {
		return ErrResourcePolicyNotFound
	}

	b.resourcePolicy = nil

	return nil
}

// DescribeResourcePolicy returns the organization resource policy.
func (b *InMemoryBackend) DescribeResourcePolicy() (*ResourcePolicy, error) {
	b.mu.RLock("DescribeResourcePolicy")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if b.resourcePolicy == nil {
		return nil, ErrResourcePolicyNotFound
	}

	cp := *b.resourcePolicy

	return &cp, nil
}

// PutResourcePolicy creates or replaces the organization resource policy.
func (b *InMemoryBackend) PutResourcePolicy(content string) (*ResourcePolicy, error) {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	rpID := "p-rp-default"

	rp := &ResourcePolicy{
		ID:      rpID,
		ARN:     b.resourcePolicyARN(b.org.ID),
		Content: content,
	}

	b.resourcePolicy = rp

	cp := *rp

	return &cp, nil
}

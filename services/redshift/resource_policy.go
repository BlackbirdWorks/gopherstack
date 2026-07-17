package redshift

import "fmt"

// GetResourcePolicy returns the resource policy for the given resource ARN.
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) (*ResourcePolicy, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	rp, exists := b.resourcePolicies.Get(resourceArn)
	if !exists {
		return nil, fmt.Errorf("%w: resource policy for %s not found", ErrResourcePolicyNotFound, resourceArn)
	}

	cp := *rp

	return &cp, nil
}

// PutResourcePolicy creates or replaces a resource policy for the given ARN.
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policy string) (*ResourcePolicy, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	rp := &ResourcePolicy{
		ResourceArn: resourceArn,
		Policy:      policy,
	}
	b.resourcePolicies.Put(rp)

	cp := *rp

	return &cp, nil
}

// DeleteResourcePolicy deletes the resource policy for the given ARN.
func (b *InMemoryBackend) DeleteResourcePolicy(resourceArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if _, exists := b.resourcePolicies.Get(resourceArn); !exists {
		return fmt.Errorf("%w: resource policy for %s not found", ErrResourcePolicyNotFound, resourceArn)
	}

	b.resourcePolicies.Delete(resourceArn)

	return nil
}

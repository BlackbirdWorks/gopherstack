package redshift

import "fmt"

// ---------------------------------------------------------------------------
// Serverless resource policies (Get/Put/DeleteResourcePolicy)
// ---------------------------------------------------------------------------

// GetResourcePolicySL returns the resource policy attached to resourceArn.
func (b *InMemoryBackend) GetResourcePolicySL(resourceArn string) (*ServerlessResourcePolicy, error) {
	b.mu.RLock("GetResourcePolicySL")
	defer b.mu.RUnlock()

	rp, ok := b.slResourcePolicies.Get(resourceArn)
	if !ok {
		return nil, fmt.Errorf("%w: no resource policy for %q", ErrResourcePolicySLNotFound, resourceArn)
	}

	cp := *rp

	return &cp, nil
}

// PutResourcePolicySL creates or replaces the resource policy for resourceArn.
func (b *InMemoryBackend) PutResourcePolicySL(resourceArn, policy string) (*ServerlessResourcePolicy, error) {
	b.mu.Lock("PutResourcePolicySL")
	defer b.mu.Unlock()

	rp := &ServerlessResourcePolicy{ResourceArn: resourceArn, Policy: policy}
	b.slResourcePolicies.Put(rp)

	cp := *rp

	return &cp, nil
}

// DeleteResourcePolicySL deletes the resource policy for resourceArn.
func (b *InMemoryBackend) DeleteResourcePolicySL(resourceArn string) (*ServerlessResourcePolicy, error) {
	b.mu.Lock("DeleteResourcePolicySL")
	defer b.mu.Unlock()

	rp, ok := b.slResourcePolicies.Get(resourceArn)
	if !ok {
		return nil, fmt.Errorf("%w: no resource policy for %q", ErrResourcePolicySLNotFound, resourceArn)
	}

	cp := *rp
	b.slResourcePolicies.Delete(resourceArn)

	return &cp, nil
}

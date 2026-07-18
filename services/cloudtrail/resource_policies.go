package cloudtrail

import "fmt"

// DeleteResourcePolicy removes the resource-based policy from a CloudTrail resource.
func (b *InMemoryBackend) DeleteResourcePolicy(resourceARN string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if !b.resourcePolicies.Delete(resourceARN) {
		return fmt.Errorf("%w: resource policy for %s not found", ErrNotFound, resourceARN)
	}

	return nil
}

// GetResourcePolicy returns the resource policy for the given ARN.
func (b *InMemoryBackend) GetResourcePolicy(resourceARN string) (*ResourcePolicy, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	rp, ok := b.resourcePolicies.Get(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource policy for %s not found", ErrNotFound, resourceARN)
	}
	cp := *rp

	return &cp, nil
}

// PutResourcePolicy sets the resource policy for the given ARN.
func (b *InMemoryBackend) PutResourcePolicy(resourceARN, policy string) *ResourcePolicy {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	rp := &ResourcePolicy{ResourceARN: resourceARN, ResourcePolicy: policy}
	b.resourcePolicies.Put(rp)
	cp := *rp

	return &cp
}

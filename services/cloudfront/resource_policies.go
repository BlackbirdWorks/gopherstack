package cloudfront

import (
	"fmt"
)

func (b *InMemoryBackend) PutResourcePolicy(resourceARN, policy string) error {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	b.resourcePolicies[resourceARN] = &resourcePolicyEntry{Policy: policy}

	return nil
}

func (b *InMemoryBackend) GetResourcePolicy(resourceARN string) (string, error) {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	e, ok := b.resourcePolicies[resourceARN]
	if !ok {
		return "", fmt.Errorf("%w: no resource policy for %s", ErrResourcePolicyNotFound, resourceARN)
	}

	return e.Policy, nil
}

func (b *InMemoryBackend) DeleteResourcePolicy(resourceARN string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	delete(b.resourcePolicies, resourceARN)

	return nil
}

// ---------------------------------------------------------------------------
// ConnectionGroup extra operations (Get/List/Update/Delete)
// ---------------------------------------------------------------------------

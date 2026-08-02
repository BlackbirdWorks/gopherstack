package networkmanager

import "encoding/json"

// This file implements PARITY.md family W: a resource-based IAM-style JSON
// policy document attached to a NetworkManager resource ARN (used for
// cross-account sharing) -- structurally unrelated to the versioned
// CoreNetworkPolicy (network-configuration) document in corenetworks.go
// despite the shared English word "policy" (see PARITY.md's naming-
// collision note).

// PutResourcePolicy stores policyDocument for resourceArn, creating the
// entry if absent (real AWS's PutResourcePolicy has no ResourceNotFound
// case, per PARITY.md, consistent with create-if-absent semantics).
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policyDocument string) error {
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	if !json.Valid([]byte(policyDocument)) {
		return validationError("PolicyDocument is not valid JSON")
	}

	b.resourcePolicies.Put(&resourcePolicy{ResourceArn: resourceArn, PolicyDocument: policyDocument})

	return nil
}

// GetResourcePolicy returns the stored policy document, or an empty string
// if resourceArn has none -- real AWS's GetResourcePolicy carries NO
// ResourceNotFoundException in its error set (PARITY.md family W: "[base4]
// only"), so an absent policy is reported as empty content, not an error.
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) string {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	p, ok := b.resourcePolicies.Get(resourceArn)
	if !ok {
		return ""
	}

	return p.PolicyDocument
}

func (b *InMemoryBackend) DeleteResourcePolicy(resourceArn string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	b.resourcePolicies.Delete(resourceArn)

	return nil
}

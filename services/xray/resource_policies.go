package xray

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

func cloneResourcePolicy(p *ResourcePolicy) *ResourcePolicy {
	cp := *p

	return &cp
}

// PutResourcePolicy creates or updates a resource policy with the given name and document.
// Returns ErrTooManyPolicies if the account already has maxResourcePolicies.
// Returns ErrInvalidPolicyRevisionID if revisionID doesn't match the stored one.
// Returns ErrMalformedPolicyDocument if policyDocument is not valid JSON.
func (b *InMemoryBackend) PutResourcePolicy(policyName, policyDocument, revisionID string) (*ResourcePolicy, error) {
	// Validate JSON.
	var js json.RawMessage
	if err := json.Unmarshal([]byte(policyDocument), &js); err != nil {
		return nil, fmt.Errorf("%w: policy document is not valid JSON: %w", ErrMalformedPolicyDocument, err)
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	existing, exists := b.resourcePolicies.Get(policyName)

	if !exists && b.resourcePolicies.Len() >= maxResourcePolicies {
		return nil, fmt.Errorf(
			"%w: maximum of %d resource policies per account",
			ErrTooManyPolicies,
			maxResourcePolicies,
		)
	}

	// Revision ID check: if a revision is provided it must match the stored one.
	if revisionID != "" && exists && existing.PolicyRevisionID != revisionID {
		return nil, fmt.Errorf("%w: policy revision ID does not match", ErrInvalidPolicyRevisionID)
	}

	p := &ResourcePolicy{
		PolicyName:       policyName,
		PolicyDocument:   policyDocument,
		PolicyRevisionID: uuid.NewString(),
	}
	b.resourcePolicies.Put(p)

	return cloneResourcePolicy(p), nil
}

// ListResourcePolicies returns all resource policies sorted by name.
func (b *InMemoryBackend) ListResourcePolicies() []ResourcePolicy {
	b.mu.RLock("ListResourcePolicies")
	defer b.mu.RUnlock()

	all := b.resourcePolicies.All()
	out := make([]ResourcePolicy, 0, len(all))

	for _, p := range all {
		out = append(out, *cloneResourcePolicy(p))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].PolicyName < out[j].PolicyName
	})

	return out
}

// DeleteResourcePolicy removes the resource policy with the given name.
func (b *InMemoryBackend) DeleteResourcePolicy(policyName string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if !b.resourcePolicies.Delete(policyName) {
		return fmt.Errorf("%w: resource policy %s not found", ErrResourcePolicyNotFound, policyName)
	}

	return nil
}

// AddResourcePolicyInternal seeds a resource policy directly for testing.
func (b *InMemoryBackend) AddResourcePolicyInternal(policy ResourcePolicy) {
	b.mu.Lock("AddResourcePolicyInternal")
	defer b.mu.Unlock()

	b.resourcePolicies.Put(cloneResourcePolicy(&policy))
}

const (
	// maxResourcePolicies is the maximum number of resource policies per account.
	maxResourcePolicies = 5
)

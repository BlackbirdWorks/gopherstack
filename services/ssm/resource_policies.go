package ssm

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) resourcePoliciesStore(region string) map[string][]*ResourcePolicy {
	return b.resourcePolicies[region]
}

// GetResourcePolicies returns policies attached to a resource.
func (b *InMemoryBackend) GetResourcePolicies(
	ctx context.Context,
	input *GetResourcePoliciesInput,
) (*GetResourcePoliciesOutputFull, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetResourcePolicies")
	defer b.mu.RUnlock()

	policies := b.resourcePoliciesStore(region)[input.ResourceARN]
	all := make([]ResourcePolicy, 0, len(policies))

	for _, p := range policies {
		all = append(all, *p)
	}

	page, next := paginateSlice(all, input.NextToken, int(input.MaxResults), defaultDescribeMaxResults)

	return &GetResourcePoliciesOutputFull{Policies: page, NextToken: next}, nil
}

// findResourcePolicyLocked looks up an existing policy by ID on resourceARN.
// Caller must hold b.mu.
func (b *InMemoryBackend) findResourcePolicyLocked(region, resourceARN, policyID string) *ResourcePolicy {
	for _, p := range b.resourcePoliciesStore(region)[resourceARN] {
		if p.PolicyID == policyID {
			return p
		}
	}

	return nil
}

// PutResourcePolicy creates or updates a policy attached to a resource.
// Real AWS: omitting PolicyId creates a new policy; supplying PolicyId (and
// PolicyHash, for optimistic concurrency) updates the existing one in place
// (api_op_PutResourcePolicy.go doc comment: "Creates or updates ... To
// update a policy, you must specify PolicyId and PolicyHash").
func (b *InMemoryBackend) PutResourcePolicy(
	ctx context.Context,
	input *PutResourcePolicyInput,
) (*PutResourcePolicyOutputFull, error) {
	if input.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidationException)
	}

	if input.Policy == "" {
		return nil, fmt.Errorf("%w: Policy is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	if b.resourcePolicies[region] == nil {
		b.resourcePolicies[region] = make(map[string][]*ResourcePolicy)
	}

	if input.PolicyID != "" {
		existing := b.findResourcePolicyLocked(region, input.ResourceARN, input.PolicyID)
		if existing == nil {
			return nil, ErrResourcePolicyNotFound
		}

		if input.PolicyHash != "" && input.PolicyHash != existing.PolicyHash {
			return nil, ErrResourcePolicyConflict
		}

		existing.Policy = input.Policy
		existing.PolicyHash = uuid.NewString()

		return &PutResourcePolicyOutputFull{PolicyID: existing.PolicyID, PolicyHash: existing.PolicyHash}, nil
	}

	policyID := policyIDPrefix + uuid.NewString()
	policy := &ResourcePolicy{
		PolicyID:   policyID,
		PolicyHash: uuid.NewString(),
		Policy:     input.Policy,
	}
	policies := b.resourcePoliciesStore(region)
	policies[input.ResourceARN] = append(policies[input.ResourceARN], policy)

	return &PutResourcePolicyOutputFull{PolicyID: policyID, PolicyHash: policy.PolicyHash}, nil
}

// DeleteResourcePolicy removes a policy from a resource. ResourceArn,
// PolicyId and PolicyHash are all required on the real
// DeleteResourcePolicyInput (api_op_DeleteResourcePolicy.go) -- PolicyHash
// exists specifically "to prevent multiple calls from attempting to
// overwrite a policy", so a mismatch is a real ResourcePolicyConflictException,
// not a silent success.
func (b *InMemoryBackend) DeleteResourcePolicy(
	ctx context.Context,
	input *DeleteResourcePolicyInput,
) (*DeleteResourcePolicyOutput, error) {
	if input.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrValidationException)
	}

	if input.PolicyID == "" {
		return nil, fmt.Errorf("%w: PolicyId is required", ErrValidationException)
	}

	if input.PolicyHash == "" {
		return nil, fmt.Errorf("%w: PolicyHash is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if b.resourcePolicies[region] == nil {
		b.resourcePolicies[region] = make(map[string][]*ResourcePolicy)
	}

	existing := b.findResourcePolicyLocked(region, input.ResourceARN, input.PolicyID)
	if existing == nil {
		return nil, ErrResourcePolicyNotFound
	}

	if input.PolicyHash != existing.PolicyHash {
		return nil, ErrResourcePolicyConflict
	}

	policies := b.resourcePoliciesStore(region)
	updated := policies[input.ResourceARN][:0]

	for _, p := range policies[input.ResourceARN] {
		if p.PolicyID != input.PolicyID {
			updated = append(updated, p)
		}
	}

	if len(updated) == 0 {
		delete(policies, input.ResourceARN)
	} else {
		policies[input.ResourceARN] = updated
	}

	cleanupEmptyInnerMap(b.resourcePolicies, region)

	return &DeleteResourcePolicyOutput{}, nil
}

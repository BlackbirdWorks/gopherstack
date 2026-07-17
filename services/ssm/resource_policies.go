package ssm

import (
	"context"

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
	list := make([]ResourcePolicy, 0, len(policies))

	for _, p := range policies {
		list = append(list, *p)
	}

	return &GetResourcePoliciesOutputFull{Policies: list}, nil
}

// PutResourcePolicy attaches a policy to a resource.
func (b *InMemoryBackend) PutResourcePolicy(
	ctx context.Context,
	input *PutResourcePolicyInput,
) (*PutResourcePolicyOutputFull, error) {
	region := getRegion(ctx)
	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	policyID := policyIDPrefix + uuid.NewString()
	policy := &ResourcePolicy{
		PolicyID:   policyID,
		PolicyHash: uuid.NewString(),
		Policy:     input.Policy,
	}
	if b.resourcePolicies[region] == nil {
		b.resourcePolicies[region] = make(map[string][]*ResourcePolicy)
	}
	policies := b.resourcePoliciesStore(region)
	policies[input.ResourceARN] = append(policies[input.ResourceARN], policy)

	return &PutResourcePolicyOutputFull{PolicyID: policyID, PolicyHash: policy.PolicyHash}, nil
}

// DeleteResourcePolicy removes a policy from a resource.
func (b *InMemoryBackend) DeleteResourcePolicy(
	ctx context.Context,
	input *DeleteResourcePolicyInput,
) (*DeleteResourcePolicyOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if input.ResourceARN == "" {
		return &DeleteResourcePolicyOutput{}, nil
	}

	if b.resourcePolicies[region] == nil {
		b.resourcePolicies[region] = make(map[string][]*ResourcePolicy)
	}
	policies := b.resourcePoliciesStore(region)
	existing := policies[input.ResourceARN]
	updated := existing[:0]

	for _, p := range existing {
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

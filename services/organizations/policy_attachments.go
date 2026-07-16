package organizations

import (
	"cmp"
	"slices"
)

// maxPoliciesPerTarget is the AWS limit for the number of policies of the same type attached to a target.
const maxPoliciesPerTarget = 5

// AttachPolicy attaches a policy to a target.
func (b *InMemoryBackend) AttachPolicy(policyID, targetID string) error {
	b.mu.Lock("AttachPolicy")
	defer b.mu.Unlock()

	policy, ok := b.policies.Get(policyID)
	if !ok {
		return ErrPolicyNotFound
	}

	// Validate target exists.
	if !b.targetExistsLocked(targetID) {
		return ErrTargetNotFound
	}

	targets := b.policyTargets[policyID]
	if slices.Contains(targets, targetID) {
		return ErrDuplicatePolicyAttachment
	}

	// Enforce per-target, per-policy-type limit (AWS limit is 5).
	policyType := policy.PolicySummary.Type
	typeCount := 0

	for _, attachedPolicyID := range b.targetPolicies[targetID] {
		if p, exists := b.policies.Get(attachedPolicyID); exists && p.PolicySummary.Type == policyType {
			typeCount++
		}
	}

	if typeCount >= maxPoliciesPerTarget {
		return ErrPolicyLimitExceeded
	}

	b.policyTargets[policyID] = append(targets, targetID)
	b.targetPolicies[targetID] = append(b.targetPolicies[targetID], policyID)

	return nil
}

// DetachPolicy detaches a policy from a target.
func (b *InMemoryBackend) DetachPolicy(policyID, targetID string) error {
	b.mu.Lock("DetachPolicy")
	defer b.mu.Unlock()

	if !b.policies.Has(policyID) {
		return ErrPolicyNotFound
	}

	targets := b.policyTargets[policyID]

	if !slices.Contains(targets, targetID) {
		return ErrPolicyNotAttached
	}

	b.policyTargets[policyID] = removeString(targets, targetID)
	b.targetPolicies[targetID] = removeString(b.targetPolicies[targetID], policyID)

	return nil
}

// ListPoliciesForTarget returns policies attached to a target, filtered by type.
// AWS requires a non-empty Filter; empty filter returns InvalidInputException.
func (b *InMemoryBackend) ListPoliciesForTarget(targetID, filter string) ([]*Policy, error) {
	b.mu.RLock("ListPoliciesForTarget")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if filter == "" {
		return nil, ErrInvalidInput
	}

	policyIDs := b.targetPolicies[targetID]

	var out []*Policy

	for _, pid := range policyIDs {
		if p, ok := b.policies.Get(pid); ok {
			if p.PolicySummary.Type == filter {
				out = append(out, copyPolicy(p))
			}
		}
	}

	return out, nil
}

// ListTargetsForPolicy returns targets that a policy is attached to.
func (b *InMemoryBackend) ListTargetsForPolicy(policyID string) ([]PolicyTargetSummary, error) {
	b.mu.RLock("ListTargetsForPolicy")
	defer b.mu.RUnlock()

	if !b.policies.Has(policyID) {
		return nil, ErrPolicyNotFound
	}

	targetIDs := b.policyTargets[policyID]
	out := make([]PolicyTargetSummary, 0, len(targetIDs))

	for _, tid := range targetIDs {
		summary := b.resolveTargetSummary(tid)
		out = append(out, summary)
	}

	slices.SortFunc(
		out,
		func(a, b PolicyTargetSummary) int { return cmp.Compare(a.TargetID, b.TargetID) },
	)

	return out, nil
}

// resolveTargetSummary builds a PolicyTargetSummary for a given target ID.
func (b *InMemoryBackend) resolveTargetSummary(targetID string) PolicyTargetSummary {
	if b.root != nil && b.root.ID == targetID {
		return PolicyTargetSummary{
			TargetID: targetID,
			ARN:      b.root.ARN,
			Name:     b.root.Name,
			Type:     "ROOT",
		}
	}

	if ou, ok := b.ous.Get(targetID); ok {
		return PolicyTargetSummary{
			TargetID: targetID,
			ARN:      ou.ARN,
			Name:     ou.Name,
			Type:     targetTypeOU,
		}
	}

	if acct, ok := b.accounts.Get(targetID); ok {
		return PolicyTargetSummary{
			TargetID: targetID,
			ARN:      acct.ARN,
			Name:     acct.Name,
			Type:     targetTypeAccount,
		}
	}

	return PolicyTargetSummary{TargetID: targetID, Type: targetTypeAccount}
}

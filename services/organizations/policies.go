package organizations

import (
	"cmp"
	"slices"
)

const policyStatusEnabled = "ENABLED"

// validPolicyTypes returns the policy types supported by AWS Organizations.
func validPolicyTypes() []string {
	return []string{
		"SERVICE_CONTROL_POLICY",
		"RESOURCE_CONTROL_POLICY",
		"TAG_POLICY",
		"BACKUP_POLICY",
		"AISERVICES_OPT_OUT_POLICY",
		"CHATBOT_POLICY",
		"DECLARATIVE_POLICY_EC2",
		"SECURITYHUB_POLICY",
	}
}

// CreatePolicy creates a new policy.
func (b *InMemoryBackend) CreatePolicy(
	name, description, content, policyType string,
	tags []Tag,
) (*Policy, error) {
	b.mu.Lock("CreatePolicy")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !slices.Contains(validPolicyTypes(), policyType) {
		return nil, ErrInvalidInput
	}

	policyID := newPolicyID()
	p := &Policy{
		PolicySummary: PolicySummary{
			ID:          policyID,
			ARN:         b.policyARN(b.org.ID, policyType, policyID),
			Name:        name,
			Description: description,
			Type:        policyType,
			AwsManaged:  false,
		},
		Content: content,
	}

	b.policies.Put(p)
	b.policyTargets[policyID] = []string{}
	b.setTagsLocked(policyID, tags)

	return p, nil
}

// DescribePolicy returns a policy by ID.
func (b *InMemoryBackend) DescribePolicy(policyID string) (*Policy, error) {
	b.mu.RLock("DescribePolicy")
	defer b.mu.RUnlock()

	p, ok := b.policies.Get(policyID)
	if !ok {
		return nil, ErrPolicyNotFound
	}

	return copyPolicy(p), nil
}

// UpdatePolicy updates a policy.
func (b *InMemoryBackend) UpdatePolicy(
	policyID, name, description, content string,
) (*Policy, error) {
	b.mu.Lock("UpdatePolicy")
	defer b.mu.Unlock()

	p, ok := b.policies.Get(policyID)
	if !ok {
		return nil, ErrPolicyNotFound
	}

	if name != "" {
		p.PolicySummary.Name = name
	}

	if description != "" {
		p.PolicySummary.Description = description
	}

	if content != "" {
		p.Content = content
	}

	return copyPolicy(p), nil
}

// DeletePolicy removes a policy.
func (b *InMemoryBackend) DeletePolicy(policyID string) error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	if !b.policies.Has(policyID) {
		return ErrPolicyNotFound
	}

	// AWS rejects deletion of policies that are still attached to targets.
	if len(b.policyTargets[policyID]) > 0 {
		return ErrPolicyInUse
	}

	delete(b.policyTargets, policyID)
	b.policies.Delete(policyID)
	delete(b.tags, policyID)

	return nil
}

// ListPolicies returns all policies of a given type.
// AWS requires a non-empty Filter; empty filter returns InvalidInputException.
func (b *InMemoryBackend) ListPolicies(filter string) ([]*Policy, error) {
	b.mu.RLock("ListPolicies")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if filter == "" {
		return nil, ErrInvalidInput
	}

	var out []*Policy

	for _, p := range b.policies.All() {
		if p.PolicySummary.Type == filter {
			out = append(out, copyPolicy(p))
		}
	}

	slices.SortFunc(
		out,
		func(a, b *Policy) int { return cmp.Compare(a.PolicySummary.Name, b.PolicySummary.Name) },
	)

	return out, nil
}

// EnablePolicyType enables a policy type on the root.
func (b *InMemoryBackend) EnablePolicyType(rootID, policyType string) (*Root, error) {
	b.mu.Lock("EnablePolicyType")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if b.root == nil || b.root.ID != rootID {
		return nil, ErrInvalidInput
	}

	for _, pt := range b.root.PolicyTypes {
		if pt.Type == policyType && pt.Status == policyStatusEnabled {
			return nil, ErrPolicyTypeAlreadyEnabled
		}
	}

	b.root.PolicyTypes = append(b.root.PolicyTypes, PolicyTypeSummary{
		Type:   policyType,
		Status: policyStatusEnabled,
	})

	return copyRoot(b.root), nil
}

// DisablePolicyType disables a policy type on the root.
func (b *InMemoryBackend) DisablePolicyType(rootID, policyType string) (*Root, error) {
	b.mu.Lock("DisablePolicyType")
	defer b.mu.Unlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if b.root == nil || b.root.ID != rootID {
		return nil, ErrInvalidInput
	}

	newTypes := make([]PolicyTypeSummary, 0, len(b.root.PolicyTypes))

	found := false

	for _, pt := range b.root.PolicyTypes {
		if pt.Type == policyType {
			found = true

			continue
		}

		newTypes = append(newTypes, pt)
	}

	if !found {
		return nil, ErrPolicyTypeNotEnabled
	}

	// AWS rejects disabling a policy type when policies of that type are still attached to any target.
	for policyID, targets := range b.policyTargets {
		if len(targets) > 0 {
			if p, ok := b.policies.Get(policyID); ok && p.PolicySummary.Type == policyType {
				return nil, ErrPolicyTypeAttached
			}
		}
	}

	b.root.PolicyTypes = newTypes

	return copyRoot(b.root), nil
}

// AddPolicyInternal seeds a policy directly for testing.
func (b *InMemoryBackend) AddPolicyInternal(p *Policy) {
	b.mu.Lock("AddPolicyInternal")
	defer b.mu.Unlock()

	b.policies.Put(p)

	if b.policyTargets[p.PolicySummary.ID] == nil {
		b.policyTargets[p.PolicySummary.ID] = []string{}
	}
}

// copyPolicy returns a value copy of a Policy (all fields are scalars).
func copyPolicy(p *Policy) *Policy {
	cp := *p

	return &cp
}

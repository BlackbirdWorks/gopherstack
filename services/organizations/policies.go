package organizations

import (
	"cmp"
	"encoding/json"
	"slices"
)

const policyStatusEnabled = "ENABLED"

// Policy type names, shared between validPolicyTypes, policyContentMaxSize, and
// effective_policy.go's merge-style switch (avoids goconst violations from
// repeating these literals across the package).
const (
	policyTypeSCP      = "SERVICE_CONTROL_POLICY"
	policyTypeRCP      = "RESOURCE_CONTROL_POLICY"
	policyTypeTag      = "TAG_POLICY"
	policyTypeBackup   = "BACKUP_POLICY"
	policyTypeAIOptOut = "AISERVICES_OPT_OUT_POLICY"
	policyTypeChatbot  = "CHATBOT_POLICY"
	policyTypeDeclEC2  = "DECLARATIVE_POLICY_EC2"
	policyTypeSecHub   = "SECURITYHUB_POLICY"
)

// validPolicyTypes returns the policy types supported by AWS Organizations.
func validPolicyTypes() []string {
	return []string{
		policyTypeSCP,
		policyTypeRCP,
		policyTypeTag,
		policyTypeBackup,
		policyTypeAIOptOut,
		policyTypeChatbot,
		policyTypeDeclEC2,
		policyTypeSecHub,
	}
}

// Maximum policy document sizes (characters) per the Organizations quotas
// reference (docs.aws.amazon.com/organizations/latest/userguide/orgs_reference_limits.html).
// SCP/RCP default to 5,120 (extensible via a service-quota increase, not
// modeled here). CHATBOT_POLICY/SECURITYHUB_POLICY aren't documented in that
// reference table; they're given the same 10,000 character ceiling as the
// other newer policy types (BACKUP_POLICY, TAG_POLICY, DECLARATIVE_POLICY_EC2)
// as a best-effort default pending independent verification.
const (
	policyContentLimitSCP      = 5120
	policyContentLimitTag      = 10000
	policyContentLimitBackup   = 10000
	policyContentLimitAIOptOut = 2500
	policyContentLimitChatbot  = 10000
	policyContentLimitDeclEC2  = 10000
	policyContentLimitSecHub   = 10000
)

// policyContentMaxSize returns the maximum content size for policyType and
// whether policyType is a recognized type with a modeled limit.
func policyContentMaxSize(policyType string) (int, bool) {
	switch policyType {
	case policyTypeSCP, policyTypeRCP:
		return policyContentLimitSCP, true
	case policyTypeTag:
		return policyContentLimitTag, true
	case policyTypeBackup:
		return policyContentLimitBackup, true
	case policyTypeAIOptOut:
		return policyContentLimitAIOptOut, true
	case policyTypeChatbot:
		return policyContentLimitChatbot, true
	case policyTypeDeclEC2:
		return policyContentLimitDeclEC2, true
	case policyTypeSecHub:
		return policyContentLimitSecHub, true
	default:
		return 0, false
	}
}

// validatePolicyContent checks content against AWS's syntax and size rules for
// policyType. Real AWS rejects non-JSON policy documents with
// MalformedPolicyDocumentException and documents exceeding the per-type size
// quota with ConstraintViolationException(POLICY_CONTENT_LIMIT_EXCEEDED).
func validatePolicyContent(content, policyType string) error {
	if !json.Valid([]byte(content)) {
		return ErrMalformedPolicyDocument
	}

	if limit, ok := policyContentMaxSize(policyType); ok && len(content) > limit {
		return ErrPolicyContentLimitExceeded
	}

	return nil
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

	if err := validatePolicyContent(content, policyType); err != nil {
		return nil, err
	}

	if err := validateNewTags(nil, tags); err != nil {
		return nil, err
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

	if content != "" {
		if err := validatePolicyContent(content, p.PolicySummary.Type); err != nil {
			return nil, err
		}
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

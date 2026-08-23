package verifiedpermissions

import (
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// policyTemplateARN builds the ARN for a policy template.
func policyTemplateARN(accountID, policyStoreID, templateID string) string {
	resource := fmt.Sprintf("policy-template/%s/%s", policyStoreID, templateID)

	return arn.Build("verifiedpermissions", "", accountID, resource)
}

// clonePolicyTemplate returns a deep copy of a PolicyTemplate.
func clonePolicyTemplate(pt *PolicyTemplate) *PolicyTemplate {
	cp := *pt

	return &cp
}

func policyTemplateKey(policyStoreID, policyTemplateID string) string {
	return policyStoreID + "/" + policyTemplateID
}

// CreatePolicyTemplate creates a new policy template in the given policy
// store. A non-empty clientToken makes the call idempotent for eight hours,
// same semantics as CreatePolicyStore's ClientToken.
func (b *InMemoryBackend) CreatePolicyTemplate(
	policyStoreID, description, statement, name, clientToken string,
) (*PolicyTemplate, error) {
	b.mu.Lock("CreatePolicyTemplate")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	fingerprint := policyStoreID + "\x00" + description + "\x00" + statement + "\x00" + name

	existingID, err := b.checkClientToken("CreatePolicyTemplate", clientToken, fingerprint)
	if err != nil {
		return nil, err
	}

	if existingID != "" {
		if existing, ok := b.policyTemplates.Get(policyTemplateKey(policyStoreID, existingID)); ok {
			return clonePolicyTemplate(existing), nil
		}
	}

	id := uuid.NewString()
	now := time.Now()
	pt := &PolicyTemplate{
		PolicyTemplateID: id,
		PolicyStoreID:    policyStoreID,
		Description:      description,
		Statement:        statement,
		Name:             name,
		CreatedDate:      now,
		LastUpdated:      now,
	}
	b.policyTemplates.Put(pt)
	b.arnIndex[policyTemplateARN(b.accountID, policyStoreID, id)] = arnKindPolicyTemplate + ":" + policyStoreID + ":" + id
	b.recordClientToken("CreatePolicyTemplate", clientToken, fingerprint, id)

	return clonePolicyTemplate(pt), nil
}

// GetPolicyTemplate returns the policy template with the given ID.
func (b *InMemoryBackend) GetPolicyTemplate(policyStoreID, policyTemplateID string) (*PolicyTemplate, error) {
	b.mu.RLock("GetPolicyTemplate")
	defer b.mu.RUnlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	pt, ok := b.policyTemplates.Get(policyTemplateKey(policyStoreID, policyTemplateID))
	if !ok {
		return nil, fmt.Errorf("%w: policy template %s not found", ErrPolicyTemplateNotFound, policyTemplateID)
	}

	return clonePolicyTemplate(pt), nil
}

// ListPolicyTemplates returns all policy templates in a policy store sorted by creation date.
func (b *InMemoryBackend) ListPolicyTemplates(
	policyStoreID, nextToken string,
	maxResults int,
) ([]PolicyTemplate, string, error) {
	b.mu.RLock("ListPolicyTemplates")
	defer b.mu.RUnlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, "", fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	templates := b.policyTemplatesByStore.Get(policyStoreID)
	page, tok := listByPolicyStore(templates, nextToken, maxResults,
		func(pt *PolicyTemplate) PolicyTemplate { return *clonePolicyTemplate(pt) },
		func(pt PolicyTemplate) time.Time { return pt.CreatedDate },
		func(pt PolicyTemplate) string { return pt.PolicyTemplateID },
	)

	return page, tok, nil
}

// UpdatePolicyTemplate updates the description, statement, and name of a policy template.
func (b *InMemoryBackend) UpdatePolicyTemplate(
	policyStoreID, policyTemplateID, description, statement, name string,
) (*PolicyTemplate, error) {
	b.mu.Lock("UpdatePolicyTemplate")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	pt, ok := b.policyTemplates.Get(policyTemplateKey(policyStoreID, policyTemplateID))
	if !ok {
		return nil, fmt.Errorf("%w: policy template %s not found", ErrPolicyTemplateNotFound, policyTemplateID)
	}

	if description != "" {
		pt.Description = description
	}

	if statement != "" {
		pt.Statement = statement
	}

	if name != "" {
		pt.Name = name
	}

	pt.LastUpdated = time.Now()

	return clonePolicyTemplate(pt), nil
}

// DeletePolicyTemplate removes a policy template from the given policy
// store. Per the real SDK's documented behavior ("This operation also
// deletes any policies that were created from the specified policy
// template"), it also cascade-deletes every TEMPLATE_LINKED policy that
// references this template -- otherwise those policies would be left
// pointing at a nonexistent template (a dangling reference visible to
// GetPolicy/ListPolicies/BatchGetPolicy, and silently dropped from Cedar
// evaluation, since resolveStatementLocked treats a missing template as an
// empty statement).
func (b *InMemoryBackend) DeletePolicyTemplate(policyStoreID, policyTemplateID string) error {
	b.mu.Lock("DeletePolicyTemplate")
	defer b.mu.Unlock()

	if !b.policyStores.Has(policyStoreID) {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if !b.policyTemplates.Has(policyTemplateKey(policyStoreID, policyTemplateID)) {
		return fmt.Errorf("%w: policy template %s not found", ErrPolicyTemplateNotFound, policyTemplateID)
	}

	// Index result slices mutate under Delete, so clone before the delete loop
	// (same pattern as DeletePolicyStore's cascade).
	for _, p := range slices.Clone(b.policiesByStore.Get(policyStoreID)) {
		if p.PolicyType != policyTypeTemplateLinked || p.PolicyTemplateID != policyTemplateID {
			continue
		}

		resourceARN := policyARN(b.accountID, policyStoreID, p.PolicyID)
		delete(b.arnIndex, resourceARN)
		delete(b.resourceTags, resourceARN)
		b.policies.Delete(policyKey(policyStoreID, p.PolicyID))
	}

	resourceARN := policyTemplateARN(b.accountID, policyStoreID, policyTemplateID)
	delete(b.arnIndex, resourceARN)
	delete(b.resourceTags, resourceARN)
	b.policyTemplates.Delete(policyTemplateKey(policyStoreID, policyTemplateID))
	b.invalidatePolicySetCache(policyStoreID)

	return nil
}

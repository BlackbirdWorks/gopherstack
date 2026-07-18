package verifiedpermissions

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// policyStoreARN builds the ARN for a policy store.
func policyStoreARN(accountID, _, policyStoreID string) string {
	return arnNoRegion(accountID, "policy-store", policyStoreID)
}

// clonePolicyStore returns a deep copy of a PolicyStore.
func clonePolicyStore(ps *PolicyStore) *PolicyStore {
	cp := *ps
	cp.Tags = make(map[string]string, len(ps.Tags))
	maps.Copy(cp.Tags, ps.Tags)

	return &cp
}

// CreatePolicyStore creates a new policy store.
func (b *InMemoryBackend) CreatePolicyStore(
	description string,
	tags map[string]string,
	validationMode, deletionProtection string,
) (*PolicyStore, error) {
	b.mu.Lock("CreatePolicyStore")
	defer b.mu.Unlock()

	merged := make(map[string]string, len(tags))
	maps.Copy(merged, tags)

	if err := validateTagInput(nil, merged, ErrValidation); err != nil {
		return nil, err
	}

	id := uuid.NewString()
	now := time.Now()

	if deletionProtection == "" {
		deletionProtection = DeletionProtectionDisabled
	}

	ps := &PolicyStore{
		PolicyStoreID:      id,
		Arn:                policyStoreARN(b.accountID, b.region, id),
		Description:        description,
		CreatedDate:        now,
		LastUpdated:        now,
		Tags:               merged,
		AccountID:          b.accountID,
		Region:             b.region,
		ValidationMode:     validationMode,
		DeletionProtection: deletionProtection,
	}
	b.policyStores.Put(ps)
	b.arnIndex[ps.Arn] = arnKindPolicyStore + ":" + id
	if len(merged) > 0 {
		b.resourceTags[ps.Arn] = maps.Clone(merged)
	}

	return clonePolicyStore(ps), nil
}

// GetPolicyStore returns the policy store with the given ID.
func (b *InMemoryBackend) GetPolicyStore(policyStoreID string) (*PolicyStore, error) {
	b.mu.RLock("GetPolicyStore")
	defer b.mu.RUnlock()

	ps, ok := b.policyStores.Get(policyStoreID)
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	return clonePolicyStore(ps), nil
}

// ListPolicyStores returns all policy stores sorted by creation date (newest first).
func (b *InMemoryBackend) ListPolicyStores(nextToken string, maxResults int) ([]PolicyStore, string) {
	b.mu.RLock("ListPolicyStores")
	defer b.mu.RUnlock()

	all := b.policyStores.All()
	out := make([]PolicyStore, 0, len(all))
	for _, ps := range all {
		out = append(out, *clonePolicyStore(ps))
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedDate.After(out[j].CreatedDate)
	})

	return paginate(out, nextToken, maxResults, func(ps PolicyStore) string { return ps.PolicyStoreID })
}

// UpdatePolicyStore updates a policy store.
func (b *InMemoryBackend) UpdatePolicyStore(
	policyStoreID, description, validationMode, deletionProtection string,
) (*PolicyStore, error) {
	b.mu.Lock("UpdatePolicyStore")
	defer b.mu.Unlock()

	ps, ok := b.policyStores.Get(policyStoreID)
	if !ok {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	ps.Description = description
	if validationMode != "" {
		ps.ValidationMode = validationMode
	}

	if deletionProtection != "" {
		ps.DeletionProtection = deletionProtection
	}

	ps.LastUpdated = time.Now()

	return clonePolicyStore(ps), nil
}

// DeletePolicyStore removes a policy store and all its policies and templates.
func (b *InMemoryBackend) DeletePolicyStore(policyStoreID string) error {
	b.mu.Lock("DeletePolicyStore")
	defer b.mu.Unlock()

	ps, ok := b.policyStores.Get(policyStoreID)
	if !ok {
		return fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if ps.DeletionProtection == DeletionProtectionEnabled {
		return fmt.Errorf("%w: policy store %s has deletion protection enabled", ErrConflict, policyStoreID)
	}

	// Remove ARN index entries for all child resources, then delete the
	// child resources themselves. Index result slices mutate under Delete,
	// so clone before the delete loop.
	for _, p := range slices.Clone(b.policiesByStore.Get(policyStoreID)) {
		delete(b.arnIndex, policyARN(b.accountID, policyStoreID, p.PolicyID))
		b.policies.Delete(policyKey(policyStoreID, p.PolicyID))
	}

	for _, pt := range slices.Clone(b.policyTemplatesByStore.Get(policyStoreID)) {
		delete(b.arnIndex, policyTemplateARN(b.accountID, policyStoreID, pt.PolicyTemplateID))
		b.policyTemplates.Delete(policyTemplateKey(policyStoreID, pt.PolicyTemplateID))
	}

	for _, is := range slices.Clone(b.identitySourcesByStore.Get(policyStoreID)) {
		delete(b.arnIndex, identitySourceARN(b.accountID, policyStoreID, is.IdentitySourceID))
		b.identitySources.Delete(identitySourceKey(policyStoreID, is.IdentitySourceID))
	}

	delete(b.arnIndex, ps.Arn)
	b.policyStores.Delete(policyStoreID)
	b.schemas.Delete(policyStoreID)

	return nil
}

package verifiedpermissions

import (
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// policyStoreAliasARN builds the ARN for a policy store alias. Unlike policy
// stores/policies/templates/identity sources (arnNoRegion -- see its doc),
// real AWS's own CreatePolicyStoreAlias/GetPolicyStoreAlias/
// ListPolicyStoreAliases example responses consistently include the region
// (e.g. "arn:aws:verifiedpermissions:us-east-1:123456789012:policy-store-alias/example-policy-store"),
// so alias ARNs use arn.Build's normal (region-populated) form instead.
func policyStoreAliasARN(region, accountID, aliasName string) string {
	return arn.Build("verifiedpermissions", region, accountID, aliasName)
}

// clonePolicyStoreAlias returns a shallow copy of a PolicyStoreAlias (every
// field is a value type, so a struct copy is a full deep copy).
func clonePolicyStoreAlias(a *PolicyStoreAlias) *PolicyStoreAlias {
	cp := *a

	return &cp
}

// CreatePolicyStoreAlias creates an alias pointing at policyStoreID.
//
// Real AWS semantics (this op is new to the SDK; verified against the API
// reference since no prior gopherstack pass audited it):
//   - aliasName must be prefixed "policy-store-alias/" (ValidationException).
//   - policyStoreID must reference an existing policy store, identified by
//     ID only -- unlike almost every other policyStoreId parameter in this
//     API (see Handler.resolvePolicyStoreID), alias resolution does NOT
//     apply here: "The associated policy store must be specified using its
//     ID. The alias name cannot be used." A nonexistent target is
//     ResourceNotFoundException.
//   - Idempotent on an exact (aliasName, policyStoreId) repeat against an
//     Active alias: replays the existing alias instead of erroring ("For
//     each duplicate CreatePolicyStoreAlias request, a Success response
//     will be returned and a new policy store alias will not be created").
//   - An aliasName already in use for a DIFFERENT policyStoreId, or
//     currently in the PendingDeletion state (real SDK's GetPolicyStoreAlias
//     doc: "creating a policy store alias with the same alias name will
//     fail" while PendingDeletion -- no exception carved out for the
//     same-target-store case), is a ConflictException.
func (b *InMemoryBackend) CreatePolicyStoreAlias(aliasName, policyStoreID string) (*PolicyStoreAlias, error) {
	b.mu.Lock("CreatePolicyStoreAlias")
	defer b.mu.Unlock()

	if !strings.HasPrefix(aliasName, policyStoreAliasPrefix) {
		return nil, fmt.Errorf("%w: aliasName must be prefixed %q", ErrValidation, policyStoreAliasPrefix)
	}

	if !b.policyStores.Has(policyStoreID) {
		return nil, fmt.Errorf("%w: policy store %s not found", ErrPolicyStoreNotFound, policyStoreID)
	}

	if existing, ok := b.policyStoreAliases.Get(aliasName); ok {
		if existing.State == AliasStateActive && existing.PolicyStoreID == policyStoreID {
			return clonePolicyStoreAlias(existing), nil
		}

		return nil, fmt.Errorf("%w: policy store alias %s already exists", ErrConflict, aliasName)
	}

	a := &PolicyStoreAlias{
		AliasName:     aliasName,
		Arn:           policyStoreAliasARN(b.region, b.accountID, aliasName),
		PolicyStoreID: policyStoreID,
		State:         AliasStateActive,
		CreatedAt:     time.Now(),
	}
	b.policyStoreAliases.Put(a)

	return clonePolicyStoreAlias(a), nil
}

// GetPolicyStoreAlias returns the alias with the given name, regardless of
// its state -- reporting Active vs PendingDeletion is exactly this op's job.
func (b *InMemoryBackend) GetPolicyStoreAlias(aliasName string) (*PolicyStoreAlias, error) {
	b.mu.RLock("GetPolicyStoreAlias")
	defer b.mu.RUnlock()

	a, ok := b.policyStoreAliases.Get(aliasName)
	if !ok {
		return nil, fmt.Errorf("%w: policy store alias %s not found", ErrPolicyStoreNotFound, aliasName)
	}

	return clonePolicyStoreAlias(a), nil
}

// ListPolicyStoreAliases returns all policy store aliases in the account,
// optionally narrowed to one policy store, sorted by creation date ascending
// and paginated -- the same shape/pagination convention
// ListPolicyTemplates/ListIdentitySources use (see listByPolicyStore).
func (b *InMemoryBackend) ListPolicyStoreAliases(
	policyStoreID, nextToken string, maxResults int,
) ([]PolicyStoreAlias, string) {
	b.mu.RLock("ListPolicyStoreAliases")
	defer b.mu.RUnlock()

	source := b.policyStoreAliases.All()
	if policyStoreID != "" {
		source = b.policyStoreAliasesByStore.Get(policyStoreID)
	}

	return listByPolicyStore(source, nextToken, maxResults,
		func(a *PolicyStoreAlias) PolicyStoreAlias { return *clonePolicyStoreAlias(a) },
		func(a PolicyStoreAlias) time.Time { return a.CreatedAt },
		func(a PolicyStoreAlias) string { return a.AliasName },
	)
}

// DeletePolicyStoreAlias deletes the named alias. Idempotent: a nonexistent
// aliasName is a no-op success, matching the real SDK's documented
// idempotency ("If you specify a policy store alias that does not exist, the
// request response will still return a successful HTTP 200 status code").
// hardDelete selects DeletionMode=HardDelete (immediate removal, bypassing
// PendingDeletion); the default (false, SoftDelete) instead transitions the
// alias to PendingDeletion -- it remains visible via
// GetPolicyStoreAlias/ListPolicyStoreAliases but is ineligible for a new
// CreatePolicyStoreAlias with the same name and for policyStoreId-alias
// resolution elsewhere (see ResolvePolicyStoreAlias).
func (b *InMemoryBackend) DeletePolicyStoreAlias(aliasName string, hardDelete bool) error {
	b.mu.Lock("DeletePolicyStoreAlias")
	defer b.mu.Unlock()

	a, ok := b.policyStoreAliases.Get(aliasName)
	if !ok {
		return nil
	}

	if hardDelete {
		b.policyStoreAliases.Delete(aliasName)

		return nil
	}

	a.State = AliasStatePendingDeletion

	return nil
}

// ResolvePolicyStoreAlias resolves an alias name (prefixed
// "policy-store-alias/") to its underlying policy store ID, for operations
// that accept either a policy store ID or an alias name in their
// policyStoreId field (see Handler.resolvePolicyStoreID). Only an Active
// alias resolves; a nonexistent or PendingDeletion alias is
// ResourceNotFoundException -- matching the real SDK's documented behavior:
// "If the policy store alias is used in an API that has a policyStoreId
// field, the operation will fail with a ResourceNotFound exception" once the
// alias enters PendingDeletion.
func (b *InMemoryBackend) ResolvePolicyStoreAlias(aliasName string) (string, error) {
	b.mu.RLock("ResolvePolicyStoreAlias")
	defer b.mu.RUnlock()

	a, ok := b.policyStoreAliases.Get(aliasName)
	if !ok || a.State != AliasStateActive {
		return "", fmt.Errorf("%w: policy store alias %s not found", ErrPolicyStoreNotFound, aliasName)
	}

	return a.PolicyStoreID, nil
}

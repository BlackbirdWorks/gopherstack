package cloudwatchlogs

import (
	"fmt"
	"sort"
	"strconv"
	"time"
)

// resourcePolicyStoreKey is this backend's storage key for a resource
// policy. Real AWS enforces "a maximum of 10 policies without resourceARN
// and one per LogGroup resourceARN" (PutResourcePolicy doc comment), so
// account-scope policies are keyed by name and resource-scope policies are
// keyed by their (unique) resourceArn.
func resourcePolicyStoreKey(policyName, resourceArn string) string {
	if resourceArn != "" {
		return "resource:" + resourceArn
	}

	return "account:" + policyName
}

// checkResourcePolicyRevision enforces Put/DeleteResourcePolicy's optional
// optimistic-concurrency check (ExpectedRevisionId -- "Required when
// resourceArn is provided to prevent concurrent modifications").
// expected == nil means the caller didn't ask for one.
func checkResourcePolicyRevision(existing *ResourcePolicy, expected *string) error {
	if expected == nil {
		return nil
	}

	var current string
	if existing != nil {
		current = existing.RevisionID
	}

	if *expected != current {
		return fmt.Errorf("%w: expectedRevisionId does not match the current policy revision", ErrValidation)
	}

	return nil
}

func nextResourcePolicyRevision(existing *ResourcePolicy) string {
	if existing == nil {
		return "1"
	}

	n, _ := strconv.Atoi(existing.RevisionID)

	return strconv.Itoa(n + 1)
}

// PutResourcePolicy creates or updates a resource-based policy. resourceArn
// (optional) selects the newer resource-scoped policy family over the
// legacy account-scoped one; see resourcePolicyStoreKey.
func (b *InMemoryBackend) PutResourcePolicy(
	policyName, policyDocument, resourceArn string, expectedRevisionID *string,
) (*ResourcePolicy, error) {
	if policyName == "" {
		return nil, fmt.Errorf("%w: policyName is required", ErrValidation)
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	key := resourcePolicyStoreKey(policyName, resourceArn)
	existing, _ := b.resourcePolicies.Get(key)

	if err := checkResourcePolicyRevision(existing, expectedRevisionID); err != nil {
		return nil, err
	}

	scope := policyScopeAccount
	if resourceArn != "" {
		scope = policyScopeResource
	}

	p := ResourcePolicy{
		PolicyName:      policyName,
		PolicyDocument:  policyDocument,
		ResourceArn:     resourceArn,
		PolicyScope:     scope,
		RevisionID:      nextResourcePolicyRevision(existing),
		LastUpdatedTime: time.Now().UTC().UnixMilli(),
	}
	stored := p
	b.resourcePolicies.Put(&stored)

	return &p, nil
}

// DescribeResourcePolicies returns resource policies, sorted by name, with
// Limit/NextToken pagination (api_op_DescribeResourcePolicies.go:29-42 --
// no documented default, so this falls back to defaultDescribeLimit like
// every other Describe op in this service, e.g. DescribeLogStreams).
// resourceArn (when set) looks up the single resource-scoped policy on that
// ARN. Otherwise policyScope filters by scope, defaulting to ACCOUNT per
// DescribeResourcePoliciesInput's own doc comment ("When not specified,
// defaults to ACCOUNT").
func (b *InMemoryBackend) DescribeResourcePolicies(
	policyScope, resourceArn, nextToken string, limit int,
) ([]ResourcePolicy, string) {
	b.mu.RLock("DescribeResourcePolicies")
	defer b.mu.RUnlock()

	if resourceArn != "" {
		p, ok := b.resourcePolicies.Get(resourcePolicyStoreKey("", resourceArn))
		if !ok {
			return []ResourcePolicy{}, ""
		}

		return []ResourcePolicy{*p}, ""
	}

	if policyScope == "" {
		policyScope = policyScopeAccount
	}

	out := make([]ResourcePolicy, 0, b.resourcePolicies.Len())
	for _, p := range b.resourcePolicies.All() {
		if p.PolicyScope != policyScope {
			continue
		}
		out = append(out, *p)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PolicyName < out[j].PolicyName })

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(out) {
		return []ResourcePolicy{}, ""
	}

	if limit <= 0 {
		limit = defaultDescribeLimit
	}

	end := startIdx + limit

	var outToken string
	if end < len(out) {
		outToken = encodeNextToken(end)
	} else {
		end = len(out)
	}

	return out[startIdx:end], outToken
}

// DeleteResourcePolicy removes a resource policy by name (account scope) or
// resourceArn (resource scope).
func (b *InMemoryBackend) DeleteResourcePolicy(policyName, resourceArn string, expectedRevisionID *string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	key := resourcePolicyStoreKey(policyName, resourceArn)

	existing, ok := b.resourcePolicies.Get(key)
	if !ok {
		return fmt.Errorf("%w: resource policy %q not found", ErrResourcePolicyNotFound, policyName)
	}

	if err := checkResourcePolicyRevision(existing, expectedRevisionID); err != nil {
		return err
	}

	b.resourcePolicies.Delete(key)

	return nil
}

// PutIndexPolicy creates or updates an index policy for a log group.
func (b *InMemoryBackend) PutIndexPolicy(logGroupIdentifier, policyDocument string) (*IndexPolicy, error) {
	if logGroupIdentifier == "" {
		return nil, fmt.Errorf("%w: logGroupIdentifier is required", ErrValidation)
	}

	b.mu.Lock("PutIndexPolicy")
	defer b.mu.Unlock()

	p := IndexPolicy{
		LogGroupIdentifier: logGroupIdentifier,
		PolicyDocument:     policyDocument,
		LastUpdateTime:     time.Now().UTC().UnixMilli(),
		Source:             indexSourceLogGroup,
	}
	stored := p
	b.indexPolicies.Put(&stored)

	return &p, nil
}

// DescribeIndexPolicies returns all index policies sorted by log group identifier.
func (b *InMemoryBackend) DescribeIndexPolicies() []IndexPolicy {
	b.mu.RLock("DescribeIndexPolicies")
	defer b.mu.RUnlock()

	out := make([]IndexPolicy, 0, b.indexPolicies.Len())
	for _, p := range b.indexPolicies.All() {
		out = append(out, *p)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].LogGroupIdentifier < out[j].LogGroupIdentifier })

	return out
}

// DeleteIndexPolicy removes the index policy for a log group.
func (b *InMemoryBackend) DeleteIndexPolicy(logGroupIdentifier string) error {
	b.mu.Lock("DeleteIndexPolicy")
	defer b.mu.Unlock()

	if !b.indexPolicies.Delete(logGroupIdentifier) {
		return fmt.Errorf("%w: index policy for %q not found", ErrIndexPolicyNotFound, logGroupIdentifier)
	}

	return nil
}

// validStorageTiers returns the allowed values for PutStorageTierPolicy's
// StorageTier field (aws-sdk-go-v2 types.StorageTier).
func validStorageTiers() map[string]struct{} {
	return map[string]struct{}{
		StorageTierStandard:           {},
		StorageTierIntelligentTiering: {},
	}
}

// GetStorageTierPolicy returns the account-level storage tier policy (see
// the StorageTierPolicy doc comment in models.go for why this is a
// singleton, not per-log-group). Before any PutStorageTierPolicy call, real
// AWS's default active tier is STANDARD with no LastUpdatedTime
// (GetStorageTierPolicyOutput.LastUpdatedTime is a nilable *int64), so an
// empty table synthesizes that default rather than requiring a Put first.
func (b *InMemoryBackend) GetStorageTierPolicy() StorageTierPolicy {
	b.mu.RLock("GetStorageTierPolicy")
	defer b.mu.RUnlock()

	if p, ok := b.storageTierPolicy.Get(storageTierPolicySingletonKey); ok {
		return *p
	}

	return StorageTierPolicy{StorageTier: StorageTierStandard}
}

// PutStorageTierPolicy sets the account-level storage tier policy.
func (b *InMemoryBackend) PutStorageTierPolicy(tier string) (*StorageTierPolicy, error) {
	if _, ok := validStorageTiers()[tier]; !ok {
		return nil, fmt.Errorf(
			"%w: invalid storageTier %q, must be STANDARD or INTELLIGENT_TIERING", ErrValidation, tier,
		)
	}

	b.mu.Lock("PutStorageTierPolicy")
	defer b.mu.Unlock()

	p := &StorageTierPolicy{StorageTier: tier, LastUpdatedTime: time.Now().UnixMilli()}
	b.storageTierPolicy.Put(p)

	cp := *p

	return &cp, nil
}

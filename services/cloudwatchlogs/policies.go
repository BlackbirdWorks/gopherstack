package cloudwatchlogs

import (
	"fmt"
	"sort"
	"time"
)

// PutResourcePolicy creates or updates a resource-based policy.
func (b *InMemoryBackend) PutResourcePolicy(policyName, policyDocument string) (*ResourcePolicy, error) {
	if policyName == "" {
		return nil, fmt.Errorf("%w: policyName is required", ErrValidation)
	}

	b.mu.Lock("PutResourcePolicy")
	defer b.mu.Unlock()

	p := ResourcePolicy{
		PolicyName:     policyName,
		PolicyDocument: policyDocument,
		LastUpdated:    time.Now().UTC(),
	}
	stored := p
	b.resourcePolicies.Put(&stored)

	return &p, nil
}

// DescribeResourcePolicies returns all resource policies, sorted by name.
func (b *InMemoryBackend) DescribeResourcePolicies() []ResourcePolicy {
	b.mu.RLock("DescribeResourcePolicies")
	defer b.mu.RUnlock()

	out := make([]ResourcePolicy, 0, b.resourcePolicies.Len())
	for _, p := range b.resourcePolicies.All() {
		out = append(out, *p)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PolicyName < out[j].PolicyName })

	return out
}

// DeleteResourcePolicy removes a resource policy by name.
func (b *InMemoryBackend) DeleteResourcePolicy(policyName string) error {
	b.mu.Lock("DeleteResourcePolicy")
	defer b.mu.Unlock()

	if !b.resourcePolicies.Delete(policyName) {
		return fmt.Errorf("%w: resource policy %q not found", ErrResourcePolicyNotFound, policyName)
	}

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
		LastUpdated:        time.Now().UTC(),
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

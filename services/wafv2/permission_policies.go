package wafv2

import (
	"context"
	"fmt"
)

// PutPermissionPolicy stores a permission policy for the given resource ARN.
func (b *InMemoryBackend) PutPermissionPolicy(ctx context.Context, resourceARN, policy string) error {
	b.mu.Lock("PutPermissionPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.permissionPoliciesStore(region)[resourceARN] = policy

	return nil
}

// DeletePermissionPolicy removes the permission policy for the given resource ARN.
func (b *InMemoryBackend) DeletePermissionPolicy(ctx context.Context, resourceARN string) error {
	b.mu.Lock("DeletePermissionPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	if _, ok := b.permissionPolicies[region][resourceARN]; !ok {
		return fmt.Errorf("%w: no permission policy found for resource %q", ErrPermissionPolicyNotFound, resourceARN)
	}

	delete(b.permissionPolicies[region], resourceARN)

	return nil
}

// GetPermissionPolicy returns the permission policy for the given resource ARN.
func (b *InMemoryBackend) GetPermissionPolicy(ctx context.Context, resourceARN string) (string, error) {
	b.mu.RLock("GetPermissionPolicy")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	policy, ok := b.permissionPolicies[region][resourceARN]
	if !ok {
		return "", fmt.Errorf(
			"%w: no permission policy found for resource %q",
			ErrPermissionPolicyNotFound,
			resourceARN,
		)
	}

	return policy, nil
}

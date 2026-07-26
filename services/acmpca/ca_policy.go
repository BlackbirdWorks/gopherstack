package acmpca

import (
	"context"
	"fmt"
)

// PutPolicy stores a resource policy on the given CA.
func (b *InMemoryBackend) PutPolicy(ctx context.Context, caARN, policy string) error {
	if err := validateRequiredParameter(caARN, "ResourceArn"); err != nil {
		return err
	}

	if policy == "" {
		return fmt.Errorf("%w: Policy is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutPolicy")
	defer b.mu.Unlock()

	if _, ok := b.caGet(region, caARN); !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	b.policiesStore(region)[caARN] = policy

	return nil
}

// GetPolicy returns the resource policy for the given CA.
func (b *InMemoryBackend) GetPolicy(ctx context.Context, caARN string) (string, error) {
	if err := validateRequiredParameter(caARN, "ResourceArn"); err != nil {
		return "", err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.caGet(region, caARN); !ok {
		return "", fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	policy, ok := b.policiesStoreRO(region)[caARN]
	if !ok {
		return "", fmt.Errorf("%w: policy for CA %s not found", ErrPolicyNotFound, caARN)
	}

	return policy, nil
}

// DeletePolicy deletes the resource policy for the given CA.
func (b *InMemoryBackend) DeletePolicy(ctx context.Context, caARN string) error {
	if err := validateRequiredParameter(caARN, "ResourceArn"); err != nil {
		return err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	if _, ok := b.caGet(region, caARN); !ok {
		return fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	policies := b.policiesStore(region)
	if _, ok := policies[caARN]; !ok {
		return fmt.Errorf("%w: policy for CA %s not found", ErrPolicyNotFound, caARN)
	}

	delete(policies, caARN)

	return nil
}

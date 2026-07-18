package efs

import (
	"context"
	"fmt"
)

func isValidTransitionToIA(v string) bool {
	switch v {
	case "AFTER_7_DAYS", "AFTER_14_DAYS", "AFTER_30_DAYS", "AFTER_60_DAYS",
		"AFTER_90_DAYS", "AFTER_180_DAYS", "AFTER_270_DAYS", "AFTER_365_DAYS", "NONE":
		return true
	default:
		return false
	}
}

func isValidTransitionToPrimary(v string) bool {
	return v == "AFTER_1_ACCESS"
}

func isValidTransitionToArchive(v string) bool {
	switch v {
	case "AFTER_1_ACCESS", "AFTER_7_DAYS", "AFTER_14_DAYS", "AFTER_30_DAYS",
		"AFTER_60_DAYS", "AFTER_90_DAYS", "AFTER_180_DAYS", "AFTER_270_DAYS",
		"AFTER_365_DAYS", "AFTER_90_DAYS_1":
		return true
	default:
		return false
	}
}

// DescribeLifecycleConfiguration returns lifecycle policies for a file system.
func (b *InMemoryBackend) DescribeLifecycleConfiguration(
	ctx context.Context,
	fileSystemID string,
) ([]LifecyclePolicy, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeLifecycleConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.fileSystems.Get(regionKey(region, fileSystemID)); !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	policies := b.lifecycleStore(region)[fileSystemID]
	if policies == nil {
		return []LifecyclePolicy{}, nil
	}

	result := make([]LifecyclePolicy, len(policies))
	copy(result, policies)

	return result, nil
}

// validateLifecyclePolicies checks that each policy's transition fields are valid AWS enum values.
func validateLifecyclePolicies(policies []LifecyclePolicy) error {
	for i, p := range policies {
		if p.TransitionToIA != "" && !isValidTransitionToIA(p.TransitionToIA) {
			return fmt.Errorf(
				"%w: invalid TransitionToIA value %q at index %d",
				ErrValidation,
				p.TransitionToIA,
				i,
			)
		}
		if p.TransitionToPrimaryStorageClass != "" &&
			!isValidTransitionToPrimary(p.TransitionToPrimaryStorageClass) {
			return fmt.Errorf(
				"%w: invalid TransitionToPrimaryStorageClass value %q at index %d",
				ErrValidation,
				p.TransitionToPrimaryStorageClass,
				i,
			)
		}
		if p.TransitionToArchive != "" && !isValidTransitionToArchive(p.TransitionToArchive) {
			return fmt.Errorf(
				"%w: invalid TransitionToArchive value %q at index %d",
				ErrValidation,
				p.TransitionToArchive,
				i,
			)
		}
	}

	return nil
}

// PutLifecycleConfiguration sets lifecycle policies for a file system.
func (b *InMemoryBackend) PutLifecycleConfiguration(
	ctx context.Context,
	fileSystemID string,
	policies []LifecyclePolicy,
) ([]LifecyclePolicy, error) {
	if err := validateLifecyclePolicies(policies); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutLifecycleConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.fileSystems.Get(regionKey(region, fileSystemID)); !ok {
		return nil, fmt.Errorf("%w: file system %s not found", ErrNotFound, fileSystemID)
	}

	stored := make([]LifecyclePolicy, len(policies))
	copy(stored, policies)
	b.lifecycleStore(region)[fileSystemID] = stored

	result := make([]LifecyclePolicy, len(stored))
	copy(result, stored)

	return result, nil
}

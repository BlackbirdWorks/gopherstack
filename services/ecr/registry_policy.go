package ecr

import (
	"context"
	"fmt"
)

// SetRegistryPolicyInternal sets the registry policy directly for testing.
func (b *InMemoryBackend) SetRegistryPolicyInternal(policy string) {
	b.mu.Lock("SetRegistryPolicyInternal")
	defer b.mu.Unlock()

	b.registryPolicy = policy
}

// DeleteRegistryPolicy deletes the registry-level IAM policy.
func (b *InMemoryBackend) DeleteRegistryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
) (*RegistryPolicyResult, error) {
	b.mu.Lock("DeleteRegistryPolicy")
	defer b.mu.Unlock()

	if b.registryPolicy == "" {
		return nil, fmt.Errorf("%w: no registry policy found", ErrRegistryPolicyNotFound)
	}

	policy := b.registryPolicy
	b.registryPolicy = ""

	return &RegistryPolicyResult{
		PolicyText: policy,
		RegistryID: b.accountID,
		Status:     "DELETED",
	}, nil
}

// DescribeRegistry returns registry-wide metadata.
func (b *InMemoryBackend) DescribeRegistry(
	ctx context.Context, //nolint:revive // existing issue.
) (*RegistryDescription, error) {
	b.mu.RLock("DescribeRegistry")
	defer b.mu.RUnlock()

	return &RegistryDescription{
		RegistryID:               b.accountID,
		ReplicationConfiguration: copyReplicationConfig(b.replicationConfig),
	}, nil
}

// GetRegistryPolicy returns the registry-level IAM policy.
func (b *InMemoryBackend) GetRegistryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
) (*RegistryPolicyResult, error) {
	b.mu.RLock("GetRegistryPolicy")
	defer b.mu.RUnlock()

	if b.registryPolicy == "" {
		return nil, fmt.Errorf("%w: no registry policy found", ErrRegistryPolicyNotFound)
	}

	return &RegistryPolicyResult{
		PolicyText: b.registryPolicy,
		RegistryID: b.accountID,
		Status:     imageStatusActive,
	}, nil
}

// GetRegistryScanningConfiguration returns the registry scanning configuration.
func (b *InMemoryBackend) GetRegistryScanningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
) (*RegistryScanningSettings, error) {
	b.mu.RLock("GetRegistryScanningConfiguration")
	defer b.mu.RUnlock()

	return copyRegistryScanningSettings(b.registryScanningConfig), nil
}

// PutRegistryPolicy creates or replaces the registry-level IAM policy.
func (b *InMemoryBackend) PutRegistryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	policyText string,
) (*RegistryPolicyResult, error) {
	b.mu.Lock("PutRegistryPolicy")
	defer b.mu.Unlock()

	b.registryPolicy = policyText

	return &RegistryPolicyResult{
		PolicyText: policyText,
		RegistryID: b.accountID,
		Status:     "SetComplete",
	}, nil
}

// PutRegistryScanningConfiguration updates the registry scanning configuration.
func (b *InMemoryBackend) PutRegistryScanningConfiguration(
	ctx context.Context, //nolint:revive // existing issue.
	settings *RegistryScanningSettings,
) (*RegistryScanningSettings, error) {
	b.mu.Lock("PutRegistryScanningConfiguration")
	defer b.mu.Unlock()

	if settings == nil {
		settings = &RegistryScanningSettings{ScanType: scanTypeBasic}
	}

	if settings.ScanType == "" {
		settings.ScanType = scanTypeBasic
	}

	b.registryScanningConfig = copyRegistryScanningSettings(settings)

	return copyRegistryScanningSettings(b.registryScanningConfig), nil
}

func copyRegistryScanningSettings(in *RegistryScanningSettings) *RegistryScanningSettings {
	if in == nil {
		return &RegistryScanningSettings{ScanType: scanTypeBasic}
	}

	out := &RegistryScanningSettings{
		Rules:    make([]RegistryScanningRule, len(in.Rules)),
		ScanType: in.ScanType,
	}
	for i, rule := range in.Rules {
		out.Rules[i] = RegistryScanningRule{
			RepositoryFilters: append([]RepositoryFilter(nil), rule.RepositoryFilters...),
			ScanFrequency:     rule.ScanFrequency,
		}
	}

	return out
}

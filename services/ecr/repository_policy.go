package ecr

import (
	"context"
	"fmt"
)

// GetRepositoryPolicy returns the repository-level policy.
func (b *InMemoryBackend) GetRepositoryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
) (*RepositoryPolicyResult, error) {
	b.mu.RLock("GetRepositoryPolicy")
	defer b.mu.RUnlock()

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	entry, ok := b.repositoryPolicies.Get(repositoryName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryPolicyNotFound, repositoryName)
	}

	return &RepositoryPolicyResult{
		PolicyText:     entry.PolicyText,
		RegistryID:     b.accountID,
		RepositoryName: repositoryName,
	}, nil
}

// SetRepositoryPolicy creates or replaces the repository-level IAM policy.
func (b *InMemoryBackend) SetRepositoryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName, policyText string,
) (*RepositoryPolicyResult, error) {
	b.mu.Lock("SetRepositoryPolicy")
	defer b.mu.Unlock()

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	b.repositoryPolicies.Put(&repositoryPolicyEntry{RepositoryName: repositoryName, PolicyText: policyText})

	return &RepositoryPolicyResult{
		PolicyText:     policyText,
		RegistryID:     b.accountID,
		RepositoryName: repositoryName,
	}, nil
}

// DeleteRepositoryPolicy deletes the repository-level policy.
func (b *InMemoryBackend) DeleteRepositoryPolicy(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
) (*RepositoryPolicyResult, error) {
	b.mu.Lock("DeleteRepositoryPolicy")
	defer b.mu.Unlock()

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	entry, ok := b.repositoryPolicies.Get(repositoryName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryPolicyNotFound, repositoryName)
	}

	policyText := entry.PolicyText
	b.repositoryPolicies.Delete(repositoryName)

	return &RepositoryPolicyResult{
		PolicyText:     policyText,
		RegistryID:     b.accountID,
		RepositoryName: repositoryName,
	}, nil
}

package ecrpublic

import "fmt"

// GetRepositoryPolicy returns the repository's resource policy text.
func (b *InMemoryBackend) GetRepositoryPolicy(registryID, name string) (string, error) {
	b.mu.RLock("GetRepositoryPolicy")
	defer b.mu.RUnlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return "", err
	}

	repo, ok := b.repos.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
	}

	if !repo.HasPolicy {
		return "", fmt.Errorf("%w: %s", ErrRepositoryPolicyNotFound, name)
	}

	return repo.PolicyText, nil
}

// SetRepositoryPolicy sets or replaces the repository's resource policy text.
func (b *InMemoryBackend) SetRepositoryPolicy(registryID, name, policyText string) (string, error) {
	b.mu.Lock("SetRepositoryPolicy")
	defer b.mu.Unlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return "", err
	}

	repo, ok := b.repos.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
	}

	repo.PolicyText = policyText
	repo.HasPolicy = true

	return repo.PolicyText, nil
}

// DeleteRepositoryPolicy deletes the repository's resource policy, returning
// the deleted policy text.
func (b *InMemoryBackend) DeleteRepositoryPolicy(registryID, name string) (string, error) {
	b.mu.Lock("DeleteRepositoryPolicy")
	defer b.mu.Unlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return "", err
	}

	repo, ok := b.repos.Get(name)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrRepositoryNotFound, name)
	}

	if !repo.HasPolicy {
		return "", fmt.Errorf("%w: %s", ErrRepositoryPolicyNotFound, name)
	}

	deleted := repo.PolicyText
	repo.PolicyText = ""
	repo.HasPolicy = false

	return deleted, nil
}

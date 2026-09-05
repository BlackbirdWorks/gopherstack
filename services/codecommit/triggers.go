package codecommit

import "fmt"

// GetRepositoryTriggers returns triggers for a repository.
func (b *InMemoryBackend) GetRepositoryTriggers(repoName string) ([]RepositoryTrigger, error) {
	b.mu.RLock("GetRepositoryTriggers")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	triggers := b.triggers[repoName]
	result := make([]RepositoryTrigger, len(triggers))
	copy(result, triggers)

	return result, nil
}

// PutRepositoryTriggers replaces triggers for a repository.
func (b *InMemoryBackend) PutRepositoryTriggers(repoName string, triggers []RepositoryTrigger) error {
	b.mu.Lock("PutRepositoryTriggers")
	defer b.mu.Unlock()

	if !b.repositories.Has(repoName) {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	b.triggers[repoName] = make([]RepositoryTrigger, len(triggers))
	copy(b.triggers[repoName], triggers)

	return nil
}

// TestRepositoryTriggers returns the names of triggers that succeeded. It
// tests the trigger list passed IN THIS CALL, not whatever is currently
// saved via PutRepositoryTriggers -- real AWS: "does not change or create a
// repository trigger" (codecommit@v1.36.4
// api_op_TestRepositoryTriggers.go), so the two are independent inputs.
func (b *InMemoryBackend) TestRepositoryTriggers(repoName string, triggers []RepositoryTrigger) ([]string, error) {
	b.mu.RLock("TestRepositoryTriggers")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	names := make([]string, 0, len(triggers))
	for _, t := range triggers {
		names = append(names, t.Name)
	}

	return names, nil
}

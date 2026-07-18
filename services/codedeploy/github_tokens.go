package codedeploy

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// ListGitHubAccountTokenNames returns all stored GitHub account token names.
func (b *InMemoryBackend) ListGitHubAccountTokenNames() []string {
	b.mu.RLock("ListGitHubAccountTokenNames")
	defer b.mu.RUnlock()

	names := collections.SortedKeys(b.githubTokens)

	return names
}

// DeleteGitHubAccountToken removes a stored GitHub account token name.
func (b *InMemoryBackend) DeleteGitHubAccountToken(name string) error {
	b.mu.Lock("DeleteGitHubAccountToken")
	defer b.mu.Unlock()

	if _, ok := b.githubTokens[name]; !ok {
		return fmt.Errorf("%w: GitHub account token %s not found", ErrGitHubAccountTokenNotFound, name)
	}

	delete(b.githubTokens, name)

	return nil
}

// AddGitHubAccountTokenInternal adds a GitHub token name directly (for test seeding and internal use).
func (b *InMemoryBackend) AddGitHubAccountTokenInternal(name string) {
	b.mu.Lock("AddGitHubAccountTokenInternal")
	defer b.mu.Unlock()

	b.githubTokens[name] = struct{}{}
}

package ecr

import "fmt"

// foreignRegistry reports whether id names a registry other than this backend's account.
func (h *Handler) foreignRegistry(id string) bool {
	return id != "" && id != h.Backend.AccountID()
}

// checkRegistryForRepo maps a foreign registryId to RepositoryNotFoundException.
func (h *Handler) checkRegistryForRepo(id, repo string) error {
	if h.foreignRegistry(id) {
		return fmt.Errorf("%w: %s", ErrRepositoryNotFound, repo)
	}

	return nil
}

// checkRegistryForRule maps a foreign registryId to PullThroughCacheRuleNotFoundException.
func (h *Handler) checkRegistryForRule(id, prefix string) error {
	if h.foreignRegistry(id) {
		return fmt.Errorf("%w: %s", ErrPullThroughCacheRuleNotFound, prefix)
	}

	return nil
}

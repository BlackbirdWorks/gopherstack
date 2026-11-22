package ecr

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode = "ecr"
	quotaRepos       = "L-CFEB8E8D"
)

// ErrLimitExceeded is CreateRepository's LimitExceededException (ecr@v1.64.0 deserializers.go:853).
var ErrLimitExceeded = awserr.New("LimitExceededException", awserr.ErrConflict)

// SetQuotaLimiter makes CreateRepository honour the Service Quotas repository limit in every Region; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.quotaGate.Set(f)
	}
}

// RepositoryCount returns the repositories held in region; a Region never used holds none.
func (h *Handler) RepositoryCount(region string) int {
	if home, ok := h.Backend.(*InMemoryBackend); ok && (region == "" || region == home.region) {
		return home.repositoryCount()
	}

	for _, p := range h.peers.All() {
		if b, ok := p.Backend.(*InMemoryBackend); ok && b.region == region {
			return b.repositoryCount()
		}
	}

	return 0
}

func (b *InMemoryBackend) repositoryCount() int {
	b.mu.RLock("RepositoryCount")
	defer b.mu.RUnlock()

	return b.repos.Len()
}

func (b *InMemoryBackend) repositoryLimit(region string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, region, quotaServiceCode, quotaRepos)
}

func (b *InMemoryBackend) checkRepositoryQuotaLocked(limit quotalimit.Limit) error {
	if limit.Reached(b.repos.Len()) {
		return fmt.Errorf(
			"%w: the maximum number of repositories (%.0f) has been reached",
			ErrLimitExceeded,
			limit.Value,
		)
	}

	return nil
}

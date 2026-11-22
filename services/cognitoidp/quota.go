package cognitoidp

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode = "cognito-idp"
	quotaUserPools   = "L-66E6DF30"
)

// SetQuotaLimiter makes CreateUserPool honour the Service Quotas user pool limit in every Region; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	h.Backend.quotaGate.Set(f)
}

// UserPoolCount returns the user pools held in region; a Region never used holds none.
func (h *Handler) UserPoolCount(region string) int {
	if region == "" || region == h.Backend.region {
		return h.Backend.userPoolCount()
	}

	for _, p := range h.peers.All() {
		if p.Backend.region == region {
			return p.Backend.userPoolCount()
		}
	}

	return 0
}

func (b *InMemoryBackend) userPoolCount() int {
	b.mu.RLock("UserPoolCount")
	defer b.mu.RUnlock()

	return len(b.pools.All())
}

func (b *InMemoryBackend) userPoolLimit() quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, b.region, quotaServiceCode, quotaUserPools)
}

func (b *InMemoryBackend) checkUserPoolQuotaLocked(limit quotalimit.Limit) error {
	if limit.Reached(len(b.pools.All())) {
		return fmt.Errorf("%w: the maximum number of user pools (%.0f) has been reached", ErrLimitExceeded, limit.Value)
	}

	return nil
}

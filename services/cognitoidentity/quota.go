package cognitoidentity

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode  = "cognito-identity"
	quotaIdentityPool = "L-8692CE1C"
)

// ErrLimitExceeded is CreateIdentityPool's LimitExceededException (cognitoidentity@v1.36.4 deserializers.go:84).
var ErrLimitExceeded = awserr.New("LimitExceededException", awserr.ErrConflict)

// SetQuotaLimiter makes CreateIdentityPool honour the Service Quotas identity pool limit; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) { h.Backend.quotaGate.Set(f) }

// IdentityPoolCount returns the identity pools held in region; empty region means the backend's.
func (h *Handler) IdentityPoolCount(region string) int {
	b := h.Backend
	if region == "" {
		region = b.region
	}

	b.mu.RLock("IdentityPoolCount")
	defer b.mu.RUnlock()

	return len(b.poolsByRegion.Get(region))
}

func (b *InMemoryBackend) identityPoolLimit(region string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, region, quotaServiceCode, quotaIdentityPool)
}

func (b *InMemoryBackend) checkIdentityPoolQuotaLocked(region string, limit quotalimit.Limit) error {
	if limit.Reached(len(b.poolsByRegion.Get(region))) {
		return fmt.Errorf(
			"%w: the maximum number of identity pools (%.0f) has been reached",
			ErrLimitExceeded,
			limit.Value,
		)
	}

	return nil
}

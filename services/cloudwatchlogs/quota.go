package cloudwatchlogs

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode = "logs"
	quotaLogGroups   = "L-C7B9AAAB"
)

// ErrLogGroupLimitExceeded is CreateLogGroup's LimitExceededException when the Service Quotas limit is reached.
var ErrLogGroupLimitExceeded = errors.New("LimitExceededException")

// SetQuotaLimiter makes CreateLogGroup honour the Service Quotas log group limit; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.quotaGate.Set(f)
	}
}

// LogGroupCount returns the log groups held in region; empty region means the backend's.
func (h *Handler) LogGroupCount(region string) int {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return 0
	}

	if region == "" {
		region = b.region
	}

	b.mu.RLock("LogGroupCount")
	defer b.mu.RUnlock()

	return len(b.groupsInRegion(region))
}

func (b *InMemoryBackend) logGroupLimit(region string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, region, quotaServiceCode, quotaLogGroups)
}

func (b *InMemoryBackend) checkLogGroupQuotaLocked(region string, limit quotalimit.Limit) error {
	if limit.Reached(len(b.groupsInRegion(region))) {
		return fmt.Errorf(
			"%w: the maximum number of log groups (%.0f) has been reached",
			ErrLogGroupLimitExceeded,
			limit.Value,
		)
	}

	return nil
}

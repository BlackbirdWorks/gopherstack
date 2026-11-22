package firehose

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode    = "firehose"
	quotaDeliveryStream = "L-14BB0BE7"
)

// ErrLimitExceeded is CreateDeliveryStream's LimitExceededException (firehose@v1.46.4 deserializers.go:85).
var ErrLimitExceeded = awserr.New("LimitExceededException", awserr.ErrConflict)

// SetQuotaLimiter makes CreateDeliveryStream honour the Service Quotas stream limit; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.quotaGate.Set(f)
	}
}

// DeliveryStreamCount returns the delivery streams held in region; empty region means the backend's.
func (h *Handler) DeliveryStreamCount(region string) int {
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return 0
	}

	if region == "" {
		region = b.region
	}

	b.mu.RLock("DeliveryStreamCount")
	defer b.mu.RUnlock()

	return len(b.streamsByRegion.Get(region))
}

func (b *InMemoryBackend) streamLimit(region string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, region, quotaServiceCode, quotaDeliveryStream)
}

func (b *InMemoryBackend) checkStreamQuotaLocked(region string, limit quotalimit.Limit) error {
	if limit.Reached(len(b.streamsByRegion.Get(region))) {
		return fmt.Errorf(
			"%w: the maximum number of delivery streams (%.0f) has been reached",
			ErrLimitExceeded,
			limit.Value,
		)
	}

	return nil
}

func (b *InMemoryBackend) checkNewStreamLocked(region, name string, limit quotalimit.Limit) error {
	if _, ok := b.streams.Get(regionKey(region, name)); ok {
		return fmt.Errorf("%w: stream %s already exists", ErrAlreadyExists, name)
	}

	return b.checkStreamQuotaLocked(region, limit)
}

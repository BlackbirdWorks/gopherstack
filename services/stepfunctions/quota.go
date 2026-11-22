package stepfunctions

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode   = "states"
	quotaStateMachines = "L-B66B0F91"
	quotaActivities    = "L-A9562A73"
	quotaVersions      = "L-6AFCB355"
	quotaAliases       = "L-C1D5EBBB"
)

// The limit exceptions CreateStateMachine, CreateActivity, PublishStateMachineVersion and
// CreateStateMachineAlias declare (sfn@v1.49.0 deserializers.go:220, :88, :2812, :376).
var (
	ErrStateMachineLimitExceeded = errors.New("StateMachineLimitExceeded")
	ErrActivityLimitExceeded     = errors.New("ActivityLimitExceeded")
	ErrServiceQuotaExceeded      = errors.New("ServiceQuotaExceededException")
)

// SetQuotaLimiter makes the create operations honour Service Quotas; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.quotaGate.Set(f)
	}
}

// StateMachineCount returns the state machines held in region.
func (h *Handler) StateMachineCount(region string) int {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b.indexLen(b.nameIndex, region)
	}

	return 0
}

// ActivityCount returns the activities held in region.
func (h *Handler) ActivityCount(region string) int {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b.indexLen(b.activityNameIndex, region)
	}

	return 0
}

// MaxVersionsPerStateMachine returns the most versions any state machine in region has.
func (h *Handler) MaxVersionsPerStateMachine(region string) int {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b.maxPerStateMachine(region, func(arn string) int { return len(b.versionsByStateMachine.Get(arn)) })
	}

	return 0
}

// MaxAliasesPerStateMachine returns the most aliases any state machine in region has.
func (h *Handler) MaxAliasesPerStateMachine(region string) int {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b.maxPerStateMachine(region, func(arn string) int { return len(b.smAliases[arn]) })
	}

	return 0
}

func (b *InMemoryBackend) indexLen(m map[string]map[string]string, region string) int {
	if region == "" {
		region = b.region
	}

	b.mu.RLock("QuotaCount")
	defer b.mu.RUnlock()

	return len(m[region])
}

func (b *InMemoryBackend) maxPerStateMachine(region string, count func(arn string) int) int {
	if region == "" {
		region = b.region
	}

	b.mu.RLock("QuotaCount")
	defer b.mu.RUnlock()

	most := 0

	for _, arn := range b.nameIndex[region] {
		most = max(most, count(arn))
	}

	return most
}

func (b *InMemoryBackend) quotaLimit(region, code string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, region, quotaServiceCode, code)
}

func quotaReached(limit quotalimit.Limit, current int, sentinel error, kind string) error {
	if limit.Reached(current) {
		return fmt.Errorf("%w: the maximum number of %s (%.0f) has been reached", sentinel, kind, limit.Value)
	}

	return nil
}

package cloudformation

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode = "cloudformation"
	quotaStacks      = "L-0485CB21"
)

// SetQuotaLimiter makes CreateStack honour the Service Quotas stack count in every Region; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.quotaGate.Set(f)
	}
}

// StackCount returns the stacks that exist (not DELETE_COMPLETE) in region.
func (h *Handler) StackCount(region string) int {
	for _, sb := range h.RegionBackends() {
		if b, ok := sb.(*InMemoryBackend); ok && b.region == region {
			return b.stackCount()
		}
	}

	return 0
}

func (b *InMemoryBackend) stackCount() int {
	b.mu.RLock("stackCount")
	defer b.mu.RUnlock()

	return b.liveStackCountLocked()
}

func (b *InMemoryBackend) liveStackCountLocked() int {
	n := 0

	for _, s := range b.stacks.All() {
		if s.StackStatus != statusDeleteComplete {
			n++
		}
	}

	return n
}

func (b *InMemoryBackend) quotaLimit() quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, b.region, quotaServiceCode, quotaStacks)
}

// checkStackQuotaLocked rejects a new stack name once the stack count is at the limit; an existing stack is left
// to the already-exists path.
func (b *InMemoryBackend) checkStackQuotaLocked(name string, quota quotalimit.Limit) error {
	if existing, ok := b.stacks.Get(name); ok && existing.StackStatus != statusDeleteComplete {
		return nil
	}

	if quota.Reached(b.liveStackCountLocked()) {
		return fmt.Errorf("%w: the account already has the maximum number of stacks (%.0f)",
			ErrStackLimitExceeded, quota.Value)
	}

	return nil
}

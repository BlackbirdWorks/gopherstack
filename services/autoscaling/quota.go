package autoscaling

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode = "autoscaling"
	quotaGroups      = "L-CDE20ADC"
	quotaLaunchCfgs  = "L-6B80B8FA"
)

// ErrLimitExceeded is the LimitExceeded code CreateAutoScalingGroup and CreateLaunchConfiguration declare
// (autoscaling@v1.78.3 deserializers.go:1032, :1126).
var ErrLimitExceeded = errors.New("LimitExceeded")

// SetQuotaLimiter makes the create operations honour Service Quotas in every Region; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.quotaGate.Set(f)
	}
}

// GroupCount returns the Auto Scaling groups held in region.
func (h *Handler) GroupCount(region string) int {
	if b := h.existingBackend(region); b != nil {
		b.mu.RLock("GroupCount")
		defer b.mu.RUnlock()

		return len(b.groups.All())
	}

	return 0
}

// LaunchConfigurationCount returns the launch configurations held in region.
func (h *Handler) LaunchConfigurationCount(region string) int {
	if b := h.existingBackend(region); b != nil {
		b.mu.RLock("LaunchConfigurationCount")
		defer b.mu.RUnlock()

		return len(b.launchConfigurations.All())
	}

	return 0
}

func (h *Handler) existingBackend(region string) *InMemoryBackend {
	if home, ok := h.Backend.(*InMemoryBackend); ok && (region == "" || region == home.region) {
		return home
	}

	for _, p := range h.peers.All() {
		if b, ok := p.Backend.(*InMemoryBackend); ok && b.region == region {
			return b
		}
	}

	return nil
}

func (b *InMemoryBackend) quotaLimit(code string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, b.region, quotaServiceCode, code)
}

func quotaReached(limit quotalimit.Limit, current int, kind string) error {
	if limit.Reached(current) {
		return fmt.Errorf("%w: the maximum number of %s (%.0f) has been reached", ErrLimitExceeded, kind, limit.Value)
	}

	return nil
}

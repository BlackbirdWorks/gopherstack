package elbv2

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode = "elasticloadbalancing"
	quotaALBs        = "L-53DA6B97"
	quotaNLBs        = "L-69A177A2"
)

// SetQuotaLimiter makes CreateLoadBalancer honour the Service Quotas ALB and NLB limits in every Region;
// nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.quotaGate.Set(f)
	}
}

// LoadBalancerCount returns the load balancers of lbType (application or network) held in region.
func (h *Handler) LoadBalancerCount(region, lbType string) int {
	for _, sb := range h.RegionBackends() {
		if b, ok := sb.(*InMemoryBackend); ok && b.region == region {
			return b.countLoadBalancers(lbType)
		}
	}

	return 0
}

func (b *InMemoryBackend) countLoadBalancers(lbType string) int {
	b.mu.RLock("countLoadBalancers")
	defer b.mu.RUnlock()

	return b.countLoadBalancersLocked(lbType)
}

func (b *InMemoryBackend) countLoadBalancersLocked(lbType string) int {
	n := 0

	for _, lb := range b.loadBalancers.All() {
		if lb.Type == lbType {
			n++
		}
	}

	return n
}

func (b *InMemoryBackend) quotaLimit(lbType string) quotalimit.Limit {
	code := quotaALBs

	switch lbType {
	case "", lbTypeApplication:
	case lbTypeNetwork:
		code = quotaNLBs
	default:
		return quotalimit.Limit{}
	}

	return b.quotaGate.Lookup(b.accountID, b.region, quotaServiceCode, code)
}

// reuseOrCheckQuotaLocked returns the identical existing load balancer, or the duplicate-name or quota error.
func (b *InMemoryBackend) reuseOrCheckQuotaLocked(
	lbType string, input CreateLoadBalancerInput, quota quotalimit.Limit,
) (*LoadBalancer, error) {
	if existing, err := b.existingLBLocked(lbType, input); existing != nil || err != nil {
		return existing, err
	}

	if !quota.Reached(b.countLoadBalancersLocked(lbType)) {
		return nil, nil //nolint:nilnil // no existing load balancer and room under the quota
	}

	return nil, fmt.Errorf("%w: the account already has the maximum number of %s load balancers (%.0f)",
		ErrTooManyLoadBalancers, lbType, quota.Value)
}

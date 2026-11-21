package elb

import (
	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode = "elasticloadbalancing"
	quotaClassicLBs  = "L-E9E9831D"

	defaultClassicLBLimit = 20
)

// SetQuotaLimiter makes CreateLoadBalancer honour the Service Quotas Classic Load Balancers limit;
// nil restores the built-in 20.
func (b *InMemoryBackend) SetQuotaLimiter(f quotalimit.Func) { b.quotaGate.Set(f) }

// RegionLoadBalancerCount returns the Classic Load Balancers held in region.
func (b *InMemoryBackend) RegionLoadBalancerCount(region string) int {
	b.mu.RLock("RegionLoadBalancerCount")
	defer b.mu.RUnlock()

	return len(b.lbsByRegion.Get(region))
}

func (b *InMemoryBackend) classicLBLimit(region string) int {
	if l := b.quotaGate.Lookup(b.accountID, region, quotaServiceCode, quotaClassicLBs); l.Set {
		return int(l.Value)
	}

	return defaultClassicLBLimit
}

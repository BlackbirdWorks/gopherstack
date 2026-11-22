package route53

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode     = "route53"
	quotaHostedZones     = "L-4EA4796A"
	quotaHealthChecks    = "L-ACB674F3"
	quotaDelegationSets  = "L-A72C7724"
	quotaTrafficPolicies = "L-FC688E7C"
	quotaCidrCollections = "L-AD61772F"
	quotaRecordsPerZone  = "L-E209CC9F"
)

// The limit errors CreateHostedZone, CreateHealthCheck, CreateReusableDelegationSet, CreateTrafficPolicy and
// CreateCidrCollection declare (route53@v1.70.5 deserializers.go:1212, :1039, :1803, :1988, :863).
var (
	ErrTooManyHostedZones     = errors.New("TooManyHostedZones")
	ErrTooManyHealthChecks    = errors.New("TooManyHealthChecks")
	ErrTooManyTrafficPolicies = errors.New("TooManyTrafficPolicies")
	ErrLimitsExceeded         = errors.New("LimitsExceeded")
)

// SetQuotaLimiter makes the create operations honour Service Quotas; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.quotaGate.Set(f)
	}
}

// QuotaCounter is the read-only view of the backend that Service Quotas utilization reads.
type QuotaCounter interface {
	HostedZoneCount() int
	HealthCheckCount() int
	DelegationSetCount() int
	TrafficPolicyCount() int
	CidrCollectionCount() int
	MaxRecordsPerHostedZone() int
}

// QuotaCounts returns the live counts behind the Service Quotas usage of this service.
func (h *Handler) QuotaCounts() QuotaCounter {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b
	}

	return nil
}

// HostedZoneCount returns the hosted zones of the account.
func (b *InMemoryBackend) HostedZoneCount() int {
	b.mu.RLock("HostedZoneCount")
	defer b.mu.RUnlock()

	return b.zones.Len()
}

// HealthCheckCount returns the health checks of the account.
func (b *InMemoryBackend) HealthCheckCount() int {
	b.mu.RLock("HealthCheckCount")
	defer b.mu.RUnlock()

	return b.healthChecks.Len()
}

// DelegationSetCount returns the reusable delegation sets of the account.
func (b *InMemoryBackend) DelegationSetCount() int {
	b.mu.RLock("DelegationSetCount")
	defer b.mu.RUnlock()

	return b.reusableDelegationSets.Len()
}

// TrafficPolicyCount returns the traffic policies of the account.
func (b *InMemoryBackend) TrafficPolicyCount() int {
	b.mu.RLock("TrafficPolicyCount")
	defer b.mu.RUnlock()

	return len(b.trafficPolicies)
}

// CidrCollectionCount returns the CIDR collections of the account.
func (b *InMemoryBackend) CidrCollectionCount() int {
	b.mu.RLock("CidrCollectionCount")
	defer b.mu.RUnlock()

	return b.cidrCollections.Len()
}

// MaxRecordsPerHostedZone returns the most record sets any single hosted zone holds.
func (b *InMemoryBackend) MaxRecordsPerHostedZone() int {
	b.mu.RLock("MaxRecordsPerHostedZone")
	defer b.mu.RUnlock()

	most := 0
	for _, zd := range b.zones.All() {
		most = max(most, len(zd.records))
	}

	return most
}

func (b *InMemoryBackend) quotaLimit(code string) quotalimit.Limit {
	return b.quotaGate.Lookup("", "", quotaServiceCode, code)
}

func quotaReached(limit quotalimit.Limit, current int, sentinel error, kind string) error {
	if limit.Reached(current) {
		return fmt.Errorf("%w: the maximum number of %s (%.0f) has been reached", sentinel, kind, limit.Value)
	}

	return nil
}

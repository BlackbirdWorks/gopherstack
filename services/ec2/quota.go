package ec2

import (
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceEC2 = "ec2"
	quotaServiceVPC = "vpc"
	quotaElasticIPs = "L-0263D0A3"
	quotaVPCs       = "L-F678F1CE"
)

// ErrVpcLimitExceeded and ErrAddressLimitExceeded are returned when a Service Quotas limit is reached.
var (
	ErrVpcLimitExceeded     = errors.New("VpcLimitExceeded")
	ErrAddressLimitExceeded = errors.New("AddressLimitExceeded")
)

// SetQuotaLimiter makes CreateVpc and AllocateAddress honour Service Quotas in every Region; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	for _, b := range h.memBackends() {
		b.SetQuotaLimiter(f)
	}
}

// SetQuotaLimiter installs the limit lookup shared with sibling Region backends.
func (b *InMemoryBackend) SetQuotaLimiter(f quotalimit.Func) {
	if b.quotaGate != nil {
		b.quotaGate.Set(f)
	}
}

// VpcCount returns the VPCs in the backend's Region.
func (b *InMemoryBackend) VpcCount() int {
	b.mu.RLock("VpcCount")
	defer b.mu.RUnlock()

	return len(b.vpcs.All())
}

// AddressCount returns the allocated Elastic IPs in the backend's Region.
func (b *InMemoryBackend) AddressCount() int {
	b.mu.RLock("AddressCount")
	defer b.mu.RUnlock()

	return len(b.addresses.All())
}

func (b *InMemoryBackend) quotaLimit(service, code string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.AccountID, b.Region, service, code)
}

func quotaExceeded(sentinel error, kind string, limit quotalimit.Limit) error {
	return fmt.Errorf("%w: the maximum number of %s has been reached (%.0f)", sentinel, kind, limit.Value)
}

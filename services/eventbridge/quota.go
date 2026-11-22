package eventbridge

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	quotaServiceCode  = "events"
	quotaRules        = "L-244521F2"
	quotaEventBuses   = "L-658A4FD9"
	quotaConnections  = "L-595D6D42"
	quotaEndpoints    = "L-EAC9A2AC"
	quotaAPIDestCount = "L-FB1C3A6D"
)

// QuotaCounter is the read-only view of the backend that Service Quotas utilization reads.
type QuotaCounter interface {
	CustomBusCount(region string) int
	MaxRulesPerBus(region string) int
	ConnectionsInRegion(region string) int
	EndpointsInRegion(region string) int
	APIDestinationsInRegion(region string) int
}

// SetQuotaLimiter makes the create operations honour Service Quotas; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.quotaGate.Set(f)
	}
}

// QuotaCounts returns the live counts behind the Service Quotas usage of this service.
func (h *Handler) QuotaCounts() QuotaCounter {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b
	}

	return nil
}

func (b *InMemoryBackend) regionOr(region string) string {
	if region == "" {
		return b.region
	}

	return region
}

func tableLenLocked[T any](b *InMemoryBackend, m map[string]*store.Table[T], region string) int {
	b.tableMu.Lock()
	t := m[region]
	b.tableMu.Unlock()

	if t == nil {
		return 0
	}

	return t.Len()
}

func (b *InMemoryBackend) customBusCountLocked(region string) int {
	b.tableMu.Lock()
	t := b.buses[region]
	b.tableMu.Unlock()

	if t == nil {
		return 0
	}

	n := 0

	for _, bus := range t.All() {
		if bus.Name != defaultEventBusName {
			n++
		}
	}

	return n
}

// CustomBusCount returns the custom event buses in region.
func (b *InMemoryBackend) CustomBusCount(region string) int {
	b.mu.RLock("CustomBusCount")
	defer b.mu.RUnlock()

	return b.customBusCountLocked(b.regionOr(region))
}

// MaxRulesPerBus returns the most rules any single event bus in region holds.
func (b *InMemoryBackend) MaxRulesPerBus(region string) int {
	b.mu.RLock("MaxRulesPerBus")
	defer b.mu.RUnlock()

	b.tableMu.Lock()
	tables := make([]*store.Table[Rule], 0, len(b.rules[b.regionOr(region)]))

	for _, t := range b.rules[b.regionOr(region)] {
		tables = append(tables, t)
	}
	b.tableMu.Unlock()

	most := 0
	for _, t := range tables {
		most = max(most, t.Len())
	}

	return most
}

// ConnectionsInRegion returns the connections in region.
func (b *InMemoryBackend) ConnectionsInRegion(region string) int {
	b.mu.RLock("ConnectionsInRegion")
	defer b.mu.RUnlock()

	return tableLenLocked(b, b.connections, b.regionOr(region))
}

// EndpointsInRegion returns the global endpoints in region.
func (b *InMemoryBackend) EndpointsInRegion(region string) int {
	b.mu.RLock("EndpointsInRegion")
	defer b.mu.RUnlock()

	return tableLenLocked(b, b.endpoints, b.regionOr(region))
}

// APIDestinationsInRegion returns the API destinations in region.
func (b *InMemoryBackend) APIDestinationsInRegion(region string) int {
	b.mu.RLock("APIDestinationsInRegion")
	defer b.mu.RUnlock()

	return tableLenLocked(b, b.apiDestinations, b.regionOr(region))
}

func (b *InMemoryBackend) quotaLimit(region, code string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, region, quotaServiceCode, code)
}

func quotaReached(limit quotalimit.Limit, current int, kind string) error {
	if limit.Reached(current) {
		return fmt.Errorf(
			"%w: the maximum number of %s (%.0f) has been reached",
			ErrResourceLimitExceeded,
			kind,
			limit.Value,
		)
	}

	return nil
}

func ruleLimit(limit quotalimit.Limit) int {
	if limit.Set {
		return int(limit.Value)
	}

	return maxRulesPerBus
}

func (b *InMemoryBackend) checkBusQuotaLocked(region string, limit quotalimit.Limit) error {
	if limit.Set {
		return quotaReached(limit, b.customBusCountLocked(region), "event buses")
	}

	customBusCount := 0

	for _, regionBuses := range b.buses {
		for _, bus := range regionBuses.All() {
			if bus.Name != defaultEventBusName {
				customBusCount++
			}
		}
	}

	if customBusCount >= maxEventBusesPerAccount {
		return fmt.Errorf(
			"%w: account has reached the maximum of %d custom event buses",
			ErrResourceLimitExceeded,
			maxEventBusesPerAccount,
		)
	}

	return nil
}

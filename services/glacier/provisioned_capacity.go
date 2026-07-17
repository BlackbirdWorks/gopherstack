package glacier

import (
	"sort"
	"time"
)

const (
	// capacityIDLength is the length of the generated capacity unit ID.
	capacityIDLength = 32
	// maxProvisionedCapacityUnits is the per-account cap on provisioned capacity units.
	maxProvisionedCapacityUnits = 2
	// provisionedCapacityMonths is the number of months a purchased capacity unit is active.
	provisionedCapacityMonths = 1
)

// reapExpiredCapacity removes expired provisioned capacity units for an account.
// Caller must hold b.mu.
func (b *InMemoryBackend) reapExpiredCapacity(accountID string) {
	now := time.Now().UTC()
	caps := b.provisionedCapacity[accountID]
	active := caps[:0]

	for _, c := range caps {
		exp, err := time.Parse("2006-01-02T15:04:05.000Z", c.ExpirationDate)
		if err != nil || now.Before(exp) {
			active = append(active, c)
		}
	}

	b.provisionedCapacity[accountID] = active
}

// ListProvisionedCapacity returns all non-expired provisioned capacity units for an account.
func (b *InMemoryBackend) ListProvisionedCapacity(accountID string) []*ProvisionedCapacity {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.reapExpiredCapacity(accountID)

	caps := b.provisionedCapacity[accountID]
	result := make([]*ProvisionedCapacity, 0, len(caps))

	for _, c := range caps {
		cp := *c
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].CapacityID < result[j].CapacityID })

	return result
}

// PurchaseProvisionedCapacity adds a provisioned capacity unit for an account.
// Returns ErrProvisionedCapacityLimit if the account already has 2 active units.
func (b *InMemoryBackend) PurchaseProvisionedCapacity(accountID string) (*ProvisionedCapacity, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.reapExpiredCapacity(accountID)

	if len(b.provisionedCapacity[accountID]) >= maxProvisionedCapacityUnits {
		return nil, ErrProvisionedCapacityLimit
	}

	now := time.Now().UTC()
	unit := &ProvisionedCapacity{
		CapacityID:     generateID(capacityIDLength),
		StartDate:      formatDate(now),
		ExpirationDate: formatDate(now.AddDate(0, provisionedCapacityMonths, 0)),
	}

	b.provisionedCapacity[accountID] = append(b.provisionedCapacity[accountID], unit)

	return unit, nil
}

package opensearch

import (
	"fmt"
	"time"
)

// StartDomainMaintenance starts a maintenance action on a domain.
func (b *InMemoryBackend) StartDomainMaintenance(
	domainName, action, nodeID string,
) (*DomainMaintenance, error) {
	b.mu.Lock("StartDomainMaintenance")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	b.maintenanceCounter++
	id := fmt.Sprintf("m-%d", b.maintenanceCounter)
	now := float64(time.Now().Unix())

	m := &DomainMaintenance{
		MaintenanceID: id,
		DomainName:    domainName,
		Action:        action,
		NodeID:        nodeID,
		Status:        softwareUpdateCompleted,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	b.domainMaintenances[domainName] = append(b.domainMaintenances[domainName], m)
	// Trim to the cap, keeping the most recent entries.
	if len(b.domainMaintenances[domainName]) > maxMaintenancesPerDomain {
		records := b.domainMaintenances[domainName]
		b.domainMaintenances[domainName] = records[len(records)-maxMaintenancesPerDomain:]
	}

	cp := *m

	return &cp, nil
}

// GetDomainMaintenanceStatus returns a specific maintenance record.
func (b *InMemoryBackend) GetDomainMaintenanceStatus(
	domainName, maintenanceID string,
) (*DomainMaintenance, error) {
	b.mu.RLock("GetDomainMaintenanceStatus")
	defer b.mu.RUnlock()

	for _, m := range b.domainMaintenances[domainName] {
		if m.MaintenanceID == maintenanceID {
			cp := *m

			return &cp, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: maintenance %s not found on domain %s",
		ErrConnectionNotFound,
		maintenanceID,
		domainName,
	)
}

// ListDomainMaintenances returns all maintenance records for a domain.
func (b *InMemoryBackend) ListDomainMaintenances(domainName string) ([]*DomainMaintenance, error) {
	b.mu.RLock("ListDomainMaintenances")
	defer b.mu.RUnlock()

	src := b.domainMaintenances[domainName]
	out := make([]*DomainMaintenance, len(src))

	for i, m := range src {
		cp := *m
		out[i] = &cp
	}

	return out, nil
}

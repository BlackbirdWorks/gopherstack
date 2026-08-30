package opensearch

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
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

// ListDomainMaintenances returns maintenance records for a domain, filtered
// by action/status and paginated per nextToken/maxResults
// (api_op_ListDomainMaintenances.go: Action/Status/MaxResults/NextToken are
// all query-bound, per serializers.go's HttpBindings function for this op).
// action/status empty means "no filter" -- both are optional on the wire.
func (b *InMemoryBackend) ListDomainMaintenances(
	domainName, action, status, nextToken string, maxResults int,
) (page.Page[*DomainMaintenance], error) {
	b.mu.RLock("ListDomainMaintenances")
	defer b.mu.RUnlock()

	src := b.domainMaintenances[domainName]
	all := make([]*DomainMaintenance, 0, len(src))

	for _, m := range src {
		if action != "" && m.Action != action {
			continue
		}

		if status != "" && m.Status != status {
			continue
		}

		cp := *m
		all = append(all, &cp)
	}

	limit := maxResults
	if limit <= 0 {
		limit = len(all)
	}

	return page.New(all, nextToken, limit, limit), nil
}

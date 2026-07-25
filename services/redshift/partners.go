package redshift

import "fmt"

func partnerKey(clusterID, databaseName, partnerName string) string {
	return clusterID + "/" + databaseName + "/" + partnerName
}

// AddPartner adds a partner integration to the specified cluster database.
func (b *InMemoryBackend) AddPartner(accountID, clusterID, databaseName, partnerName string) (*Partner, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}
	if databaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", ErrInvalidParameter)
	}
	if partnerName == "" {
		return nil, fmt.Errorf("%w: PartnerName is required", ErrInvalidParameter)
	}

	b.mu.Lock("AddPartner")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	partner := &Partner{
		AccountID:         accountID,
		ClusterIdentifier: clusterID,
		DatabaseName:      databaseName,
		PartnerName:       partnerName,
		Status:            partnerStatusActive,
		StatusMessage:     "",
	}
	b.partners.Put(partner)

	cp := *partner

	return &cp, nil
}

// DeletePartner removes a partner integration from the specified cluster database.
func (b *InMemoryBackend) DeletePartner(_, clusterID, databaseName, partnerName string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeletePartner")
	defer b.mu.Unlock()

	key := partnerKey(clusterID, databaseName, partnerName)
	if _, exists := b.partners.Get(key); !exists {
		return fmt.Errorf("%w: partner %s not found", ErrPartnerNotFound, partnerName)
	}

	b.partners.Delete(key)

	return nil
}

// DescribePartners returns partner integrations filtered by cluster, database, or partner name.
func (b *InMemoryBackend) DescribePartners(_, clusterID, databaseName, partnerName string) ([]Partner, error) {
	b.mu.RLock("DescribePartners")
	defer b.mu.RUnlock()

	var result []Partner

	for _, p := range b.partners.All() {
		if clusterID != "" && p.ClusterIdentifier != clusterID {
			continue
		}

		if databaseName != "" && p.DatabaseName != databaseName {
			continue
		}

		if partnerName != "" && p.PartnerName != partnerName {
			continue
		}

		cp := *p
		result = append(result, cp)
	}

	return result, nil
}

// UpdatePartnerStatus updates the status and status message of a partner integration.
func (b *InMemoryBackend) UpdatePartnerStatus(
	_, clusterID, databaseName, partnerName, status, statusMessage string,
) (*Partner, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdatePartnerStatus")
	defer b.mu.Unlock()

	key := partnerKey(clusterID, databaseName, partnerName)

	p, exists := b.partners.Get(key)
	if !exists {
		return nil, fmt.Errorf("%w: partner %s not found", ErrPartnerNotFound, partnerName)
	}

	if status != "" {
		p.Status = status
	}

	if statusMessage != "" {
		p.StatusMessage = statusMessage
	}

	cp := *p

	return &cp, nil
}

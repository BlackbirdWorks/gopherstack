package redshift

import (
	"fmt"
	"time"
)

// customDomainCertExpiryDays is a fabricated-but-consistent validity window for a
// custom domain association's certificate, mirroring Redshift Serverless's own
// slCertExpiryDays (serverless.go) -- this backend does not do real ACM
// certificate issuance for classic Redshift either.
const customDomainCertExpiryDays = 365

// newCustomDomainCertExpiry returns a fresh customDomainCertExpiryDays-out
// expiry timestamp, formatted the way this backend's other RFC3339 wire
// timestamps are.
func newCustomDomainCertExpiry() string {
	return time.Now().Add(customDomainCertExpiryDays * 24 * time.Hour).UTC().Format(time.RFC3339)
}

// CreateCustomDomainAssociation creates a custom domain name association for a cluster.
func (b *InMemoryBackend) CreateCustomDomainAssociation(
	clusterID, customDomainName, customDomainCertificateArn string,
) (*CustomDomainAssociation, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if customDomainName == "" {
		return nil, fmt.Errorf("%w: CustomDomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCustomDomainAssociation")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	key := clusterID + ":" + customDomainName
	if _, exists := b.customDomains.Get(key); exists {
		return nil, fmt.Errorf("%w: custom domain %s already associated with cluster %s",
			ErrCustomDomainAlreadyExists, customDomainName, clusterID)
	}

	assoc := &CustomDomainAssociation{
		ClusterIdentifier:          clusterID,
		CustomDomainName:           customDomainName,
		CustomDomainCertificateArn: customDomainCertificateArn,
		CustomDomainCertExpiryTime: newCustomDomainCertExpiry(),
	}
	b.customDomains.Put(assoc)

	cp := *assoc

	return &cp, nil
}

// DeleteCustomDomainAssociation removes the custom domain association for a cluster.
func (b *InMemoryBackend) DeleteCustomDomainAssociation(clusterID, customDomainName string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if customDomainName == "" {
		return fmt.Errorf("%w: CustomDomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteCustomDomainAssociation")
	defer b.mu.Unlock()

	key := clusterID + ":" + customDomainName
	if _, exists := b.customDomains.Get(key); !exists {
		return fmt.Errorf("%w: custom domain %s not associated with cluster %s",
			ErrCustomDomainNotFound, customDomainName, clusterID)
	}

	b.customDomains.Delete(key)

	return nil
}

// DescribeCustomDomainAssociations returns custom domain associations, optionally filtered.
func (b *InMemoryBackend) DescribeCustomDomainAssociations(
	clusterID, customDomainName string,
) ([]CustomDomainAssociation, error) {
	b.mu.RLock("DescribeCustomDomainAssociations")
	defer b.mu.RUnlock()

	if clusterID != "" && customDomainName != "" {
		key := clusterID + ":" + customDomainName
		a, exists := b.customDomains.Get(key)
		if !exists {
			return nil, fmt.Errorf("%w: custom domain %s not associated with cluster %s",
				ErrCustomDomainNotFound, customDomainName, clusterID)
		}

		cp := *a

		return []CustomDomainAssociation{cp}, nil
	}

	result := make([]CustomDomainAssociation, 0, b.customDomains.Len())

	for _, a := range b.customDomains.All() {
		if clusterID != "" && a.ClusterIdentifier != clusterID {
			continue
		}

		result = append(result, *a)
	}

	return result, nil
}

// ModifyCustomDomainAssociation updates the certificate ARN for an existing custom domain.
func (b *InMemoryBackend) ModifyCustomDomainAssociation(
	clusterID, customDomainName, customDomainCertificateArn string,
) (*CustomDomainAssociation, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	if customDomainName == "" {
		return nil, fmt.Errorf("%w: CustomDomainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyCustomDomainAssociation")
	defer b.mu.Unlock()

	key := clusterID + ":" + customDomainName
	a, exists := b.customDomains.Get(key)
	if !exists {
		return nil, fmt.Errorf("%w: custom domain %s not associated with cluster %s",
			ErrCustomDomainNotFound, customDomainName, clusterID)
	}

	a.CustomDomainCertificateArn = customDomainCertificateArn
	a.CustomDomainCertExpiryTime = newCustomDomainCertExpiry()
	cp := *a

	return &cp, nil
}

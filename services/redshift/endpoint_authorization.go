package redshift

import (
	"fmt"
	"time"
)

func endpointAuthKey(clusterID, grantee string) string {
	return clusterID + "/" + grantee
}

// AuthorizeEndpointAccess authorizes an account to create a VPC endpoint to the cluster.
func (b *InMemoryBackend) AuthorizeEndpointAccess(
	clusterID, grantee string,
	vpcIDs []string,
) (*EndpointAuthorization, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}
	if grantee == "" {
		return nil, fmt.Errorf("%w: Account is required", ErrInvalidParameter)
	}

	b.mu.Lock("AuthorizeEndpointAccess")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	key := endpointAuthKey(clusterID, grantee)
	if _, exists := b.endpointAuths.Get(key); exists {
		return nil, fmt.Errorf("%w: endpoint authorization already exists for cluster %s and account %s",
			ErrEndpointAuthAlreadyExists, clusterID, grantee)
	}

	allowedVPCs := make([]string, len(vpcIDs))
	copy(allowedVPCs, vpcIDs)

	auth := &EndpointAuthorization{
		Grantor:           b.accountID,
		Grantee:           grantee,
		ClusterIdentifier: clusterID,
		AuthorizeTime:     time.Now(),
		ClusterStatus:     clusterStatusAvailable,
		Status:            endpointAuthStatusAuthorized,
		AllowedAllVPCs:    len(vpcIDs) == 0,
		AllowedVPCs:       allowedVPCs,
		EndpointCount:     0,
	}
	b.endpointAuths.Put(auth)

	return cloneEndpointAuth(auth), nil
}

// DescribeEndpointAuthorization returns endpoint authorizations filtered by cluster and account.
func (b *InMemoryBackend) DescribeEndpointAuthorization(
	clusterID, account string,
	grantee bool,
) ([]EndpointAuthorization, error) {
	b.mu.RLock("DescribeEndpointAuthorization")
	defer b.mu.RUnlock()

	var result []EndpointAuthorization

	for _, ea := range b.endpointAuths.All() {
		if clusterID != "" && ea.ClusterIdentifier != clusterID {
			continue
		}

		if account != "" {
			if grantee && ea.Grantee != account {
				continue
			}

			if !grantee && ea.Grantor != account {
				continue
			}
		}

		cp := *cloneEndpointAuth(ea)
		result = append(result, cp)
	}

	return result, nil
}

// RevokeEndpointAccess revokes endpoint authorization for a cluster and account.
func (b *InMemoryBackend) RevokeEndpointAccess(
	clusterID, account string,
	_ []string,
	force bool,
) (*EndpointAuthorization, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("RevokeEndpointAccess")
	defer b.mu.Unlock()

	key := endpointAuthKey(clusterID, account)

	ea, exists := b.endpointAuths.Get(key)
	if !exists {
		return nil, fmt.Errorf(
			"%w: endpoint authorization for cluster %s account %s not found",
			ErrEndpointAuthNotFound,
			clusterID,
			account,
		)
	}

	ea.Status = "Revoking"

	cp := cloneEndpointAuth(ea)

	if force {
		b.endpointAuths.Delete(key)
	}

	return cp, nil
}

// cloneEndpointAuth returns a deep copy of an EndpointAuthorization.
func cloneEndpointAuth(ea *EndpointAuthorization) *EndpointAuthorization {
	cp := *ea
	cp.AllowedVPCs = make([]string, len(ea.AllowedVPCs))
	copy(cp.AllowedVPCs, ea.AllowedVPCs)

	return &cp
}

package redshift

import (
	"fmt"
	"time"
)

const (
	// endpointStatusActive is the status for an active Redshift endpoint access.
	endpointStatusActive = "active"
	// redshiftDefaultPort is the default Redshift cluster port.
	redshiftDefaultPort = 5439
)

// CreateEndpointAccess creates a new Redshift-managed VPC endpoint. Real
// CreateEndpointAccessInput has no VpcId field -- the endpoint's VPC is derived from
// SubnetGroupName (confirmed against aws-sdk-go-v2/service/redshift@v1.62.3's
// CreateEndpointAccessInput). When subnetGroupName names a known
// ClusterSubnetGroup, VpcID is populated from it; otherwise it is left empty
// (real AWS would fall back to the cluster's own subnet group, which this backend
// does not track independently of ClusterSubnetGroup).
func (b *InMemoryBackend) CreateEndpointAccess(
	clusterID, endpointName, subnetGroupName, resourceOwner string, vpcSecurityGroupIDs []string,
) (*EndpointAccess, error) {
	if endpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", ErrInvalidParameter)
	}

	if clusterID == "" {
		return nil, fmt.Errorf("%w: ClusterIdentifier is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateEndpointAccess")
	defer b.mu.Unlock()

	if _, exists := b.clusters.Get(clusterID); !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}

	if _, exists := b.endpointAccesses.Get(endpointName); exists {
		return nil, fmt.Errorf("%w: endpoint %s already exists", ErrEndpointAccessAlreadyExists, endpointName)
	}

	var vpcID string
	if subnetGroupName != "" {
		if sg, exists := b.subnetGroups.Get(subnetGroupName); exists {
			vpcID = sg.VpcID
		}
	}

	ep := &EndpointAccess{
		ClusterIdentifier:   clusterID,
		EndpointName:        endpointName,
		EndpointStatus:      endpointStatusActive,
		EndpointCreateTime:  time.Now().UTC().Format(time.RFC3339),
		Port:                redshiftDefaultPort,
		VpcID:               vpcID,
		SubnetGroupName:     subnetGroupName,
		ResourceOwner:       resourceOwner,
		VpcSecurityGroupIDs: append([]string(nil), vpcSecurityGroupIDs...),
	}
	b.endpointAccesses.Put(ep)

	cp := *ep

	return &cp, nil
}

// DeleteEndpointAccess deletes the named endpoint access entry.
func (b *InMemoryBackend) DeleteEndpointAccess(endpointName string) (*EndpointAccess, error) {
	if endpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteEndpointAccess")
	defer b.mu.Unlock()

	ep, exists := b.endpointAccesses.Get(endpointName)
	if !exists {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrEndpointAccessNotFound, endpointName)
	}

	b.endpointAccesses.Delete(endpointName)

	cp := *ep
	cp.EndpointStatus = "deleting"

	return &cp, nil
}

// DescribeEndpointAccess returns endpoint accesses, optionally filtered by cluster and endpoint name.
func (b *InMemoryBackend) DescribeEndpointAccess(
	clusterID, endpointName string,
) ([]EndpointAccess, error) {
	b.mu.RLock("DescribeEndpointAccess")
	defer b.mu.RUnlock()

	if endpointName != "" {
		ep, exists := b.endpointAccesses.Get(endpointName)
		if !exists {
			return nil, fmt.Errorf("%w: endpoint %s not found", ErrEndpointAccessNotFound, endpointName)
		}

		cp := *ep

		return []EndpointAccess{cp}, nil
	}

	result := make([]EndpointAccess, 0, b.endpointAccesses.Len())

	for _, ep := range b.endpointAccesses.All() {
		if clusterID != "" && ep.ClusterIdentifier != clusterID {
			continue
		}

		result = append(result, *ep)
	}

	return result, nil
}

// ModifyEndpointAccess updates the VPC security groups for an endpoint. Real
// ModifyEndpointAccessInput only supports changing VpcSecurityGroupIds (confirmed
// against aws-sdk-go-v2/service/redshift@v1.62.3) -- there is no VpcId parameter.
func (b *InMemoryBackend) ModifyEndpointAccess(
	endpointName string, vpcSecurityGroupIDs []string,
) (*EndpointAccess, error) {
	if endpointName == "" {
		return nil, fmt.Errorf("%w: EndpointName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyEndpointAccess")
	defer b.mu.Unlock()

	ep, exists := b.endpointAccesses.Get(endpointName)
	if !exists {
		return nil, fmt.Errorf("%w: endpoint %s not found", ErrEndpointAccessNotFound, endpointName)
	}

	if vpcSecurityGroupIDs != nil {
		ep.VpcSecurityGroupIDs = append([]string(nil), vpcSecurityGroupIDs...)
	}

	cp := *ep

	return &cp, nil
}

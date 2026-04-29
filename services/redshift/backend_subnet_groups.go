package redshift

import (
	"fmt"
)

// Subnet represents a single subnet within a cluster subnet group.
type Subnet struct {
	SubnetIdentifier string `json:"subnetIdentifier"`
	SubnetStatus     string `json:"subnetStatus"`
}

// ClusterSubnetGroup represents a Redshift cluster subnet group.
type ClusterSubnetGroup struct {
	ClusterSubnetGroupName string   `json:"clusterSubnetGroupName"`
	Description            string   `json:"description"`
	VpcID                  string   `json:"vpcId"`
	SubnetGroupStatus      string   `json:"subnetGroupStatus"`
	Subnets                []Subnet `json:"subnets"`
}

// CreateClusterSubnetGroup creates a new cluster subnet group.
func (b *InMemoryBackend) CreateClusterSubnetGroup(
	name, description, vpcID string,
	subnetIDs []string,
) (*ClusterSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ClusterSubnetGroupName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateClusterSubnetGroup")
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; exists {
		return nil, fmt.Errorf("%w: subnet group %s already exists", ErrSubnetGroupAlreadyExists, name)
	}

	subnets := make([]Subnet, 0, len(subnetIDs))
	for _, id := range subnetIDs {
		subnets = append(subnets, Subnet{SubnetIdentifier: id, SubnetStatus: "Active"})
	}

	sg := &ClusterSubnetGroup{
		ClusterSubnetGroupName: name,
		Description:            description,
		VpcID:                  vpcID,
		SubnetGroupStatus:      "Complete",
		Subnets:                subnets,
	}
	b.subnetGroups[name] = sg

	return cloneSubnetGroup(sg), nil
}

// DeleteClusterSubnetGroup removes a cluster subnet group.
func (b *InMemoryBackend) DeleteClusterSubnetGroup(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ClusterSubnetGroupName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteClusterSubnetGroup")
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; !exists {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}

	delete(b.subnetGroups, name)

	return nil
}

// DescribeClusterSubnetGroups returns all subnet groups, or a specific one if name is non-empty.
func (b *InMemoryBackend) DescribeClusterSubnetGroups(name string) ([]ClusterSubnetGroup, error) {
	b.mu.RLock("DescribeClusterSubnetGroups")
	defer b.mu.RUnlock()

	if name != "" {
		sg, exists := b.subnetGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
		}

		return []ClusterSubnetGroup{*cloneSubnetGroup(sg)}, nil
	}

	result := make([]ClusterSubnetGroup, 0, len(b.subnetGroups))
	for _, sg := range b.subnetGroups {
		result = append(result, *cloneSubnetGroup(sg))
	}

	return result, nil
}

// ModifyClusterSubnetGroup updates the description or subnets of a cluster subnet group.
func (b *InMemoryBackend) ModifyClusterSubnetGroup(
	name, description string,
	subnetIDs []string,
) (*ClusterSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ClusterSubnetGroupName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyClusterSubnetGroup")
	defer b.mu.Unlock()

	sg, exists := b.subnetGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}

	if description != "" {
		sg.Description = description
	}

	if len(subnetIDs) > 0 {
		subnets := make([]Subnet, 0, len(subnetIDs))
		for _, id := range subnetIDs {
			subnets = append(subnets, Subnet{SubnetIdentifier: id, SubnetStatus: "Active"})
		}

		sg.Subnets = subnets
	}

	return cloneSubnetGroup(sg), nil
}

// cloneSubnetGroup returns a deep copy of a ClusterSubnetGroup.
func cloneSubnetGroup(sg *ClusterSubnetGroup) *ClusterSubnetGroup {
	cp := *sg
	cp.Subnets = make([]Subnet, len(sg.Subnets))
	copy(cp.Subnets, sg.Subnets)

	return &cp
}

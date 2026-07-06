package redshift

import (
	"fmt"
)

// CreateClusterSecurityGroup creates a new cluster security group.
func (b *InMemoryBackend) CreateClusterSecurityGroup(name, description string) (*ClusterSecurityGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ClusterSecurityGroupName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateClusterSecurityGroup")
	defer b.mu.Unlock()

	if _, exists := b.securityGroups.Get(name); exists {
		return nil, fmt.Errorf("%w: security group %s already exists", ErrSecurityGroupAlreadyExists, name)
	}

	sg := &ClusterSecurityGroup{
		ClusterSecurityGroupName: name,
		Description:              description,
		IPRanges:                 []IPRange{},
		EC2SecurityGroups:        []EC2SecurityGroup{},
	}
	b.securityGroups.Put(sg)

	return cloneSecurityGroup(sg), nil
}

// DeleteClusterSecurityGroup removes a cluster security group.
func (b *InMemoryBackend) DeleteClusterSecurityGroup(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ClusterSecurityGroupName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteClusterSecurityGroup")
	defer b.mu.Unlock()

	if _, exists := b.securityGroups.Get(name); !exists {
		return fmt.Errorf("%w: security group %s not found", ErrSecurityGroupNotFound, name)
	}

	b.securityGroups.Delete(name)

	return nil
}

// DescribeClusterSecurityGroups returns all security groups, or a specific one if name is non-empty.
func (b *InMemoryBackend) DescribeClusterSecurityGroups(name string) ([]ClusterSecurityGroup, error) {
	b.mu.RLock("DescribeClusterSecurityGroups")
	defer b.mu.RUnlock()

	if name != "" {
		sg, exists := b.securityGroups.Get(name)
		if !exists {
			return nil, fmt.Errorf("%w: security group %s not found", ErrSecurityGroupNotFound, name)
		}

		return []ClusterSecurityGroup{*cloneSecurityGroup(sg)}, nil
	}

	result := make([]ClusterSecurityGroup, 0, b.securityGroups.Len())
	for _, sg := range b.securityGroups.All() {
		result = append(result, *cloneSecurityGroup(sg))
	}

	return result, nil
}

// RevokeClusterSecurityGroupIngress removes an ingress rule from a cluster security group.
func (b *InMemoryBackend) RevokeClusterSecurityGroupIngress(
	groupName, cidrIP, ec2GroupName, ec2GroupOwnerID string,
) (*ClusterSecurityGroup, error) {
	if groupName == "" {
		return nil, fmt.Errorf("%w: ClusterSecurityGroupName is required", ErrInvalidParameter)
	}
	if cidrIP == "" && ec2GroupName == "" {
		return nil, fmt.Errorf("%w: CIDRIP or EC2SecurityGroupName is required", ErrInvalidParameter)
	}

	b.mu.Lock("RevokeClusterSecurityGroupIngress")
	defer b.mu.Unlock()

	sg, exists := b.securityGroups.Get(groupName)
	if !exists {
		return nil, fmt.Errorf("%w: security group %s not found", ErrSecurityGroupNotFound, groupName)
	}

	if cidrIP != "" {
		filtered := sg.IPRanges[:0]

		for _, r := range sg.IPRanges {
			if r.CIDRIP != cidrIP {
				filtered = append(filtered, r)
			}
		}

		sg.IPRanges = filtered
	}

	if ec2GroupName != "" {
		filtered := sg.EC2SecurityGroups[:0]

		for _, g := range sg.EC2SecurityGroups {
			if g.EC2SecurityGroupName != ec2GroupName ||
				(ec2GroupOwnerID != "" && g.EC2SecurityGroupOwnerID != ec2GroupOwnerID) {
				filtered = append(filtered, g)
			}
		}

		sg.EC2SecurityGroups = filtered
	}

	return cloneSecurityGroup(sg), nil
}

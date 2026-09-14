package dms

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// CreateReplicationSubnetGroup creates a subnet group.
func (b *InMemoryBackend) CreateReplicationSubnetGroup(
	ctx context.Context,
	identifier, description, vpcID string,
	subnetIDs []string,
	kv map[string]string,
) (*ReplicationSubnetGroup, error) {
	b.mu.Lock("CreateReplicationSubnetGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.replicationSubnetGroups.Has(regionKey(region, identifier)) {
		return nil, fmt.Errorf(
			"%w: replication subnet group %s already exists",
			ErrAlreadyExists,
			identifier,
		)
	}

	sgARN := arn.Build("dms", region, b.accountID, "subgrp:"+identifier)
	t := tags.New("dms.replication-subnet-group." + identifier + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}
	sg := &ReplicationSubnetGroup{
		ReplicationSubnetGroupIdentifier:  identifier,
		ReplicationSubnetGroupArn:         sgARN,
		ReplicationSubnetGroupDescription: description,
		VpcID:                             vpcID,
		SubnetIDs:                         subnetIDs,
		AccountID:                         b.accountID,
		Region:                            region,
		Tags:                              t,
	}
	b.replicationSubnetGroups.Put(sg)
	cp := *sg

	return &cp, nil
}

// DeleteReplicationSubnetGroup deletes a subnet group by identifier or ARN.
func (b *InMemoryBackend) DeleteReplicationSubnetGroup(ctx context.Context, identifierOrArn string) error {
	b.mu.Lock("DeleteReplicationSubnetGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if sg, ok := b.replicationSubnetGroups.Get(regionKey(region, identifierOrArn)); ok {
		sg.Tags.Close()
		b.replicationSubnetGroups.Delete(regionKey(region, identifierOrArn))

		return nil
	}

	if sg, ok := lookupUnique(b.replicationSubnetGroupsByARN, regionKey(region, identifierOrArn)); ok {
		sg.Tags.Close()
		b.replicationSubnetGroups.Delete(regionKey(region, sg.ReplicationSubnetGroupIdentifier))

		return nil
	}

	return fmt.Errorf("%w: replication subnet group %s not found", ErrNotFound, identifierOrArn)
}

// ModifyReplicationSubnetGroup updates a subnet group's description and
// subnet membership by identifier or ARN. SubnetIds is a required
// ModifyReplicationSubnetGroupInput member (api_op_ModifyReplicationSubnetGroup.go)
// that this backend previously dropped entirely -- real
// types.ReplicationSubnetGroup.Subnets (types.go:4135) was never populated by
// either Create or Modify, so a real client always saw an empty Subnets list
// regardless of what it requested.
func (b *InMemoryBackend) ModifyReplicationSubnetGroup(
	ctx context.Context,
	identifierOrArn, description string,
	subnetIDs []string,
) (*ReplicationSubnetGroup, error) {
	b.mu.Lock("ModifyReplicationSubnetGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if sg, ok := b.replicationSubnetGroups.Get(regionKey(region, identifierOrArn)); ok {
		if description != "" {
			sg.ReplicationSubnetGroupDescription = description
		}

		if len(subnetIDs) > 0 {
			sg.SubnetIDs = subnetIDs
		}
		cp := *sg

		return &cp, nil
	}

	if sg, ok := lookupUnique(b.replicationSubnetGroupsByARN, regionKey(region, identifierOrArn)); ok {
		if description != "" {
			sg.ReplicationSubnetGroupDescription = description
		}

		if len(subnetIDs) > 0 {
			sg.SubnetIDs = subnetIDs
		}
		cp := *sg

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: replication subnet group %s not found", ErrNotFound, identifierOrArn)
}

// DescribeReplicationSubnetGroups returns all subnet groups.
func (b *InMemoryBackend) DescribeReplicationSubnetGroups(ctx context.Context) ([]*ReplicationSubnetGroup, error) {
	b.mu.RLock("DescribeReplicationSubnetGroups")
	defer b.mu.RUnlock()

	items := b.replicationSubnetGroupsByRegion.Get(getRegion(ctx, b.region))
	list := make([]*ReplicationSubnetGroup, 0, len(items))
	for _, sg := range items {
		cp := *sg
		list = append(list, &cp)
	}

	return list, nil
}

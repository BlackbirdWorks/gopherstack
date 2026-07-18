package docdb

import (
	"context"
	"fmt"
	"sort"
)

func (b *InMemoryBackend) CreateDBSubnetGroup(
	ctx context.Context,
	name, description, vpcID string,
	subnetIDs []string,
	tags map[string]string,
) (*DBSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBSubnetGroupName is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBSubnetGroup")
	defer b.mu.Unlock()
	if b.subnetGroupHas(region, name) {
		return nil, fmt.Errorf("%w: subnet group %s already exists", ErrSubnetGroupAlreadyExists, name)
	}
	ids := make([]string, len(subnetIDs))
	copy(ids, subnetIDs)
	sgArn := b.subnetGroupARN(region, name)
	sg := &DBSubnetGroup{
		region:                   region,
		DBSubnetGroupName:        name,
		DBSubnetGroupDescription: description,
		VpcID:                    vpcID,
		Status:                   "complete",
		SubnetIDs:                ids,
		DBSubnetGroupArn:         sgArn,
		Tags:                     copyTags(tags),
	}
	b.subnetGroupPut(sg)
	if len(tags) > 0 {
		b.tagsStore(region)[sgArn] = tagsFromMap(tags)
	}
	cp := *sg
	cp.SubnetIDs = make([]string, len(ids))
	copy(cp.SubnetIDs, ids)
	cp.Tags = copyTags(sg.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBSubnetGroups(ctx context.Context, name string) ([]DBSubnetGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBSubnetGroups")
	defer b.mu.RUnlock()
	if name != "" {
		sg, exists := b.subnetGroupGet(region, name)
		if !exists {
			return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
		}
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)
		cp.Tags = copyTags(sg.Tags)

		return []DBSubnetGroup{cp}, nil
	}
	subnetGroups := b.subnetGroupsInRegion(region)
	result := make([]DBSubnetGroup, 0, len(subnetGroups))
	for _, sg := range subnetGroups {
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)
		cp.Tags = copyTags(sg.Tags)
		result = append(result, cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBSubnetGroupName < result[j].DBSubnetGroupName
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBSubnetGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBSubnetGroup")
	defer b.mu.Unlock()
	if !b.subnetGroupHas(region, name) {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	for _, c := range b.clustersInRegion(region) {
		if c.DBSubnetGroupName == name {
			return fmt.Errorf(
				"%w: subnet group %s is used by cluster %s",
				ErrSubnetGroupInUse,
				name,
				c.DBClusterIdentifier,
			)
		}
	}
	b.subnetGroupDelete(region, name)
	delete(b.tagsStore(region), b.subnetGroupARN(region, name))

	return nil
}

// ModifyDBSubnetGroup modifies a DB subnet group.
func (b *InMemoryBackend) ModifyDBSubnetGroup(
	ctx context.Context,
	name, description string,
	subnetIDs []string,
) (*DBSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBSubnetGroupName is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBSubnetGroup")
	defer b.mu.Unlock()
	sg, exists := b.subnetGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	if description != "" {
		sg.DBSubnetGroupDescription = description
	}
	if len(subnetIDs) > 0 {
		ids := make([]string, len(subnetIDs))
		copy(ids, subnetIDs)
		sg.SubnetIDs = ids
	}
	cp := *sg
	cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
	copy(cp.SubnetIDs, sg.SubnetIDs)
	cp.Tags = copyTags(sg.Tags)

	return &cp, nil
}

package memorydb

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateSubnetGroup creates a new subnet group.
func (b *InMemoryBackend) CreateSubnetGroup(ctx context.Context, req *createSubnetGroupRequest) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if err := validateResourceName(req.SubnetGroupName, "subnet group"); err != nil {
		return nil, err
	}

	if _, exists := b.subnetGroupsStore(region).Get(req.SubnetGroupName); exists {
		return nil, ErrSubnetGroupAlreadyExists
	}

	sgARN := arn.Build("memorydb", region, b.accountID, "subnetgroup/"+req.SubnetGroupName)

	sg := &SubnetGroup{
		Name:        req.SubnetGroupName,
		ARN:         sgARN,
		Description: req.Description,
		SubnetIDs:   req.SubnetIDs,
		Tags:        tagsFromSlice(req.Tags),
		CreatedAt:   time.Now(),
	}

	b.subnetGroupsStore(region).Put(sg)
	b.arnToResourceStore(region)[sgARN] = resourceRef{Kind: resourceKindSubnetGroup, Name: req.SubnetGroupName}

	return cloneSubnetGroup(sg), nil
}

// DescribeSubnetGroups returns subnet groups, optionally filtered by name.
func (b *InMemoryBackend) DescribeSubnetGroups(ctx context.Context, name string) ([]*SubnetGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	t := b.subnetGroups[region]

	if name != "" {
		sg, ok := tableGet(t, name)

		if !ok {
			return nil, ErrSubnetGroupNotFound
		}

		return []*SubnetGroup{cloneSubnetGroup(sg)}, nil
	}

	all := tableAll(t)
	result := make([]*SubnetGroup, 0, len(all))
	for _, sg := range all {
		result = append(result, cloneSubnetGroup(sg))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteSubnetGroup removes a subnet group.
func (b *InMemoryBackend) DeleteSubnetGroup(ctx context.Context, name string) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	sg, ok := b.subnetGroupsStore(region).Get(name)
	if !ok {
		return nil, ErrSubnetGroupNotFound
	}

	for _, c := range tableAll(b.clusters[region]) {
		if c.SubnetGroupName == name {
			return nil, fmt.Errorf(
				"subnet group %q is associated with cluster %q: %w",
				name, c.Name, ErrSubnetGroupInUse,
			)
		}
	}

	b.subnetGroupsStore(region).Delete(name)
	delete(b.arnToResourceStore(region), sg.ARN)

	return sg, nil
}

// UpdateSubnetGroup modifies an existing subnet group.
func (b *InMemoryBackend) UpdateSubnetGroup(ctx context.Context, req *updateSubnetGroupRequest) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	sg, ok := b.subnetGroupsStore(region).Get(req.SubnetGroupName)
	if !ok {
		return nil, ErrSubnetGroupNotFound
	}

	if req.Description != "" {
		sg.Description = req.Description
	}

	if len(req.SubnetIDs) > 0 {
		sg.SubnetIDs = req.SubnetIDs
	}

	return cloneSubnetGroup(sg), nil
}

// -- User operations -------------------------------------------------------------

// cloneSubnetGroup returns a shallow copy of the subnet group with separate slices.
func cloneSubnetGroup(sg *SubnetGroup) *SubnetGroup {
	if sg == nil {
		return nil
	}

	cp := *sg
	cp.Tags = maps.Clone(sg.Tags)
	cp.SubnetIDs = append([]string(nil), sg.SubnetIDs...)

	return &cp
}

// AddSubnetGroupInternal inserts a subnet group directly into the backend for testing.
func (b *InMemoryBackend) AddSubnetGroupInternal(name string) *SubnetGroup {
	b.mu.Lock()
	defer b.mu.Unlock()

	sgARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "subnetgroup/"+name)
	sg := &SubnetGroup{
		Name:      name,
		ARN:       sgARN,
		Tags:      make(map[string]string),
		CreatedAt: time.Now(),
	}
	b.subnetGroupsStore(b.defaultRegion).Put(sg)
	b.arnToResourceStore(b.defaultRegion)[sgARN] = resourceRef{Kind: resourceKindSubnetGroup, Name: name}

	return sg
}

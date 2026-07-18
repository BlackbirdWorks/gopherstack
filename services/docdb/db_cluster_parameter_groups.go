package docdb

import (
	"context"
	"fmt"
	"maps"
	"sort"
)

func (b *InMemoryBackend) CreateDBClusterParameterGroup(
	ctx context.Context,
	name, family, description string,
	tags map[string]string,
) (*DBClusterParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBClusterParameterGroup")
	defer b.mu.Unlock()
	if b.clusterParameterGroupHas(region, name) {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrClusterParameterGroupAlreadyExists,
			name,
		)
	}
	pg := &DBClusterParameterGroup{
		region:                      region,
		DBClusterParameterGroupName: name,
		DBParameterGroupFamily:      family,
		Description:                 description,
		DBClusterParameterGroupArn:  b.clusterParameterGroupARN(region, name),
		Tags:                        copyTags(tags),
		Parameters:                  make(map[string]string),
	}
	b.clusterParameterGroupPut(pg)
	pgArn := b.clusterParameterGroupARN(region, name)
	if len(tags) > 0 {
		b.tagsStore(region)[pgArn] = tagsFromMap(tags)
	}
	cp := *pg
	cp.Tags = copyTags(pg.Tags)
	cp.Parameters = maps.Clone(pg.Parameters)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBClusterParameterGroups(
	ctx context.Context,
	name string,
) ([]DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.clusterParameterGroupGet(region, name)
		if !exists {
			return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
		}
		cp := *pg
		cp.Tags = copyTags(pg.Tags)

		return []DBClusterParameterGroup{cp}, nil
	}
	pgStore := b.clusterParameterGroupsInRegion(region)
	result := make([]DBClusterParameterGroup, 0, len(pgStore))
	for _, pg := range pgStore {
		cp := *pg
		cp.Tags = copyTags(pg.Tags)
		result = append(result, cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBClusterParameterGroupName < result[j].DBClusterParameterGroupName
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBClusterParameterGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBClusterParameterGroup")
	defer b.mu.Unlock()
	if !b.clusterParameterGroupHas(region, name) {
		return fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	for _, c := range b.clustersInRegion(region) {
		if c.DBClusterParameterGroupName == name {
			return fmt.Errorf(
				"%w: parameter group %s is used by cluster %s",
				ErrParameterGroupInUse,
				name,
				c.DBClusterIdentifier,
			)
		}
	}
	b.clusterParameterGroupDelete(region, name)
	delete(b.tagsStore(region), b.clusterParameterGroupARN(region, name))

	return nil
}

func (b *InMemoryBackend) ModifyDBClusterParameterGroup(
	ctx context.Context,
	name string,
	parameters map[string]string,
) (*DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}

	if pg.Parameters == nil {
		pg.Parameters = make(map[string]string)
	}

	maps.Copy(pg.Parameters, parameters)

	cp := *pg
	cp.Tags = copyTags(pg.Tags)
	cp.Parameters = maps.Clone(pg.Parameters)

	return &cp, nil
}

// CopyDBClusterParameterGroup copies a DB cluster parameter group.
func (b *InMemoryBackend) CopyDBClusterParameterGroup(
	ctx context.Context,
	sourceGroupName, targetName, targetDescription string,
) (*DBClusterParameterGroup, error) {
	if sourceGroupName == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	if targetName == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CopyDBClusterParameterGroup")
	defer b.mu.Unlock()
	src, exists := b.clusterParameterGroupGet(region, sourceGroupName)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s not found",
			ErrClusterParameterGroupNotFound,
			sourceGroupName,
		)
	}
	if b.clusterParameterGroupHas(region, targetName) {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrClusterParameterGroupAlreadyExists,
			targetName,
		)
	}
	desc := targetDescription
	if desc == "" {
		desc = src.Description
	}
	pg := &DBClusterParameterGroup{
		region:                      region,
		DBClusterParameterGroupName: targetName,
		DBParameterGroupFamily:      src.DBParameterGroupFamily,
		Description:                 desc,
		DBClusterParameterGroupArn:  b.clusterParameterGroupARN(region, targetName),
		Parameters:                  maps.Clone(src.Parameters),
	}
	b.clusterParameterGroupPut(pg)
	cp := *pg
	cp.Tags = copyTags(pg.Tags)
	cp.Parameters = maps.Clone(pg.Parameters)

	return &cp, nil
}

// DescribeDBClusterParameters returns the parameters for a DB cluster parameter group.
func (b *InMemoryBackend) DescribeDBClusterParameters(
	ctx context.Context,
	groupName string,
) ([]DBClusterParameter, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterParameters")
	defer b.mu.RUnlock()
	if groupName == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	pg, exists := b.clusterParameterGroupGet(region, groupName)
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, groupName)
	}

	defaults := []DBClusterParameter{
		{
			ParameterName:  "tls",
			ParameterValue: paramEnabled,
			Description:    "Specifies the TLS setting",
			Source:         "system",
			ApplyType:      "static",
			DataType:       paramTypeStr,
			IsModifiable:   true,
		},
		{
			ParameterName:  "ttl_monitor",
			ParameterValue: paramEnabled,
			Description:    "Specifies the TTL monitor setting",
			Source:         "system",
			ApplyType:      "dynamic",
			DataType:       paramTypeStr,
			IsModifiable:   true,
		},
	}

	params := make([]DBClusterParameter, 0, len(defaults))
	for _, p := range defaults {
		if pg.Parameters != nil {
			if v, ok := pg.Parameters[p.ParameterName]; ok {
				p.ParameterValue = v
				p.Source = "user"
			}
		}

		params = append(params, p)
	}

	return params, nil
}

// DescribeEngineDefaultClusterParameters returns the default parameters for an engine family.
func (b *InMemoryBackend) DescribeEngineDefaultClusterParameters(
	_ context.Context,
	_ string,
) []DBClusterParameter {
	return []DBClusterParameter{
		{
			ParameterName:  "tls",
			ParameterValue: paramEnabled,
			Description:    "Specifies the TLS setting",
			Source:         "engine-default",
			ApplyType:      "static",
			DataType:       paramTypeStr,
			IsModifiable:   true,
		},
		{
			ParameterName:  "ttl_monitor",
			ParameterValue: paramEnabled,
			Description:    "Specifies the TTL monitor setting",
			Source:         "engine-default",
			ApplyType:      "dynamic",
			DataType:       paramTypeStr,
			IsModifiable:   true,
		},
	}
}

// ResetDBClusterParameterGroup resets a parameter group to its default values.
func (b *InMemoryBackend) ResetDBClusterParameterGroup(
	ctx context.Context,
	name string,
) (*DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ResetDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	cp := *pg
	cp.Tags = copyTags(pg.Tags)

	return &cp, nil
}
